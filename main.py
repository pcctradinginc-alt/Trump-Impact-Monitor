import os
import re
import json
import base64
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import socket
import html
import feedparser
import requests
import yfinance as yf
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from anthropic import Anthropic
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import smtplib
import sys
import time
import xml.etree.ElementTree as ET
from config import (
    WATCHLIST_HIGH, WATCHLIST_MEDIUM, WATCHLIST_LOW, WATCHLIST_ALL,
    MAX_ALERTS, LOOKBACK_HOURS, MIN_CONFIDENCE, MIN_MAGNITUDE,
    SRC_TRUTH, SRC_RSS, SRC_WHITEHOUSE,
    SRC_FEDREGISTER, SRC_EDGAR, SRC_OGE, SEND_NO_TRADE, INCLUDE_RETWEETS,
    confidence_ok, magnitude_ok,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.Formatter.converter = time.gmtime  # Timestamps immer in UTC
log = logging.getLogger("trump_monitor")

# Externe Libraries auf WARNING setzen — deren DEBUG/INFO-Spam unterdrücken
for _lib in ("httpx", "httpcore", "anthropic", "urllib3", "feedparser"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# Globaler Timeout — gilt für feedparser, smtplib und alle socket-basierten Calls.
# yfinance nutzt intern requests; dessen Session-Timeout wird separat in
# fetch_market_data gesetzt (timeout= Parameter).
socket.setdefaulttimeout(30)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  (alle Werte kommen aus GitHub Secrets / lokalen Env-Vars)
# ─────────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SCRAPE_KEY        = os.getenv("SCRAPE_CREATORS_API_KEY")
GMAIL_EMAIL       = os.getenv("GMAIL_EMAIL")
GMAIL_PASS        = os.getenv("GMAIL_APP_PASSWORD")
RECIPIENT         = os.getenv("RECIPIENT_EMAIL")

TRUMP_TRUTH_ID       = "107780257626128497"
DB_PATH              = "alerts.db"
MAX_ALERTS_PER_RUN   = MAX_ALERTS  # aus config.yml
MAX_TICKERS_PER_ART  = 3
MODEL                = "claude-sonnet-4-6"

# ─────────────────────────────────────────────────────────────────────────────
# SECRETS-VALIDIERUNG
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED = {
    "ANTHROPIC_API_KEY":       ANTHROPIC_API_KEY,
    "GMAIL_EMAIL":             GMAIL_EMAIL,
    "GMAIL_APP_PASSWORD":      GMAIL_PASS,
    "RECIPIENT_EMAIL":         RECIPIENT,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    log.error(f"❌ Fehlende Secrets: {', '.join(missing)}")
    sys.exit(1)

# Optional: ScrapeCreators ist kostenpflichtig und nur letzter Fallback.
# Ohne Key laufen alle kostenlosen Quellen (trumpstruth.org, CNN-Archiv) normal.
if not SCRAPE_KEY:
    log.info("ℹ️  SCRAPE_CREATORS_API_KEY nicht gesetzt – nur kostenlose Truth-Social-Quellen aktiv")

# ─────────────────────────────────────────────────────────────────────────────
# SQLITE  –  Dedup-Datenbank
# ─────────────────────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")   # verhindert DB-Korruption bei parallelen Runs
conn.execute("PRAGMA synchronous=NORMAL") # WAL + NORMAL: schnell und sicher
conn.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id     TEXT PRIMARY KEY,
        source       TEXT,
        published_at TEXT,
        raw_text     TEXT,
        hash         TEXT UNIQUE,
        ticker       TEXT,
        processed_at TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS outcomes (
        event_id     TEXT PRIMARY KEY,
        ticker       TEXT,
        direction    TEXT,
        price_alert  REAL,
        price_24h    REAL,
        price_7d     REAL,
        checked_at   TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS edgar_filings (
        accession    TEXT PRIMARY KEY,
        form_type    TEXT,
        filed_date   TEXT,
        ticker       TEXT,
        issuer       TEXT,
        tx_type      TEXT,
        tx_shares    TEXT,
        tx_price     TEXT,
        tx_date      TEXT,
        alerted_at   TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS rate_limit (
        key        TEXT PRIMARY KEY,
        count      INTEGER DEFAULT 0,
        window_start TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS trump_278e_snapshot (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        year         INTEGER,
        asset_name   TEXT,
        ticker       TEXT,
        value_range  TEXT,
        income_type  TEXT,
        income_amount TEXT,
        pdf_url      TEXT,
        parsed_at    TEXT
    )
""")
conn.execute("""
    CREATE TABLE IF NOT EXISTS trump_holdings (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker      TEXT,
        asset_name  TEXT,
        tx_type     TEXT,
        amount      TEXT,
        tx_date     TEXT,
        pdf_url     TEXT,
        created_at  TEXT
    )
""")
conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# RATE LIMITING  –  tägliches Sonnet-Budget + per-Ticker-Cooldown
# ─────────────────────────────────────────────────────────────────────────────
DAILY_SONNET_CAP    = 40   # max. Sonnet-Calls pro UTC-Tag
TICKER_COOLDOWN_H   = 4    # gleicher Ticker nicht öfter als alle 4h

def _rate_limit_ok(ticker: str) -> bool:
    """
    Prüft zwei Limits:
    1. Tägliches Sonnet-Budget (DAILY_SONNET_CAP)
    2. Per-Ticker-Cooldown (TICKER_COOLDOWN_H)
    Gibt False zurück wenn eines überschritten ist.
    """
    today = now_utc().strftime("%Y-%m-%d")

    # — Tagesbudget ───────────────────────────────────────────────────────────
    row = conn.execute(
        "SELECT count, window_start FROM rate_limit WHERE key='daily_sonnet'"
    ).fetchone()
    if row:
        count, window_start = row
        if window_start == today and count >= DAILY_SONNET_CAP:
            log.warning(
                "🚦 Tages-Cap erreicht (%d/%d Sonnet-Calls) – kein weiterer Alert heute",
                count, DAILY_SONNET_CAP,
            )
            return False
        if window_start != today:
            # Neuer Tag → Zähler zurücksetzen
            conn.execute(
                "UPDATE rate_limit SET count=0, window_start=? WHERE key='daily_sonnet'",
                (today,),
            )
    else:
        conn.execute(
            "INSERT INTO rate_limit VALUES ('daily_sonnet', 0, ?)", (today,)
        )

    # — Ticker-Cooldown ───────────────────────────────────────────────────────
    cooldown_key = f"ticker_{ticker.upper()}"
    row = conn.execute(
        "SELECT count, window_start FROM rate_limit WHERE key=?", (cooldown_key,)
    ).fetchone()
    if row:
        _, last_alert_ts = row
        try:
            last_dt = datetime.fromisoformat(last_alert_ts)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            elapsed_h = (now_utc() - last_dt).total_seconds() / 3600
            if elapsed_h < TICKER_COOLDOWN_H:
                log.info(
                    "  ⏳ %s Cooldown: letzter Alert vor %.1fh (min. %dh) → übersprungen",
                    ticker, elapsed_h, TICKER_COOLDOWN_H,
                )
                return False
        except Exception:
            pass

    conn.commit()
    return True


def _rate_limit_record(ticker: str) -> None:
    """Zählt einen verbrauchten Sonnet-Call und aktualisiert den Ticker-Timestamp."""
    today = now_utc().strftime("%Y-%m-%d")
    now_iso = now_utc().isoformat()

    conn.execute("""
        INSERT INTO rate_limit (key, count, window_start) VALUES ('daily_sonnet', 1, ?)
        ON CONFLICT(key) DO UPDATE SET
            count        = CASE WHEN window_start = excluded.window_start
                                THEN count + 1 ELSE 1 END,
            window_start = excluded.window_start
    """, (today,))

    cooldown_key = f"ticker_{ticker.upper()}"
    conn.execute("""
        INSERT INTO rate_limit (key, count, window_start) VALUES (?, 1, ?)
        ON CONFLICT(key) DO UPDATE SET count=1, window_start=excluded.window_start
    """, (cooldown_key, now_iso))

    conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT  (einmalig instanziieren)
# ─────────────────────────────────────────────────────────────────────────────
client = Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────────────────────────────────────────
# TRUMP-NAMES  –  Pflichtfilter für allgemeine Finanz-RSS-Feeds
# ─────────────────────────────────────────────────────────────────────────────
TRUMP_NAMES = {
    "trump", "donald trump", "donald j. trump", "potus",
    "mar-a-lago", "truth social", "trump administration",
    "the president", "white house", "executive order",
    "commander in chief", "oval office", "trump tariff",
    "trump tax", "trump trade", "trump ban", "trump deal",
    "trade representative", "ustr",
}

def mentions_trump(text: str) -> bool:
    t = text.lower()
    return any(name in t for name in TRUMP_NAMES)

# ─────────────────────────────────────────────────────────────────────────────
# FINANZ-KEYWORDS  –  Pre-Filter vor teurem LLM-Aufruf
# ─────────────────────────────────────────────────────────────────────────────
FINANCIAL_KEYWORDS = {
    # Märkte & Instrumente
    "tariff", "tariffs", "sanction", "sanctions", "trade deal", "trade war",
    "invest", "investment", "stock", "stocks", "shares", "share price", "market",
    "deal", "contract", "merger", "acquisition", "ipo", "spin-off", "buyout",
    "ban", "subsidy", "subsidies", "tax", "fine", "penalty", "lawsuit", "settlement",
    "regulation", "deregulation", "import", "export", "duty", "duties", "quota",
    "manufacturer", "factory", "production", "supply chain", "supply chains",
    "revenue", "profit", "earnings", "dividend", "valuation", "market cap",
    "billions", "millions", "trillion", "billion dollar", "million dollar",
    "economy", "economic", "gdp", "recession", "federal reserve", "fed rate",
    "interest rate", "inflation", "deflation", "currency", "dollar", "yuan",
    "oil", "crude", "gas", "lng", "energy", "nuclear", "solar", "wind power",
    "chip", "chips", "semiconductor", "wafer", "fab", "foundry",
    "defense", "military contract", "pentagon", "nato", "weapons",
    "crypto", "bitcoin", "ethereum", "blockchain", "token", "coin",
    "privatize", "nationalize", "stimulus", "bailout", "debt ceiling",
    "price", "cost", "costs", "jobs", "layoff", "layoffs", "hire", "hiring",
    "company", "corporation", "enterprise", "business", "firm", "brand",
    "ceo", "founder", "chairman", "executive",
    # ── Trump-spezifische direkte Kauf-/Verkaufsempfehlungen ──────────────
    "buy", "buying", "sell", "selling", "selling off", "purchase", "invest in",
    "short", "short sell", "put option", "call option",
    # ── Trump-typische Superlative & Endorsements ──────────────────────────
    "great", "greatest", "great company", "great deal", "great job",
    "best", "best company", "best in the world", "best ever",
    "fantastic", "fantastically", "incredible", "incredibly",
    "amazing", "amazingly", "tremendous", "tremendously",
    "beautiful", "perfect", "genius", "brilliant", "outstanding",
    "terrific", "wonderful", "spectacular", "extraordinary",
    "winning", "winner", "winners", "huge win", "big win",
    "love", "love it", "love them", "love what they",
    "congratulations", "congrats", "well done", "proud of",
    # ── Trump-typische Negativaussagen & Boykott-Signale ──────────────────
    "terrible", "horrible", "disaster", "catastrophe", "failing", "failed",
    "loser", "losers", "overrated", "disgrace", "corrupt", "incompetent",
    "boycott", "never buy", "stay away", "avoid", "bad company", "weak",
    "rip off", "ripoff", "scam", "fraud", "unfair", "wrong", "fake",
    "angry", "furious", "not happy", "very unhappy", "disappointed",
    # ── Branchen-Kontext ───────────────────────────────────────────────────
    "pharmaceutical", "pharma", "drug", "vaccine", "biotech",
    "artificial intelligence", "ai company", "tech company",
    "steel", "aluminum", "aluminium", "copper", "lithium", "rare earth",
    "agriculture", "farm", "farmer", "wheat", "corn", "soy", "soybean",
    "shipping", "freight", "logistics", "port", "cargo",
    "real estate", "housing", "construction", "infrastructure",
    "bank", "banking", "finance", "insurance", "wall street",
}

# Zusätzlicher Sentiment-Filter NUR für Truth Social (Original-Posts Trump)
# Greift wenn FINANCIAL_KEYWORDS nicht matcht — fängt direkte Nennungen auf
TRUMP_DIRECT_SIGNALS = {
    "dell", "nvidia", "apple", "microsoft", "amazon", "google", "tesla",
    "meta", "palantir", "boeing", "lockheed", "raytheon", "spacex",
    "elon", "musk", "tim cook", "jensen huang", "andy jassy",
    "wall street", "nasdaq", "dow jones", "s&p", "nasdaq",
    "tariff", "trade", "china", "mexico", "canada", "europe", "eu",
}

def is_financially_relevant(text: str, truth_social: bool = False) -> bool:
    t = text.lower()
    if any(kw in t for kw in FINANCIAL_KEYWORDS):
        return True
    # Bei Truth-Social-Posts: auch direkte Firmen-/Personen-Nennungen
    if truth_social and any(kw in t for kw in TRUMP_DIRECT_SIGNALS):
        return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# TEXT-REINIGUNG  –  HTML / URLs entfernen vor Entity-Matching
# ─────────────────────────────────────────────────────────────────────────────
def clean_text(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)            # HTML-Tags zuerst (sonst zerreißt
    text = re.sub(r'https?://\S+', '', text)        # die URL-Entfernung Attribute)
    text = re.sub(r'\b[\w.-]+\.(com|org|net|gov)/\S*', '', text)  # schemalose URL-Reste
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ─────────────────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────────────────
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

CUTOFF: datetime  # set in main() at runtime

def is_recent(ts) -> bool:
    if ts is None:
        return False
    try:
        if isinstance(ts, datetime):
            dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        elif hasattr(ts, "tm_year"):
            dt = datetime(*ts[:6], tzinfo=timezone.utc)
        else:
            raw = str(ts).strip()
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= CUTOFF
    except Exception as e:
        log.warning(f"  ⚠️  Zeitstempel nicht parsebar ({ts!r}): {e} → übersprungen")
        return False

def get_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def event_hash(ticker: str, text: str) -> str:
    """Kanonisch: 'TICKER|text' — einheitliche Reihenfolge verhindert Dedup-Bug."""
    return hashlib.sha256(f"{ticker.upper()}|{text}".encode("utf-8")).hexdigest()

def already_seen(h: str) -> bool:
    return conn.execute("SELECT 1 FROM events WHERE hash=?", (h,)).fetchone() is not None

# ─────────────────────────────────────────────────────────────────────────────
# E-MAIL
# ─────────────────────────────────────────────────────────────────────────────
def send_gmail(subject: str, html_body: str) -> bool:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_EMAIL
    msg["To"]      = RECIPIENT
    msg.attach(MIMEText(html_body, "html"))

    for attempt in range(1, 4):
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(GMAIL_EMAIL, GMAIL_PASS)
                server.sendmail(GMAIL_EMAIL, RECIPIENT, msg.as_string())
            log.info("E-Mail gesendet: %s", subject)
            return True
        except smtplib.SMTPAuthenticationError:
            log.error("Gmail: Authentifizierung fehlgeschlagen – App-Passwort prüfen")
            return False  # Auth-Fehler: Retry sinnlos
        except smtplib.SMTPException as e:
            log.warning("Gmail SMTP-Fehler (Versuch %d/3): %s", attempt, e)
        except Exception as e:
            log.warning("Gmail unbekannter Fehler (Versuch %d/3): %s", attempt, e)

        if attempt < 3:
            time.sleep(5 * attempt)  # 5s, dann 10s

    log.error("E-Mail konnte nach 3 Versuchen nicht gesendet werden: %s", subject)
    return False

# ─────────────────────────────────────────────────────────────────────────────
# ENTITY RESOLUTION  –  3-Tier-Matching gegen entities.json
# ─────────────────────────────────────────────────────────────────────────────
ENTITIES_FILE = os.path.join(os.path.dirname(__file__), "entities.json")
with open(ENTITIES_FILE, encoding="utf-8") as f:
    ENTITIES: dict = json.load(f)

# Schneller kombinierter Tier-1-Regex: alle ~7000 Symbole in einem Pattern
# Regex-Engine baut daraus einen effizienten Automaten → O(text_length) statt O(n_tickers)
# Mindestlänge 2 Zeichen um False Positives wie "J", "A" zu vermeiden
_ALL_SYMBOLS = sorted((s for s in ENTITIES if len(s) >= 2), key=len, reverse=True)
_TIER1_REGEX = re.compile(
    r'\b(' + '|'.join(re.escape(s) for s in _ALL_SYMBOLS) + r')\b'
)

# Tickersymbole die zugleich häufige englische Wörter sind. Diese matchen nur
# mit $-Präfix ($ALL, $NOW) oder über Tier 2 (Firmenname) — sonst erzeugt jeder
# Trump-Post in GROSSBUCHSTABEN Dutzende Phantom-Ticker (ON, ALL, WIN, BIG …).
_AMBIGUOUS_SYMBOLS = {
    "ALL", "AN", "ANY", "ARE", "AT", "BE", "BIG", "BY", "CAN", "CAT", "DO",
    "EAT", "EVER", "FAST", "FLY", "FOR", "FUN", "GO", "GOOD", "HAS", "HE",
    "HER", "HIM", "HOPE", "HUGE", "IS", "IT", "LOVE", "LOW", "MAIN", "MAN",
    "ME", "MORE", "NEXT", "NICE", "NO", "NOW", "ON", "ONE", "OPEN", "OR",
    "OUT", "PAY", "PLAY", "REAL", "RUN", "SAFE", "SEE", "SHIP", "SO", "SUN",
    "TALK", "TECH", "TRUE", "TWO", "UP", "US", "USA", "VERY", "WAY", "WELL",
    "WIN", "YOU", "ALSO", "BEST", "EVEN", "FREE", "GAIN", "GREAT", "JOB",
    "LIFE", "LIKE", "MUST", "NEW", "OLD", "OUR", "OWN", "SAME", "SAY",
    "SELF", "SHE", "THE", "TOO", "WANT", "WAR", "WILL", "WOW",
}

def _is_mostly_uppercase(text: str) -> bool:
    """True wenn >70% der Buchstaben Großbuchstaben sind (typischer Trump-Post)."""
    letters = [c for c in text if c.isalpha()]
    if len(letters) < 20:
        return False
    return sum(1 for c in letters if c.isupper()) / len(letters) > 0.70

def find_all_tickers(text: str) -> list[tuple[str, str]]:
    """
    Tier 1 – symbol  : kombinierter Regex über alle ~7000 Symbole → "hoch"
    Tier 2 – company : case-insensitiv, Firmenname/CEO (nur bekannte 512) → "hoch"
    Tier 3 – weak    : case-insensitiv, Finanzkontext, Mindestlänge ≥5 → "niedrig"
    """
    results:     list[tuple[str, str]] = []
    seen:        set[str]              = set()
    has_finance: bool                  = is_financially_relevant(text)
    all_caps:    bool                  = _is_mostly_uppercase(text)

    # Tier 1 — kombinierter Regex: O(text_length), deckt alle ~7000 Symbole ab
    # Schutz vor Phantom-Tickern: mehrdeutige Symbole (englische Wörter) und
    # ALL-CAPS-Posts brauchen ein $-Präfix, sonst zählt nur Tier 2 (Firmenname).
    for m in _TIER1_REGEX.finditer(text):
        t = m.group(1).upper()
        if t in seen:
            continue
        has_dollar_prefix = m.start() > 0 and text[m.start() - 1] == "$"
        if not has_dollar_prefix and (t in _AMBIGUOUS_SYMBOLS or all_caps):
            continue
        results.append((t, "hoch"))
        seen.add(t)

    # Tier 4: Plural/Possessiv normalisieren vor Tier-2-Matching
    normalized = re.sub(r"'s\b", "", text)
    normalized = re.sub(r"(\b[A-Za-z]{3,})(s)\b", lambda m: m.group(1), normalized)

    # Tier 2 — Firmenname/CEO (nur Ticker mit nicht-leeren company-Aliases)
    # Aliases < 4 Zeichen ohne Ziffer/& (auto-generierter Junk wie 'Api', 'Fb')
    # werden ignoriert — sie matchen case-insensitiv praktisch jeden Text.
    # '3M', 'P&G', 'S&T' bleiben erlaubt; reine Kürzel matchen weiter via Tier 1.
    for ticker, tiers in ENTITIES.items():
        t = ticker.upper()
        if t in seen or not tiers.get("company"):
            continue
        for alias in tiers["company"]:
            if len(alias) < 4 and not re.search(r'[\d&]', alias):
                continue
            if re.search(r'\b' + re.escape(alias) + r'\b', normalized, re.IGNORECASE):
                results.append((t, "hoch"))
                seen.add(t)
                break

    # Tier 3 — schwache Aliases, nur mit Finanzkontext
    if has_finance:
        for ticker, tiers in ENTITIES.items():
            t = ticker.upper()
            if t in seen or not tiers.get("weak"):
                continue
            for alias in tiers["weak"]:
                if len(alias) >= 5 and re.search(
                    r'\b' + re.escape(alias) + r'\b', text, re.IGNORECASE
                ):
                    results.append((t, "niedrig"))
                    seen.add(t)
                    break

    return results

# ─────────────────────────────────────────────────────────────────────────────
# DATA SOURCES
# ─────────────────────────────────────────────────────────────────────────────

CNN_ARCHIVE_URL = "https://ix.cnn.io/data/truth-social/truth_archive.json"
TRUMPSTRUTH_FEED = "https://trumpstruth.org/feed"

# Browser-ähnlicher User-Agent — einige Feeds (z.B. CNBC) blocken Default-UAs
FEED_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _parse_feed(url: str):
    """
    RSS/Atom via requests laden (certifi-SSL, Timeout, Browser-UA) und mit
    feedparser parsen. Robuster als feedparser.parse(url) direkt (urllib).
    """
    r = requests.get(url, headers={"User-Agent": FEED_AGENT}, timeout=20)
    r.raise_for_status()
    return feedparser.parse(r.content)


def _fetch_truth_trumpstruth() -> list[dict]:
    """
    Primärquelle: trumpstruth.org RSS-Archiv (kostenlos, kein Key, ~150 KB).
    Liefert Original-URL auf truthsocial.com via truth:originalUrl Namespace.
    """
    feed = _parse_feed(TRUMPSTRUTH_FEED)
    if feed.bozo and not feed.entries:
        raise RuntimeError(f"trumpstruth.org Feed nicht lesbar: {feed.bozo_exception}")
    normalized = []
    for e in feed.entries:
        normalized.append({
            "text":       e.get("summary", e.get("description", e.get("title", ""))),
            "created_at": e.get("published", ""),
            "url":        e.get("truth_originalurl",
                          e.get("link", "https://truthsocial.com/@realDonaldTrump")),
            "_source":    "trumpstruth.org",
        })
    return normalized


def _fetch_truth_cnn() -> list[dict]:
    """
    Primärquelle: CNN Truth Social JSON-Archiv (~5min Refresh).
    Kein API-Key, keine Kosten. Gibt Posts im internen Format zurück.
    """
    r = requests.get(CNN_ARCHIVE_URL, timeout=15)
    r.raise_for_status()
    raw = r.json()
    # Format: Liste von Objekten oder {"posts": [...]}
    posts = raw if isinstance(raw, list) else raw.get("posts", raw.get("data", []))
    # Normalisierung auf einheitliches Format
    normalized = []
    for p in posts:
        normalized.append({
            "text":       p.get("content", p.get("text", p.get("body", ""))),
            "created_at": p.get("created_at", p.get("published_at", p.get("date", ""))),
            "url":        p.get("url", p.get("uri", p.get("link",
                          "https://truthsocial.com/@realDonaldTrump"))),
            "_source":    "CNN-Archiv",
        })
    return normalized


def _fetch_truth_scrapecreators() -> list[dict]:
    """Letzter Fallback: ScrapeCreators API (kostenpflichtig, optional)."""
    if not SCRAPE_KEY:
        return []
    url     = (f"https://api.scrapecreators.com/v1/truthsocial/user/posts"
               f"?user_id={TRUMP_TRUTH_ID}&limit=20")
    headers = {"x-api-key": SCRAPE_KEY}
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            posts = r.json().get("posts", r.json().get("data", []))
            # Normalisierung
            normalized = []
            for p in posts:
                normalized.append({
                    "text":       p.get("text", p.get("content", "")),
                    "created_at": p.get("created_at", p.get("published", "")),
                    "url":        p.get("url", p.get("uri",
                                  "https://truthsocial.com/@realDonaldTrump")),
                    "_source":    "ScapeCreators",
                })
            return normalized
        except requests.exceptions.Timeout:
            log.warning(f"  ⚠️  ScapeCreators: Timeout (Versuch {attempt}/3)")
        except requests.exceptions.HTTPError as e:
            log.warning(f"  ⚠️  ScapeCreators HTTP-Fehler: {e} (Versuch {attempt}/3)")
            if e.response is not None and e.response.status_code < 500:
                break
        except Exception as e:
            log.warning(f"  ⚠️  ScapeCreators Fehler: {e} (Versuch {attempt}/3)")
        if attempt < 3:
            time.sleep(2 ** attempt)
    return []


def fetch_truth_social() -> list[dict]:
    """
    Kostenlose Quellen zuerst: trumpstruth.org (leicht) → CNN-Archiv (15 MB)
    → ScrapeCreators (kostenpflichtig, nur wenn Key gesetzt).
    """
    try:
        posts = _fetch_truth_trumpstruth()
        log.info(f"  Truth Social (trumpstruth.org): {len(posts)} Posts")
        if posts:
            return posts
    except Exception as e:
        log.warning(f"  ⚠️  trumpstruth.org nicht verfügbar: {e} → CNN-Archiv Fallback")
    try:
        posts = _fetch_truth_cnn()
        log.info(f"  Truth Social (CNN-Archiv): {len(posts)} Posts")
        if posts:
            return posts
    except Exception as e:
        log.warning(f"  ⚠️  CNN-Archiv nicht verfügbar: {e} → ScrapeCreators Fallback")
    posts = _fetch_truth_scrapecreators()
    log.info(f"  Truth Social (ScrapeCreators): {len(posts)} Posts")
    return posts


def fetch_federal_register() -> list[dict]:
    """
    Federal Register API — Executive Orders, Proklamationen, Presidential Documents.
    Kein API-Key nötig. Täglich morgens aktualisiert.
    """
    url = (
        "https://www.federalregister.gov/api/v1/documents.json"
        "?conditions[president][]=donald-trump"
        "&conditions[type][]=PRESDOCU"
        "&conditions[type][]=EXECORD"
        "&conditions[type][]=PROCLAM"
        "&order=publication_date"
        "&per_page=20"
        "&fields[]=title&fields[]=publication_date&fields[]=abstract"
        "&fields[]=html_url&fields[]=document_number&fields[]=type"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        docs = r.json().get("results", [])
        log.info(f"  Federal Register: {len(docs)} Dokumente")
        normalized = []
        for d in docs:
            title    = d.get("title", "")
            abstract = d.get("abstract", "") or ""
            doc_type = d.get("type", "Presidential Document")
            normalized.append({
                "title":       f"[{doc_type}] {title}",
                "description": abstract[:500],
                "publishedAt": d.get("publication_date", ""),
                "url":         d.get("html_url", "https://www.federalregister.gov"),
                "_source":     "Federal Register",
            })
        return normalized
    except Exception as e:
        log.warning(f"  ⚠️  Federal Register Fehler: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# TRUMP BEKANNTE POSITIONEN  (manuell gepflegt, Stand: öffentliche Quellen)
# ─────────────────────────────────────────────────────────────────────────────
TRUMP_KNOWN_HOLDINGS = [
    {
        "asset":    "Trump Media & Technology Group (DJT)",
        "type":     "Aktie (börsennotiert)",
        "stake":    "~57 % / ~114 Mio. Shares",
        "disclosed": "2024-09-20",
        "source":   "SEC Form 4",
        "url":      "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000947033&type=4",
    },
    {
        "asset":    "TRUMP Memecoin (Official Trump)",
        "type":     "Kryptowährung",
        "stake":    "~80 % bei Trump-nahen Entitäten (200 Mio. von 250 Mio. Token)",
        "disclosed": "2025-01-17",
        "source":   "Projektwebsite / Whitepaper",
        "url":      "https://gettrumpmemes.com",
    },
    {
        "asset":    "MELANIA Memecoin",
        "type":     "Kryptowährung",
        "stake":    "~87.75 % bei Melania-nahen Entitäten",
        "disclosed": "2025-01-19",
        "source":   "Projektwebsite",
        "url":      "https://melaniameme.com",
    },
    {
        "asset":    "World Liberty Financial (WLFI Token)",
        "type":     "DeFi / Crypto",
        "stake":    "~75 % der Token bei Trump-Familie (ca. 22.5 Mrd. Token)",
        "disclosed": "2024-10-15",
        "source":   "WLFI Whitepaper",
        "url":      "https://worldlibertyfinancial.com",
    },
    {
        "asset":    "Diverse Aktien (Costco, Meta, Pfizer u.a.)",
        "type":     "Aktien-Portfolio",
        "stake":    "Wertspannen laut OGE Form 278e (keine exakten Stückzahlen)",
        "disclosed": "2025 (jährliche Offenlegung)",
        "source":   "OGE Form 278e",
        "url":      "https://extapps2.oge.gov/201/Presiden.nsf",
    },
]


_VALUE_SORT_KEY = {
    "Over $50,000,000": 0, "$25,000,001 - $50,000,000": 1,
    "$5,000,001 - $25,000,000": 2, "$1,000,001 - $5,000,000": 3,
    "$500,001 - $1,000,000": 4, "$250,001 - $500,000": 5,
    "$100,001 - $250,000": 6, "$50,001 - $100,000": 7,
    "$15,001 - $50,000": 8, "$1,001 - $15,000": 9,
    "None (or less than $1,001)": 10,
}

# Farbige Hinterlegung je Wertbereich (bg, text, label)
_VALUE_COLORS = {
    "Over $50,000,000":         ("#1d1d1f", "#ffffff", ">$50M"),
    "$25,000,001 - $50,000,000":("#2d3748", "#ffffff", "$25M–$50M"),
    "$5,000,001 - $25,000,000": ("#1e3a5f", "#ffffff", "$5M–$25M"),
    "$1,000,001 - $5,000,000":  ("#1a4971", "#e0f0ff", "$1M–$5M"),
    "$500,001 - $1,000,000":    ("#0f5499", "#e8f4fd", "$500K–$1M"),
    "$250,001 - $500,000":      ("#1565c0", "#e3f2fd", "$250K–$500K"),
    "$100,001 - $250,000":      ("#1976d2", "#e3f2fd", "$100K–$250K"),
    "$50,001 - $100,000":       ("#0288d1", "#e1f5fe", "$50K–$100K"),
    "$15,001 - $50,000":        ("#0097a7", "#e0f7fa", "$15K–$50K"),
    "$1,001 - $15,000":         ("#00796b", "#e0f2f1", "$1K–$15K"),
    "None (or less than $1,001)":("#9ca3af","#f5f5f7", "<$1K"),
}

def holdings_html_block() -> str:
    """Generiert strukturierten HTML-Block: 278e Snapshot + PTR Deltas."""

    # ── 278e Snapshot (Basis-Portfolio) ──────────────────────────────────────
    try:
        snap_year = conn.execute(
            "SELECT MAX(year) FROM trump_278e_snapshot"
        ).fetchone()[0]
        snap_rows = conn.execute("""
            SELECT asset_name, ticker, value_range, income_type, income_amount
            FROM trump_278e_snapshot WHERE year=?
            ORDER BY asset_name
        """, (snap_year,)).fetchall() if snap_year else []
    except Exception:
        snap_rows = []
        snap_year = None

    # ── PTR Transaktionen (Deltas seit 278e) ─────────────────────────────────
    try:
        ptr_rows = conn.execute("""
            SELECT ticker, asset_name,
                   SUM(CASE WHEN tx_type='KAUF'    THEN 1 ELSE 0 END) AS buys,
                   SUM(CASE WHEN tx_type='VERKAUF' THEN 1 ELSE 0 END) AS sells,
                   MAX(tx_date) AS last_date,
                   GROUP_CONCAT(tx_type || '|' || COALESCE(tx_date,'') ORDER BY created_at ASC) AS history
            FROM trump_holdings GROUP BY ticker
        """).fetchall()
        ptr_map = {r[0]: r for r in ptr_rows}  # ticker → row
    except Exception:
        ptr_map = {}

    th = ('style="padding:0 10px 6px 0;font-size:10px;font-weight:600;color:#9ca3af;'
          'text-align:left;text-transform:uppercase;letter-spacing:0.06em;"')
    td = 'style="padding:5px 10px 5px 0;font-size:11px;vertical-align:top;border-bottom:1px solid #f5f5f7;"'

    # ── Aktienportfolio: NUR Positionen mit PTR-Änderung + Zusammenfassung ─────
    # Geänderte Positionen (haben PTR-Einträge)
    changed_rows = []
    for ticker, ptr_row in sorted(ptr_map.items(), key=lambda x: x[1][4] or "", reverse=True):
        _, asset_name, buys, sells, last_date, history = ptr_row
        net = buys - sells
        txs = [x.split("|") for x in (history or "").split(",") if x]
        last_tx = txs[-1][0] if txs else ""
        prev_net = sum(1 if t[0]=="KAUF" else -1 if t[0]=="VERKAUF" else 0 for t in txs[:-1])

        if net <= 0:
            label, bg, fg = "SOLD OUT", "#fee2e2", "#dc2626"
        elif prev_net <= 0:
            label, bg, fg = "NEW BUY", "#d1fae5", "#059669"
        elif last_tx == "KAUF":
            label, bg, fg = "ADD", "#dbeafe", "#2563eb"
        elif last_tx == "VERKAUF":
            label, bg, fg = "REDUCED", "#fef3c7", "#d97706"
        else:
            label, bg, fg = "HELD", "#f5f5f7", "#6e6e73"

        # 278e-Wert wenn vorhanden
        snap_val = ""
        if snap_rows:
            snap_hit = next((r[2] for r in snap_rows if r[1] == ticker), None)
            snap_val = snap_hit or "nicht in 278e"

        changed_rows.append((ticker, asset_name, label, bg, fg, snap_val, last_date))

    # Sortiere nach Positionsgröße (278e-Wert), dann nach Datum
    changed_rows.sort(key=lambda r: (_VALUE_SORT_KEY.get(r[5], 9), r[6] or ""))

    # Gruppiere nach Wertbereich für farbige Trennzeilen
    current_group = None
    changed_html = ""
    for ticker, asset_name, label, bg, fg, snap_val, last_date in changed_rows:
        badge = (f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
                 f'padding:2px 5px;border-radius:3px;">{label}</span>')

        # Wertbereich-Farbe
        val_bg, val_fg, val_label = _VALUE_COLORS.get(
            snap_val, ("#f5f5f7", "#6e6e73", snap_val or "–"))

        # Gruppen-Trennzeile wenn neuer Wertbereich
        if snap_val != current_group and snap_val in _VALUE_COLORS:
            current_group = snap_val
            changed_html += (
                f'<tr>'
                f'<td colspan="5" style="padding:4px 8px;font-size:9px;font-weight:700;'
                f'letter-spacing:0.06em;text-transform:uppercase;'
                f'background:{val_bg};color:{val_fg};border-radius:4px;">'
                f'{val_label}</td></tr>'
            )

        val_chip = (
            f'<span style="background:{val_bg};color:{val_fg};font-size:9px;'
            f'font-weight:700;padding:1px 5px;border-radius:3px;">{val_label}</span>'
            if snap_val in _VALUE_COLORS else
            f'<span style="font-size:10px;color:#9ca3af;">{snap_val or "–"}</span>'
        )

        changed_html += (
            f'<tr>'
            f'<td {td} style="font-weight:600;color:#1d1d1f;">{ticker}</td>'
            f'<td {td} style="color:#6e6e73;">{(asset_name or "")[:40]}</td>'
            f'<td {td}>{val_chip}</td>'
            f'<td {td} style="color:#9ca3af;font-size:10px;white-space:nowrap;">{last_date or "–"}</td>'
            f'<td {td}>{badge}</td>'
            f'</tr>'
        )

    n_unchanged = len(snap_rows) - len(changed_rows) if snap_rows else 0
    unchanged_note = (
        f'<tr><td colspan="5" style="padding:6px 0;font-size:10px;color:#9ca3af;">'
        f'+ {n_unchanged} weitere Positionen unverändert seit 278e {snap_year}'
        f'</td></tr>'
    ) if n_unchanged > 0 else ""

    if not changed_html:
        changed_html = ('<tr><td colspan="5" style="padding:8px 0;font-size:11px;color:#9ca3af;">'
                        'Keine PTR-Änderungen bekannt.</td></tr>')

    basis_note = f"Basis: 278e {snap_year} · " if snap_year else ""
    stocks_section = f"""
<p style="margin:12px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">
  Trumps Positionen · {basis_note}Änderungen laut PTR
</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;">
  <thead><tr>
    <th {th}>Ticker</th><th {th}>Asset</th>
    <th {th}>278e Wert</th><th {th}>PTR Datum</th>
    <th {th}>Status</th>
  </tr></thead>
  <tbody>{changed_html}{unchanged_note}</tbody>
</table>"""

    # ── Strategische Beteiligungen + Krypto (kompakt, eine Zeile je) ──────────
    static_rows = (
        '<tr>'
        '<td style="padding:4px 10px 4px 0;font-size:11px;font-weight:600;color:#1d1d1f;border-bottom:1px solid #f5f5f7;">DJT</td>'
        '<td style="padding:4px 10px 4px 0;font-size:11px;color:#6e6e73;border-bottom:1px solid #f5f5f7;">Trump Media ~57%</td>'
        '<td style="padding:4px 0;font-size:10px;color:#9ca3af;border-bottom:1px solid #f5f5f7;">SEC Form 4 · 2024-09-20</td>'
        '</tr>'
    )
    for h in TRUMP_KNOWN_HOLDINGS:
        if h["type"] in ("Aktie (börsennotiert)", "Aktien-Portfolio"):
            continue
        static_rows += (
            f'<tr>'
            f'<td style="padding:4px 10px 4px 0;font-size:11px;font-weight:600;color:#1d1d1f;border-bottom:1px solid #f5f5f7;">'
            f'<a href="{h["url"]}" style="color:#0071e3;text-decoration:none;">{h["asset"][:30]}</a></td>'
            f'<td style="padding:4px 10px 4px 0;font-size:11px;color:#6e6e73;border-bottom:1px solid #f5f5f7;">{h["stake"][:35]}</td>'
            f'<td style="padding:4px 0;font-size:10px;color:#9ca3af;border-bottom:1px solid #f5f5f7;">{h["source"]} · {h["disclosed"]}</td>'
            f'</tr>'
        )

    static_section = f"""
<p style="margin:14px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">Strategisch &amp; Krypto</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;">
  <tbody>{static_rows}</tbody>
</table>"""

    return stocks_section + static_section


# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR  –  Trump Insider-Transaktions-Monitor
# ─────────────────────────────────────────────────────────────────────────────
TRUMP_CIK      = "0000947033"
EDGAR_BASE     = "https://data.sec.gov"
EDGAR_ARCHIVE  = "https://www.sec.gov/Archives/edgar/data/947033"
EDGAR_HEADERS  = {"User-Agent": "TrumpImpactMonitor research@trump-monitor.local"}

WATCHED_FORMS  = {"4", "4/A", "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}


def fetch_edgar_filings() -> list[dict]:
    """Holt aktuelle SEC-Filings für Trump CIK via EDGAR Submissions API."""
    try:
        r = requests.get(
            f"{EDGAR_BASE}/submissions/CIK{TRUMP_CIK}.json",
            headers=EDGAR_HEADERS, timeout=15,
        )
        r.raise_for_status()
        recent   = r.json().get("filings", {}).get("recent", {})
        forms    = recent.get("form", [])
        dates    = recent.get("filingDate", [])
        accnos   = recent.get("accessionNumber", [])
        docs     = recent.get("primaryDocument", [])
        results  = []
        for i, form in enumerate(forms):
            if form in WATCHED_FORMS:
                acc_clean = accnos[i].replace("-", "") if i < len(accnos) else ""
                results.append({
                    "form":      form,
                    "date":      dates[i]  if i < len(dates)  else "",
                    "accession": acc_clean,
                    "acc_fmt":   accnos[i] if i < len(accnos) else "",
                    "document":  docs[i]   if i < len(docs)   else "",
                })
        log.info(f"  EDGAR: {len(results)} relevante Filings gefunden")
        return results
    except Exception as e:
        log.warning(f"  ⚠️  EDGAR Submissions Fehler: {e}")
        return []


def parse_form4(accession: str, document: str) -> dict:
    """
    Parst Form-4-XML und gibt Transaktionsdetails zurück.
    Gibt leeres Dict zurück wenn kein XML verfügbar.
    Ältere EDGAR-Dokumente sind HTML-wrapped — strip_tags entfernt das.
    """
    url = f"{EDGAR_ARCHIVE}/{accession}/{document}"
    try:
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        r.raise_for_status()
        text = r.text
        # Ältere Filings: XML in HTML eingebettet → XML-Block extrahieren
        if text.strip().startswith("<"):
            xml_start = text.find("<?xml")
            if xml_start == -1:
                xml_start = text.find("<ownershipDocument")
            if xml_start > 0:
                text = text[xml_start:]
        # Ungültige XML-Entities bereinigen
        text = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)([^;]{1,20};?)', r'&amp;\1', text)
        root = ET.fromstring(text)

        def get(path: str) -> str:
            el = root.find(path)
            return el.text.strip() if el is not None and el.text else ""

        issuer = get(".//issuerName")
        ticker = get(".//issuerTradingSymbol")

        # Erste Non-Derivative-Transaktion auslesen
        tx = root.find(".//nonDerivativeTransaction")
        if tx is None:
            tx = root.find(".//derivativeTransaction")

        tx_date   = ""
        tx_shares = ""
        tx_price  = ""
        tx_type   = ""

        if tx is not None:
            tx_date   = (tx.findtext(".//transactionDate/value") or "").strip()
            tx_shares = (tx.findtext(".//transactionShares/value") or
                         tx.findtext(".//transactionTotalValue/value") or "").strip()
            tx_price  = (tx.findtext(".//transactionPricePerShare/value") or "").strip()
            code      = (tx.findtext(".//transactionAcquiredDisposedCode/value") or "").strip().upper()
            tx_type   = "KAUF" if code == "A" else "VERKAUF" if code == "D" else code

        return {
            "issuer":    issuer,
            "ticker":    ticker.upper() if ticker else "",
            "tx_type":   tx_type,
            "tx_shares": tx_shares,
            "tx_price":  tx_price,
            "tx_date":   tx_date,
        }
    except Exception as e:
        log.warning(f"  ⚠️  Form-4-Parse Fehler ({accession}): {e}")
        return {}


def send_edgar_alert(filing: dict, details: dict) -> None:
    """Sendet Apple-Style E-Mail Alert für neue EDGAR-Transaktion."""
    form      = filing["form"]
    filed     = filing["date"]
    acc_fmt   = filing["acc_fmt"]
    ticker    = details.get("ticker", "")
    issuer    = details.get("issuer", "Unbekannt")
    tx_type   = details.get("tx_type", "")
    tx_shares = details.get("tx_shares", "")
    tx_price  = details.get("tx_price", "")
    tx_date   = details.get("tx_date", filed)

    direction_emoji = "📈" if tx_type == "KAUF" else "📉" if tx_type == "VERKAUF" else "📋"
    edgar_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={TRUMP_CIK}&type={form}"

    price_str  = f"${float(tx_price):.2f}" if tx_price else "k.A."
    shares_str = f"{float(tx_shares):,.0f}" if tx_shares else "k.A."

    market_note = ""
    if ticker:
        data = fetch_market_data(ticker)
        if data:
            market_note = (
                f'<div style="background:#f5f5f7;border-radius:10px;padding:14px 18px;margin-top:12px;">'
                f'<pre style="margin:0;font-family:\'SF Mono\',Menlo,monospace;font-size:12px;'
                f'line-height:1.6;color:#1d1d1f;">{format_market_block(ticker)}</pre></div>'
            )

    html_body = f"""<!DOCTYPE html>
<html lang="de">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f5f7;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f7;padding:32px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

  <tr><td style="background:#1d1d1f;border-radius:16px 16px 0 0;padding:28px 32px;">
    <p style="margin:0 0 4px 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      SEC EDGAR · {form}
    </p>
    <h1 style="margin:0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:26px;font-weight:700;color:#f5f5f7;letter-spacing:-0.02em;">
      {direction_emoji} Trump: {tx_type or form} {ticker or issuer}
    </h1>
    <p style="margin:8px 0 0 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:13px;color:#6e6e73;">
      Eingereicht: {filed} &nbsp;·&nbsp; Transaktion: {tx_date}
    </p>
  </td></tr>

  <tr><td style="background:#ffffff;padding:24px 32px 20px;">
    <p style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Transaktionsdetails
    </p>
    <table style="border-collapse:collapse;width:100%;">
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;width:140px;">Emittent</td>
          <td style="padding:8px 0;font-size:13px;color:#1d1d1f;">{issuer}</td></tr>
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;">Ticker</td>
          <td style="padding:8px 0;font-size:14px;font-weight:700;color:#1d1d1f;">{ticker or "–"}</td></tr>
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;">Typ</td>
          <td style="padding:8px 0;font-size:13px;color:#1d1d1f;">{tx_type or form}</td></tr>
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;">Stückzahl</td>
          <td style="padding:8px 0;font-size:13px;color:#1d1d1f;">{shares_str}</td></tr>
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;">Preis/Stück</td>
          <td style="padding:8px 0;font-size:13px;color:#1d1d1f;">{price_str}</td></tr>
      <tr><td style="padding:8px 0;font-size:13px;color:#6e6e73;">Accession</td>
          <td style="padding:8px 0;font-size:11px;color:#6e6e73;">{acc_fmt}</td></tr>
    </table>
    {market_note}
    <p style="margin:14px 0 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:12px;">
      <a href="{edgar_url}" style="color:#0071e3;text-decoration:none;">Auf SEC EDGAR ansehen ↗</a>
    </p>
  </td></tr>

  <tr><td style="background:#ffffff;padding:0 32px;">
    <div style="border-top:1px solid #e5e5ea;"></div>
  </td></tr>

  <tr><td style="background:#ffffff;padding:20px 32px 24px;">
    <p style="margin:0 0 12px 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Alle bekannten Trump-Positionen
    </p>
    <div style="font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;">
      {holdings_html_block()}
    </div>
  </td></tr>

  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:16px 32px;
       border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;color:#6e6e73;">
      Quelle: SEC EDGAR · Filer CIK {TRUMP_CIK} ·
      Generiert: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

</table></td></tr></table>
</body></html>"""

    subject = f"{direction_emoji} SEC Alert: Trump {tx_type or form} {ticker or issuer} – {filed}"
    send_gmail(subject, html_body)
    log.info(f"  📨 EDGAR Alert gesendet: {form} | {ticker} | {tx_type} | {filed}")


EDGAR_LOOKBACK_DAYS = 90  # Nur Filings der letzten 90 Tage alertieren

def check_edgar_alerts() -> None:
    """
    Hauptfunktion für EDGAR-Monitoring.
    Prüft neue Filings, parst Form 4, sendet Alert bei unbekannten Transaktionen.
    Datum-Filter verhindert Spam mit historischen Filings beim ersten Run.
    """
    log.info("\n🏦 SEC EDGAR …")
    filings      = fetch_edgar_filings()
    new_count    = 0
    cutoff_edgar = (now_utc() - timedelta(days=EDGAR_LOOKBACK_DAYS)).date()

    for filing in filings:
        acc = filing["accession"]
        if not acc:
            continue

        # Datum-Filter: nur Filings der letzten 90 Tage alertieren
        try:
            filing_date = datetime.strptime(filing["date"], "%Y-%m-%d").date()
            if filing_date < cutoff_edgar:
                # Trotzdem in DB speichern damit kein Re-Alert nach Rollover
                conn.execute(
                    "INSERT OR IGNORE INTO edgar_filings VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (acc, filing["form"], filing["date"], "", "", "", "", "", "",
                     "HISTORICAL-NO-ALERT"),
                )
                continue
        except ValueError:
            pass

        exists = conn.execute(
            "SELECT 1 FROM edgar_filings WHERE accession=?", (acc,)
        ).fetchone()
        if exists:
            continue

        # Neue Filing — parsen
        details = {}
        if filing["form"] in ("4", "4/A") and filing["document"].endswith(".xml"):
            details = parse_form4(acc, filing["document"])

        # In DB speichern
        conn.execute(
            "INSERT OR IGNORE INTO edgar_filings VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                acc,
                filing["form"],
                filing["date"],
                details.get("ticker", ""),
                details.get("issuer", ""),
                details.get("tx_type", ""),
                details.get("tx_shares", ""),
                details.get("tx_price", ""),
                details.get("tx_date", ""),
                now_utc().isoformat(),
            ),
        )
        conn.commit()

        send_edgar_alert(filing, details)
        new_count += 1

    conn.commit()
    if new_count == 0:
        log.info("  EDGAR: Keine neuen Filings")


FINANCIAL_RSS_FEEDS = [
    # Reuters + AP haben öffentliche RSS-Feeds eingestellt (API-Key nötig) → entfernt
    # Ersatz: Seeking Alpha + Investopedia + Bloomberg (via Google News Proxy)
    ("CNBC Markets",        "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("MarketWatch",         "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Yahoo Finance",       "https://finance.yahoo.com/rss/topstories"),
    ("Seeking Alpha",       "https://seekingalpha.com/market_currents.xml"),
    ("WSJ Markets",         "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"),
    # Trump-spezifische Google News Feeds
    ("Google News Trump",         "https://news.google.com/rss/search?q=trump+tariff+trade&hl=en-US&gl=US&ceid=US:en"),
    ("Google News Trump Markets", "https://news.google.com/rss/search?q=trump+stock+market+executive+order&hl=en-US&gl=US&ceid=US:en"),
    ("Google News Trump Economy", "https://news.google.com/rss/search?q=trump+economy+sanctions+deal&hl=en-US&gl=US&ceid=US:en"),
    ("Politico Economy",          "https://rss.politico.com/economy.xml"),
]

def _rss_to_dict(entry, source: str) -> dict:
    return {
        "title":       entry.get("title", ""),
        "description": entry.get("summary", entry.get("description", "")),
        "publishedAt": entry.get("published", ""),
        "url":         entry.get("link", ""),
        "_source":     source,
    }

def fetch_financial_rss() -> list[dict]:
    results = []
    for name, url in FINANCIAL_RSS_FEEDS:
        try:
            feed = _parse_feed(url)
            arts = [_rss_to_dict(e, name) for e in feed.entries[:20]]
            results.extend(arts)
            log.info(f"  {name}: {len(arts)} Artikel")
        except Exception as ex:
            log.warning(f"  ⚠️  {name} Fehler: {ex}")
    return results

# whitehouse.gov/feed/ liefert seit dem Website-Relaunch 404 — diese drei
# Unterfeeds funktionieren und decken News, Executive Orders und Statements ab.
WHITEHOUSE_FEEDS = [
    "https://www.whitehouse.gov/news/feed/",
    "https://www.whitehouse.gov/presidential-actions/feed/",
    "https://www.whitehouse.gov/briefings-statements/feed/",
]

def fetch_whitehouse() -> list:
    entries: list = []
    seen_links: set[str] = set()
    for url in WHITEHOUSE_FEEDS:
        try:
            feed = _parse_feed(url)
            for e in feed.entries[:30]:
                link = e.get("link", "")
                if link and link in seen_links:
                    continue  # gleicher Beitrag in mehreren WH-Feeds
                if link:
                    seen_links.add(link)
                entries.append(e)
        except Exception as e:
            log.warning(f"  ⚠️  White House RSS Fehler ({url}): {e}")
    log.info(f"  White House RSS: {len(entries)} Einträge (3 Feeds, dedupliziert)")
    return entries


# ─────────────────────────────────────────────────────────────────────────────
# OGE 278-T MONITOR  –  Periodic Transaction Reports (kostenlos, kein Key)
# ─────────────────────────────────────────────────────────────────────────────
# Strategie: Drei Quellen parallel, erste erfolgreiche gewinnt.
# 1. OGE public portal (Lotus Notes, HTML-Parsing)
# 2. Whitehouse.gov Disclosures Seite (direkte PDF-Links)
# 3. Fallback: Google News RSS nach neuen PTR-Berichten
OGE_PORTAL_URL    = (
    "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
    "Periodic+Transaction+Reports?OpenDocument"
)
OGE_WH_URL        = "https://www.whitehouse.gov/disclosures/"
OGE_HEADERS       = {"User-Agent": "Mozilla/5.0 (compatible; TrumpMonitor/1.0)"}
OGE_TRUMP_PATTERN = re.compile(r"trump", re.IGNORECASE)


def _extract_pdf_links(html_text: str, base_url: str) -> list[str]:
    """Extrahiert alle absoluten PDF-URLs aus einem HTML-Dokument."""
    raw = re.findall(r'href=["\']([^"\']+\.pdf)["\']', html_text, re.IGNORECASE)
    links = []
    base = "/".join(base_url.split("/")[:3])
    for link in raw:
        if link.startswith("http"):
            links.append(link)
        elif link.startswith("/"):
            links.append(base + link)
        else:
            links.append(base + "/" + link)
    return links


def _oge_via_playwright() -> list[dict]:
    """
    OGE-Portal mit Playwright — rendert JavaScript, findet echte PDF-Links.
    Gibt [{pdf_url, source}] zurück.

    Läuft nur wenn OGE_FULL=1 (im Workflow nur 1× täglich gesetzt) — der
    Chromium-Download bei jedem Stundenlauf kostet sonst ~2 min CI-Zeit,
    obwohl neue PTRs nur alle paar Wochen erscheinen. Whitehouse.gov-
    Disclosures werden weiterhin stündlich geprüft (billiger HTTP-GET).
    """
    if os.getenv("OGE_FULL") != "1":
        log.info("  OGE Portal (Playwright) übersprungen — nur im Tageslauf (OGE_FULL=1)")
        return []
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.warning("Playwright nicht installiert — OGE Portal-Scraping übersprungen")
        return []

    found = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page    = browser.new_page(user_agent=OGE_HEADERS["User-Agent"])
            page.set_default_timeout(30_000)

            # OGE Portal laden und auf vollständiges Rendering warten
            page.goto(OGE_PORTAL_URL)
            try:
                page.wait_for_load_state("networkidle", timeout=20_000)
            except PWTimeout:
                pass  # Seite teilweise geladen reicht meist

            html = page.content()
            browser.close()

        pdfs = _extract_pdf_links(html, OGE_PORTAL_URL)
        for pdf in pdfs:
            if OGE_TRUMP_PATTERN.search(pdf) or OGE_TRUMP_PATTERN.search(html[:8000]):
                found.append({"pdf_url": pdf, "source": "OGE Portal"})
                log.info("OGE Portal: Trump-PTR gefunden: %s", pdf)

        log.info("OGE Portal (Playwright): %d Trump-PDFs gefunden", len(found))
    except Exception as e:
        log.warning("OGE Playwright Fehler: %s", e)
    return found


def fetch_oge_ptr_links() -> list[dict]:
    """
    Sucht auf OGE-Portal (Playwright) und Whitehouse.gov nach neuen Trump PTR-PDFs.
    Gibt [{pdf_url, source}] zurück.
    """
    found: list[dict] = []

    # — Quelle 1: OGE Public Portal via Playwright (JS-Rendering nötig) ───────
    found.extend(_oge_via_playwright())

    # — Quelle 2: Whitehouse.gov Disclosures (kein JS nötig) ──────────────────
    try:
        r = requests.get(OGE_WH_URL, headers=OGE_HEADERS, timeout=20)
        if r.ok:
            pdfs = _extract_pdf_links(r.text, OGE_WH_URL)
            for pdf in pdfs:
                if OGE_TRUMP_PATTERN.search(pdf):  # nur Trump-PTRs, nicht alle WH-Mitarbeiter
                    found.append({"pdf_url": pdf, "source": "Whitehouse.gov"})
    except Exception as e:
        log.warning("Whitehouse Disclosures: %s", e)

    # — Quelle 3: Google News RSS Fallback ———————————————————————————————————
    if not found:
        try:
            rss_url = (
                "https://news.google.com/rss/search"
                "?q=Trump+%22Periodic+Transaction+Report%22+OGE"
                "&hl=en-US&gl=US&ceid=US:en"
            )
            feed = _parse_feed(rss_url)
            for entry in feed.entries[:5]:
                title = entry.get("title", "")
                link  = entry.get("link", "")
                if "periodic transaction" in title.lower() or "278" in title:
                    found.append({"pdf_url": link, "source": "Google News / OGE"})
        except Exception as e:
            log.warning(f"  ⚠️  OGE Google News Fallback: {e}")

    # Deduplizieren
    seen_urls: set[str] = set()
    unique = []
    for item in found:
        if item["pdf_url"] not in seen_urls:
            seen_urls.add(item["pdf_url"])
            unique.append(item)
    return unique


def parse_oge_ptr_pdf(pdf_url: str, db_conn=None, pdf_bytes: bytes | None = None) -> list[dict]:
    """
    Lädt ein OGE 278-T PDF und extrahiert Transaktionszeilen mit pdfplumber.
    Gibt [{asset, tx_type, date, amount, row_text}] zurück.
    Fällt auf Text-Extraktion zurück wenn keine Tabelle gefunden.
    """
    try:
        import pdfplumber, io
    except ImportError:
        log.warning("  ⚠️  pdfplumber nicht installiert — PDF-Parsing übersprungen")
        return []
    try:
        if pdf_bytes is None:
            r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=30)
            r.raise_for_status()
            pdf_bytes = r.content
        transactions = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                # Tabellenbasiert
                for table in (page.extract_tables() or []):
                    for row in table:
                        if not row:
                            continue
                        cells = [str(c).strip() for c in row if c]
                        row_text = " | ".join(cells)
                        # Transaktionszeilen erkennen: P/S/E oder ausgeschrieben
                        if any(kw in row_text for kw in
                               ["Purchase", "Sale", "Exchange", " P ", " S ", " E "]):
                            # Asset-Name meist in erster Zelle
                            asset = cells[0] if cells else ""
                            tx_type = "?"
                            for c in cells:
                                if c in ("P", "Purchase"): tx_type = "KAUF"
                                elif c in ("S", "Sale"):    tx_type = "VERKAUF"
                                elif c in ("E", "Exchange"):tx_type = "TAUSCH"
                            transactions.append({
                                "asset":    asset,
                                "tx_type":  tx_type,
                                "row_text": row_text,
                            })
                # Fallback: Volltext durchsuchen wenn keine Tabelle
                if not transactions:
                    text = page.extract_text() or ""
                    for line in text.splitlines():
                        if any(kw in line for kw in
                               ["Purchase", "Sale", "Exchange", "P –", "S –"]):
                            transactions.append({
                                "asset":    line[:80],
                                "tx_type":  "?",
                                "row_text": line,
                            })
        _save_oge_holdings(transactions, pdf_url, db_conn=db_conn)
        return transactions
    except Exception as e:
        log.warning(f"  ⚠️  OGE PDF Parse Fehler ({pdf_url}): {e}")
        return []


# Ticker-Mapping für häufige Asset-Namen in OGE-PDFs
def _ticker_from_asset_name(name: str) -> str | None:
    """
    Matcht Asset-Namen (Aktien UND Anleihen) gegen entities.json.
    Gibt den bekannten Ticker zurück wenn ein Firmenname erkannt wird.

    Strategie:
    1. entities.json company-Namen durchsuchen (zuverlässig, ~7000 Firmen)
    2. Fallback: exaktes Ticker-Symbol in Klammern suchen, z.B. "(BA)"
    3. KEIN blindes Regex-Matching mehr → verhindert Phantom-Ticker aus Anleihe-OCR

    Beispiele:
    "BOEING COMPANY SENIOR NOTES DUE 2031" → BA
    "APPLE INC COMMON STOCK"              → AAPL
    "THF BOEING COMPANY PERP 3.3%"        → BA  (OCR-Präfix ignoriert)
    "OPCA GENERAL TRUST GRANT STREET..."  → None (kein Match → verworfen)
    """
    low = name.lower()

    # 1. entities.json company-Namen (längste zuerst für Präzision)
    for ticker, tiers in ENTITIES.items():
        for alias in tiers.get("company", []):
            if alias and len(alias) >= 4 and alias.lower() in low:
                return ticker.upper()

    # 2. Explizites Symbol in Klammern: "... (BA)" oder "... BA)"
    m = re.search(r'\(([A-Z]{1,5})\)', name)
    if m and m.group(1) in ENTITIES:
        return m.group(1)

    return None

def _save_oge_holdings(transactions: list[dict], pdf_url: str, db_conn=None) -> None:
    """Speichert OGE-Transaktionen in trump_holdings-Tabelle."""
    c   = db_conn or conn
    now = now_utc().isoformat()
    saved = 0
    for tx in transactions:
        ticker = _ticker_from_asset_name(tx.get("asset", ""))
        if not ticker:
            continue
        exists = c.execute(
            "SELECT 1 FROM trump_holdings WHERE ticker=? AND pdf_url=? AND tx_type=?",
            (ticker, pdf_url, tx.get("tx_type", "?")),
        ).fetchone()
        if exists:
            continue
        c.execute(
            "INSERT INTO trump_holdings (ticker,asset_name,tx_type,amount,tx_date,pdf_url,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (ticker, tx.get("asset",""), tx.get("tx_type","?"),
             tx.get("amount",""), tx.get("tx_date",""), pdf_url, now),
        )
        saved += 1
    if saved:
        c.commit()
        log.info(f"  💾 OGE Holdings: {saved} Ticker in trump_holdings gespeichert")


_AMOUNT_LOWER_RE = re.compile(r'\$([\d,]+)')

def _amount_lower_bound(amount: str) -> int:
    """Untere Grenze einer OGE-Betragsspanne: '$250,001 - $500,000' → 250001."""
    m = _AMOUNT_LOWER_RE.search(amount or "")
    if not m:
        return 0
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return 0


def _fmt_usd(v: int) -> str:
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M+"
    if v >= 1_000:
        return f"${v/1_000:.0f}K+"
    return f"${v}+"


def _perf_since_date(ticker: str, tx_date: str) -> float | None:
    """Kursperformance in % vom Transaktionsdatum bis heute (yfinance)."""
    d = None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime((tx_date or "").strip()[:10], fmt).date()
            break
        except ValueError:
            continue
    if not d:
        return None
    try:
        yf_sym = YF_TICKER_MAP.get(ticker, ticker)
        hist = yf.Ticker(yf_sym).history(start=d.isoformat(), auto_adjust=True, timeout=15)
        if not hist.empty and float(hist["Close"].iloc[0]) > 0:
            return (float(hist["Close"].iloc[-1]) / float(hist["Close"].iloc[0]) - 1) * 100
    except Exception as e:
        log.warning(f"  ⚠️  Perf seit Kauf ({ticker}): {e}")
    return None


_NEGATIVE_NEWS_KEYWORDS = (
    "lawsuit", "fraud", "investigation", "probe", "recall", "downgrade",
    "sec charges", "bankruptcy", "guidance cut", "misses", "plunge", "scandal",
)

def _recent_headlines(ticker: str, asset_name: str, limit: int = 3) -> list[str]:
    """Aktuelle Google-News-Schlagzeilen zum Unternehmen (Gratis-RSS)."""
    company = re.sub(r'\b(INC|CORP|CO|PLC|LTD|COM|NEW|CL A|CL B|CL C)\b', '',
                     (asset_name or "").upper()).strip().title()
    q = requests.utils.quote(f'{company or ticker} {ticker} stock', safe='')
    try:
        feed = _parse_feed(
            f"https://news.google.com/rss/search?q={q}+when:14d&hl=en-US&gl=US&ceid=US:en")
        return [e.get("title", "") for e in feed.entries[:limit] if e.get("title")]
    except Exception as e:
        log.warning(f"  ⚠️  News-Check ({ticker}): {e}")
        return []


def _interpret_top_buys(top_buys: list[dict]) -> list[dict]:
    """
    Reichert die größten Käufe eines PTR mit Performance seit Kaufdatum,
    aktuellen Schlagzeilen und einem Claude-Interpretationshinweis an.
    Eingabe: [{ticker, asset, badge, total_lower, txs}] — Ausgabe erweitert um
    first_date, perf_since, headlines, news_warning, rating, hinweis.
    """
    enriched = []
    for b in top_buys:
        earliest = min((tx.get("tx_date", "") for tx in b["txs"] if tx.get("tx_date")),
                       default="")
        perf  = _perf_since_date(b["ticker"], earliest)
        heads = _recent_headlines(b["ticker"], b["asset"])
        warn  = any(kw in h.lower() for h in heads for kw in _NEGATIVE_NEWS_KEYWORDS)
        enriched.append({**b, "first_date": earliest, "perf_since": perf,
                         "headlines": heads, "news_warning": warn,
                         "rating": "", "hinweis": ""})

    # Ein Haiku-Call für alle Positionen (Kostenoptimierung)
    try:
        lines = []
        for b in enriched:
            perf_s = f"{b['perf_since']:+.1f}%" if b["perf_since"] is not None else "unbekannt"
            lines.append(
                f"- {b['ticker']} ({b['asset'][:40]}): {b['badge']}, "
                f"Volumen ≥{_fmt_usd(b['total_lower'])} über {len(b['txs'])} Tx, "
                f"erster Kauf {b['first_date'] or 'unbekannt'}, "
                f"Performance seit Kauf: {perf_s}, "
                f"Schlagzeilen: {'; '.join(b['headlines']) or 'keine gefunden'}"
            )
        prompt = (
            "Du bewertest Käufe aus Trumps OGE-278-T-Filing für einen deutschen "
            "Privatanleger, der überlegt, Positionen trotz Melde-Latenz nachzukaufen.\n"
            "Positionen:\n" + "\n".join(lines) + "\n\n"
            "Für jede Position: nüchterne 1-2-Satz-Einschätzung auf Deutsch. "
            "Berücksichtige: Ist die Position seit Trumps Kauf schon stark gelaufen "
            "(Nachkauf teurer)? Gibt es negative Schlagzeilen? Wie groß/überzeugt "
            "wirkt der Kauf (NEW BUY mit hohem Volumen = stärkstes Signal)?\n"
            'Antworte NUR als JSON-Array: [{"ticker": "...", '
            '"rating": "attraktiv|neutral|vorsicht", "hinweis": "..."}]'
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        m = re.search(r'\[.*\]', resp.content[0].text, re.DOTALL)
        for row in (json.loads(m.group(0)) if m else []):
            for b in enriched:
                if b["ticker"] == str(row.get("ticker", "")).upper():
                    b["rating"]  = str(row.get("rating", "")).lower()
                    b["hinweis"] = str(row.get("hinweis", ""))
    except Exception as e:
        log.warning(f"  ⚠️  Interpretations-Call fehlgeschlagen: {e}")

    # Regelbasierter Fallback wenn Claude nichts geliefert hat
    for b in enriched:
        if b["rating"]:
            continue
        if b["news_warning"]:
            b["rating"]  = "vorsicht"
            b["hinweis"] = "Negative Schlagzeilen seit Kauf — vor Nachkauf prüfen."
        elif b["perf_since"] is not None and b["perf_since"] > 15:
            b["rating"]  = "neutral"
            b["hinweis"] = (f"Seit Trumps Kauf bereits {b['perf_since']:+.0f}% gelaufen — "
                            f"Einstieg deutlich teurer als sein Kaufkurs.")
        else:
            b["rating"]  = "attraktiv"
            b["hinweis"] = (f"{b['badge']} mit Volumen {_fmt_usd(b['total_lower'])}, "
                            f"Kurs seit Kauf kaum gelaufen — Latenz-Nachteil gering.")
    return enriched


def send_oge_alert(pdf_url: str, source: str, transactions: list[dict]) -> None:
    """
    Sendet PTR-Alert mit Portfolio-Kontext:
    Jede Transaktion wird gegen 278e-Snapshot und bestehende trump_holdings verglichen
    um NEW BUY / ADD / SELL / SOLD OUT zu bestimmen.
    """
    # Aktueller 278e-Snapshot als Basis
    snap_year = conn.execute(
        "SELECT MAX(year) FROM trump_278e_snapshot"
    ).fetchone()[0]
    snap_map = {}  # ticker → value_range
    if snap_year:
        for ticker, val in conn.execute(
            "SELECT ticker, value_range FROM trump_278e_snapshot WHERE year=? AND ticker IS NOT NULL",
            (snap_year,)
        ).fetchall():
            if ticker:
                snap_map[ticker.upper()] = val

    # Bisherige trump_holdings (alle PTRs vor diesem)
    prev_holdings = {}  # ticker → net (KAUF=+1, VERKAUF=-1)
    for ticker, tx_type in conn.execute(
        "SELECT ticker, tx_type FROM trump_holdings WHERE pdf_url!=? AND ticker IS NOT NULL",
        (pdf_url,)
    ).fetchall():
        if ticker:
            t = ticker.upper()
            prev_holdings[t] = prev_holdings.get(t, 0) + (
                1 if tx_type == "KAUF" else -1 if tx_type == "VERKAUF" else 0)

    def _context_badge(ticker, tx_type):
        """Bestimmt Badge basierend auf Portfolio-Kontext."""
        t = (ticker or "").upper()
        in_278e    = t in snap_map
        prev_net   = prev_holdings.get(t, 0)
        currently_held = in_278e or prev_net > 0

        if tx_type == "KAUF":
            if not currently_held:
                return "NEW BUY", "#d1fae5", "#059669"
            return "ADD", "#dbeafe", "#2563eb"
        if tx_type == "VERKAUF":
            # Netto nach diesem Verkauf
            all_buys  = conn.execute(
                "SELECT COUNT(*) FROM trump_holdings WHERE ticker=? AND tx_type='KAUF'", (t,)
            ).fetchone()[0]
            all_sells = conn.execute(
                "SELECT COUNT(*) FROM trump_holdings WHERE ticker=? AND tx_type='VERKAUF'", (t,)
            ).fetchone()[0]
            net_after = (all_buys - all_sells)
            if net_after <= 0 and not in_278e:
                return "SOLD OUT", "#fee2e2", "#dc2626"
            return "SELL", "#fef3c7", "#d97706"
        return "EXCHANGE", "#f5f5f7", "#6e6e73"

    # Transaktionen aufteilen: Aktien vs. Anleihen/Sonstiges
    stock_txs = []
    other_txs  = []
    for tx in transactions:
        ticker = _ticker_from_asset_name(tx.get("asset", ""))
        tx["_ticker"] = ticker
        if ticker:
            stock_txs.append(tx)
        else:
            other_txs.append(tx)

    th = ('style="padding:0 10px 6px 0;font-size:10px;font-weight:600;color:#9ca3af;'
          'text-align:left;text-transform:uppercase;letter-spacing:0.05em;"')

    def _td(content, color="#1d1d1f", bold=False, small=False):
        fs = "10px" if small else "11px"
        fw = "700" if bold else "400"
        return (f'<td style="padding:5px 10px 5px 0;font-size:{fs};font-weight:{fw};'
                f'color:{color};vertical-align:top;border-bottom:1px solid #f5f5f7;">'
                f'{content}</td>')

    # Aktien-Tabelle (mit Kontext-Badges)
    stock_rows_html = ""
    n_new_buy = n_add = n_sell = n_sold_out = 0
    buy_positions: dict[str, dict] = {}  # Ticker → aggregierte Kauf-Position
    for tx in stock_txs:
        ticker   = tx["_ticker"]
        label, bg, fg = _context_badge(ticker, tx.get("tx_type","?"))
        if label in ("NEW BUY", "ADD") and ticker:
            pos = buy_positions.setdefault(ticker, {
                "ticker": ticker, "asset": tx.get("asset", ""),
                "badge": label, "total_lower": 0, "txs": [],
            })
            pos["total_lower"] += _amount_lower_bound(tx.get("amount", ""))
            pos["txs"].append(tx)
            if label == "NEW BUY":
                pos["badge"] = "NEW BUY"  # NEW BUY dominiert über ADD
        badge = (f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
                 f'padding:2px 5px;border-radius:3px;">{label}</span>')
        snap_val = snap_map.get((ticker or "").upper(), "–")
        snap_info = f'278e: {snap_val}' if snap_val != "–" else "nicht in 278e"
        if label == "NEW BUY":    n_new_buy += 1
        elif label == "ADD":      n_add += 1
        elif label == "SELL":     n_sell += 1
        elif label == "SOLD OUT": n_sold_out += 1
        stock_rows_html += (
            f'<tr style="background:{"#fafffe" if label in ("NEW BUY","ADD") else "#fff8f8" if label in ("SELL","SOLD OUT") else "#fff"};">'
            + _td(ticker or "–", bold=True)
            + _td(tx.get("asset","")[:45], "#6e6e73")
            + _td(tx.get("tx_type","?"), fg, bold=True)
            + _td(tx.get("tx_date","–"), "#6e6e73")
            + _td(tx.get("amount","–"))
            + _td(snap_info, "#9ca3af", small=True)
            + _td(badge)
            + "</tr>"
        )

    if not stock_rows_html:
        stock_rows_html = ('<tr><td colspan="7" style="padding:10px 0;font-size:11px;color:#9ca3af;">'
                           'Keine Aktien-Transaktionen in diesem PTR erkannt.</td></tr>')

    # Anleihen-Tabelle (kompakt, ohne Kontext)
    bond_rows_html = ""
    for tx in other_txs[:30]:
        color = "#d1fae5" if tx.get("tx_type") == "KAUF" else \
                "#fee2e2" if tx.get("tx_type") == "VERKAUF" else "#f5f5f7"
        bond_rows_html += (
            f'<tr style="background:{color};">'
            + _td(tx.get("asset","")[:60], "#6e6e73", small=True)
            + _td(tx.get("tx_type","?"), "#1d1d1f", bold=True, small=True)
            + _td(tx.get("tx_date","–"), "#6e6e73", small=True)
            + _td(tx.get("amount","–"), "#1d1d1f", small=True)
            + "</tr>"
        )
    bond_note = f" (erste 30 von {len(other_txs)})" if len(other_txs) > 30 else ""

    # Top-Käufe: größte NEW BUY/ADD-Positionen mit Interpretation & Nachkauf-Check
    top_buys = sorted(buy_positions.values(),
                      key=lambda p: p["total_lower"], reverse=True)[:6]
    interp_html = ""
    if top_buys:
        try:
            enriched = _interpret_top_buys(top_buys)
        except Exception as e:
            log.warning(f"  ⚠️  Top-Buy-Interpretation: {e}")
            enriched = []
        rating_style = {
            "attraktiv": ("#d1fae5", "#059669"),
            "neutral":   ("#fef3c7", "#d97706"),
            "vorsicht":  ("#fee2e2", "#dc2626"),
        }
        rows = ""
        for b in enriched:
            bg_r, fg_r = rating_style.get(b["rating"], ("#f5f5f7", "#6e6e73"))
            perf_s = (f"{b['perf_since']:+.1f}%" if b["perf_since"] is not None else "–")
            perf_c = ("#059669" if (b["perf_since"] or 0) >= 0 else "#dc2626")
            news_s = "⚠️ ja" if b["news_warning"] else "keine"
            rows += (
                f'<tr style="background:#fff;">'
                f'<td style="padding:8px 10px 8px 0;font-size:12px;font-weight:700;'
                f'vertical-align:top;border-bottom:1px solid #f5f5f7;">{b["ticker"]}<br>'
                f'<span style="font-size:9px;font-weight:400;color:#9ca3af;">'
                f'{b["asset"][:32]}</span></td>'
                f'<td style="padding:8px 10px 8px 0;font-size:11px;vertical-align:top;'
                f'border-bottom:1px solid #f5f5f7;">'
                f'<span style="background:{"#d1fae5" if b["badge"]=="NEW BUY" else "#dbeafe"};'
                f'color:{"#059669" if b["badge"]=="NEW BUY" else "#2563eb"};font-size:9px;'
                f'font-weight:700;padding:2px 5px;border-radius:3px;">{b["badge"]}</span><br>'
                f'<span style="font-size:10px;color:#6e6e73;">{_fmt_usd(b["total_lower"])}'
                f' · {len(b["txs"])} Tx</span></td>'
                f'<td style="padding:8px 10px 8px 0;font-size:11px;font-weight:700;'
                f'color:{perf_c};vertical-align:top;border-bottom:1px solid #f5f5f7;">'
                f'{perf_s}<br><span style="font-size:9px;font-weight:400;color:#9ca3af;">'
                f'seit {b["first_date"] or "?"}</span></td>'
                f'<td style="padding:8px 10px 8px 0;font-size:10px;color:#6e6e73;'
                f'vertical-align:top;border-bottom:1px solid #f5f5f7;">{news_s}</td>'
                f'<td style="padding:8px 0;font-size:10px;vertical-align:top;'
                f'border-bottom:1px solid #f5f5f7;">'
                f'<span style="background:{bg_r};color:{fg_r};font-size:9px;font-weight:700;'
                f'padding:2px 5px;border-radius:3px;text-transform:uppercase;">'
                f'{b["rating"] or "?"}</span><br>'
                f'<span style="color:#3c3c43;">{b["hinweis"]}</span></td>'
                f'</tr>'
            )
        if rows:
            interp_html = f"""
  <tr><td style="background:#fff;padding:8px 32px 8px;">
    <p style="margin:12px 0 2px;font-size:10px;font-weight:700;color:#059669;
       text-transform:uppercase;letter-spacing:0.06em;">
      💡 Größte Käufe · Interpretation &amp; Nachkauf-Check
    </p>
    <table style="border-collapse:collapse;width:100%;">
      <thead><tr>
        <th {th}>Ticker</th><th {th}>Kauf</th><th {th}>Seit Kauf</th>
        <th {th}>Negativ-News</th><th {th}>Einschätzung</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <p style="margin:6px 0 0;font-size:9px;color:#9ca3af;">
      Betrag = untere Grenze der gemeldeten OGE-Spannen, summiert über alle Käufe im PTR.
      Performance ab erstem Kaufdatum im Filing bis heute. Keine Anlageberatung.
    </p>
  </td></tr>"""

    html_body = f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f7;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f7;padding:32px 0;">
<tr><td align="center">
<table width="700" cellpadding="0" cellspacing="0" style="max-width:700px;width:100%;">

  <tr><td style="background:#1d1d1f;border-radius:16px 16px 0 0;padding:28px 32px;">
    <p style="margin:0 0 4px;font-family:-apple-system,sans-serif;font-size:11px;
       font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      OGE Form 278-T · Periodic Transaction Report
    </p>
    <h1 style="margin:0;font-family:-apple-system,sans-serif;font-size:22px;
       font-weight:700;color:#f5f5f7;letter-spacing:-0.02em;">
      Neuer Trump PTR · {now_utc().strftime('%d.%m.%Y')}
    </h1>
    <p style="margin:6px 0 0;font-family:-apple-system,sans-serif;font-size:12px;color:#6e6e73;">
      {len(transactions)} Transaktionen gesamt · {len(stock_txs)} Aktien · {len(other_txs)} Anleihen/Sonstiges
    </p>
  </td></tr>

  <tr><td style="background:#fff;padding:16px 32px 8px;">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      {''.join(f'<td style="text-align:center;padding:8px 10px;border-radius:8px;background:{bg};">'
               f'<div style="font-size:18px;font-weight:700;color:{fg};">{n}</div>'
               f'<div style="font-size:9px;font-weight:700;color:{fg};text-transform:uppercase;">{lbl}</div>'
               f'</td>'
               for n, lbl, bg, fg in [
                   (n_new_buy, "New Buy", "#d1fae5", "#059669"),
                   (n_add,     "Add",     "#dbeafe", "#2563eb"),
                   (n_sell,    "Sell",    "#fef3c7", "#d97706"),
                   (n_sold_out,"Sold Out","#fee2e2", "#dc2626"),
                   (len(stock_txs), "Aktien", "#f5f5f7", "#1d1d1f"),
                   (len(other_txs), "Anleihen", "#f5f5f7", "#6e6e73"),
               ])}
    </tr></table>
  </td></tr>
{interp_html}
  <tr><td style="background:#fff;padding:8px 32px 24px;">
    <p style="margin:12px 0 6px;font-size:10px;font-weight:700;color:#6e6e73;
       text-transform:uppercase;letter-spacing:0.06em;">
      Aktien · Portfolio-Kontext vs. 278e {snap_year or "–"}
    </p>
    <table style="border-collapse:collapse;width:100%;">
      <thead><tr>
        <th {th}>Ticker</th><th {th}>Asset</th><th {th}>Typ</th>
        <th {th}>Datum</th><th {th}>Betrag</th>
        <th {th}>278e Basis</th><th {th}>Änderung</th>
      </tr></thead>
      <tbody>{stock_rows_html}</tbody>
    </table>
  </td></tr>

  {'<tr><td style="background:#fff;padding:0 32px 24px;"><p style="margin:0 0 6px;font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:0.06em;">Anleihen &amp; Sonstiges' + bond_note + '</p><table style="border-collapse:collapse;width:100%;"><thead><tr><th ' + th + '>Asset</th><th ' + th + '>Typ</th><th ' + th + '>Datum</th><th ' + th + '>Betrag</th></tr></thead><tbody>' + bond_rows_html + '</tbody></table></td></tr>' if bond_rows_html else ''}

  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:16px 32px;
       border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-family:-apple-system,sans-serif;font-size:11px;color:#6e6e73;">
      OGE 278-T · {source} · 278e-Basis: {snap_year or "nicht verfügbar"} ·
      <a href="{pdf_url}" style="color:#0071e3;text-decoration:none;">PDF ↗</a> ·
      {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

</table></td></tr></table>
</body></html>"""

    top_hint = (f" · Top: {top_buys[0]['ticker']} {_fmt_usd(top_buys[0]['total_lower'])}"
                if top_buys else "")
    subject = (f"🏛️ Trump PTR: {n_new_buy} NEW BUY · {n_add} ADD · "
               f"{n_sell} SELL · {n_sold_out} SOLD OUT · "
               f"{len(transactions)} Tx gesamt{top_hint}")
    send_gmail(subject, html_body)
    log.info(f"  📨 PTR Alert: {len(transactions)} Tx · {len(stock_txs)} Aktien · "
             f"{n_new_buy} NEW, {n_add} ADD, {n_sell} SELL, {n_sold_out} SOLD OUT")


OGE_LOOKBACK_DAYS = 90  # Nur PDFs der letzten 90 Tage alertieren (wie EDGAR)

def _oge_date_from_url(pdf_url: str) -> str:
    """Extrahiert YYYY/MM aus whitehouse.gov-URL z.B. /2025/06/..."""
    m = re.search(r'/(\d{4})/(\d{2})/', pdf_url)
    return f"{m.group(1)}-{m.group(2)}-01" if m else ""


# ─────────────────────────────────────────────────────────────────────────────
# OGE FORM 278e  –  Jährlicher Snapshot via Claude Vision OCR
# ─────────────────────────────────────────────────────────────────────────────
_278E_PART6_START  = 86   # Fallback-Seite wenn Auto-Detection fehlschlägt
_278E_PART6_END    = 146  # Fallback-Ende
_VISION_BATCH_SIZE = 5    # Seiten pro API-Call (max ~8 sinnvoll)
_VISION_MAX_ERRORS = 3    # Circuit-Breaker: Abbruch nach N aufeinanderfolgenden Fehlern

_PTR_EXTRACT_PROMPT = """This is a page from Trump's OGE Form 278-T Periodic Transaction Report.
Extract EVERY transaction row from the table.
Return ONLY a JSON array, no other text. Each element:
{"asset": "full description as written", "type": "Purchase or Sale or Exchange", "date": "MM/DD/YYYY", "amount": "dollar range as written e.g. $1,001 - $15,000"}
Skip header rows, cover page, signature pages, and blank rows.
If this page has no transaction table, return [].
Important: include ALL transactions, even municipal bonds and fixed income."""

_278E_EXTRACT_PROMPT = """These are pages from Part 6 (Other Assets and Income) of Trump's OGE Form 278e annual financial disclosure.
Extract EVERY row from the asset tables visible in these pages.
The table has columns: # | Description | EIF | Value | Income Type | Income Amount.
Return ONLY a valid JSON array — no explanation, no markdown, no preamble. Each element:
{"asset": "full description as written", "value": "value range e.g. $1,001 - $15,000", "income_type": "e.g. DIVIDEND or empty string", "income_amount": "e.g. $201 - $1,000 or empty string"}
Include ALL rows including those with 'None (or less than $1,001)' as value.
Skip only the column header row and section dividers like 'INVESTMENT ACCOUNT #4'.
If no asset rows are visible, return []."""

def parse_278e_via_claude_vision(pdf_url: str, year: int) -> int:
    """
    Lädt 278e-PDF, rendert Part-6-Seiten als Bilder, schickt sie an Claude Vision,
    speichert extrahierte Positionen in trump_278e_snapshot.
    Gibt Anzahl gespeicherter Zeilen zurück.
    """
    try:
        import fitz
    except ImportError:
        log.warning("pymupdf nicht installiert — 278e Vision-Parsing übersprungen")
        return 0

    log.info(f"  📄 278e Vision-OCR: Lade PDF ({pdf_url[:60]}…)")
    try:
        r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=60)
        r.raise_for_status()
        doc = fitz.open(stream=r.content, filetype="pdf")
    except Exception as e:
        log.warning(f"  ⚠️  278e PDF Download Fehler: {e}")
        return 0

    # Bestehende Einträge für dieses Jahr löschen (Neuparse überschreibt)
    conn.execute("DELETE FROM trump_278e_snapshot WHERE year=? AND pdf_url=?",
                 (year, pdf_url))
    conn.commit()

    total_saved = 0
    # Auto-Detection Part 6: suche via fitz-Text nach "Part 6" Header,
    # falle auf Hardcode zurück wenn nicht gefunden (Bild-Scan ohne Text-Layer)
    part6_start = _278E_PART6_START - 1  # 0-indexed
    part6_end   = min(_278E_PART6_END, len(doc))
    for i, page in enumerate(doc):
        txt = page.get_text().strip()
        if "Part 6" in txt and "Other Assets" in txt:
            part6_start = i
            log.info(f"  📄 278e Auto-Detection: Part 6 beginnt auf Seite {i+1}")
            break
        # Auch Bilderseiten: via kleines Thumbnail und Haiku prüfen (nur erste 10 Seiten ab Kandidat)
    # Part-7-Start = Ende von Part 6
    for i in range(part6_start + 1, len(doc)):
        txt = doc[i].get_text().strip()
        if "Part 7" in txt and len(txt) > 50:
            part6_end = i
            log.info(f"  📄 278e Auto-Detection: Part 6 endet auf Seite {i} (Part 7 ab S.{i+1})")
            break
    # Auto-Detection-Sanity-Check: erkannte Startseite darf nicht > Ende sein
    if part6_start >= part6_end:
        log.warning(f"  ⚠️  278e Auto-Detection ungültig (S.{part6_start+1} ≥ {part6_end}), nutze Fallback")
        part6_start = _278E_PART6_START - 1
        part6_end   = min(_278E_PART6_END, len(doc))

    pages_to_parse = list(range(part6_start, part6_end))
    batches = [pages_to_parse[i:i+_VISION_BATCH_SIZE]
               for i in range(0, len(pages_to_parse), _VISION_BATCH_SIZE)]
    log.info(f"  📄 278e: {len(pages_to_parse)} Seiten in {len(batches)} Batches à {_VISION_BATCH_SIZE} (S.{part6_start+1}–{part6_end})")

    consecutive_errors = 0
    now = now_utc().isoformat()

    for batch_idx, batch in enumerate(batches):
        if consecutive_errors >= _VISION_MAX_ERRORS:
            log.warning(f"  ⛔ 278e Circuit-Breaker: {_VISION_MAX_ERRORS} Fehler in Folge → Abbruch")
            break
        try:
            content = []
            for pg_idx in batch:
                pix = doc[pg_idx].get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
                img_b64 = base64.standard_b64encode(pix.tobytes("png")).decode()
                content.append({"type": "text", "text": f"Page {pg_idx+1}:"})
                content.append({"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": img_b64
                }})
            content.append({"type": "text", "text": _278E_EXTRACT_PROMPT})

            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=8192,
                messages=[{"role": "user", "content": content}],
            )
            raw  = resp.content[0].text.strip()
            m    = re.search(r'\[.*\]', raw, re.DOTALL)
            rows = json.loads(m.group(0)) if m else []

            for row in rows:
                asset_name  = str(row.get("asset", "")).strip()
                value_range = str(row.get("value", "")).strip()
                income_type = str(row.get("income_type", "")).strip()
                income_amt  = str(row.get("income_amount", "")).strip()
                if not asset_name:
                    continue
                ticker = _ticker_from_asset_name(asset_name)
                conn.execute(
                    "INSERT INTO trump_278e_snapshot "
                    "(year,asset_name,ticker,value_range,income_type,income_amount,pdf_url,parsed_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (year, asset_name, ticker, value_range, income_type, income_amt, pdf_url, now),
                )
                total_saved += 1
            conn.commit()
            consecutive_errors = 0
            log.info(f"    Batch {batch_idx+1}/{len(batches)} (S.{batch[0]+1}-{batch[-1]+1}): {len(rows)} Positionen")

        except json.JSONDecodeError:
            consecutive_errors += 1
            log.warning(f"    Batch {batch_idx+1}: JSON-Fehler ({consecutive_errors}/{_VISION_MAX_ERRORS})")
        except Exception as e:
            consecutive_errors += 1
            log.warning(f"    Batch {batch_idx+1}: {e} ({consecutive_errors}/{_VISION_MAX_ERRORS})")

    log.info(f"  ✅ 278e: {total_saved} Positionen gespeichert (Jahr {year})")
    if total_saved > 0:
        send_278e_alert(pdf_url, year)
    else:
        log.warning("  ⚠️  278e: 0 Positionen geparst — kein Alert verschickt")
    return total_saved


def send_278e_alert(pdf_url: str, year: int) -> None:
    """
    Sendet E-Mail mit vollständigem 278e-Portfolio-Vergleich:
    neues Jahr vs. Vorjahr → NEW BUY / ADD / SOLD OUT / REDUCED / UNCHANGED
    """
    # Neues Jahr
    new_rows = conn.execute("""
        SELECT asset_name, ticker, value_range, income_type, income_amount
        FROM trump_278e_snapshot WHERE year=? ORDER BY asset_name
    """, (year,)).fetchall()

    # Vorjahr
    prev_year = conn.execute("""
        SELECT MAX(year) FROM trump_278e_snapshot WHERE year<?
    """, (year,)).fetchone()[0]
    prev_map = {}
    if prev_year:
        prev_rows = conn.execute("""
            SELECT asset_name, ticker, value_range FROM trump_278e_snapshot WHERE year=?
        """, (prev_year,)).fetchall()
        prev_map = {r[0].strip().upper(): (r[1], r[2]) for r in prev_rows}

    # Wert-Rang für Sortierung und Vergleich
    def _rank(v):
        return _VALUE_SORT_KEY.get(v or "", 10)

    def _change_badge(asset_name, new_val):
        key = asset_name.strip().upper()
        if key not in prev_map:
            return "NEW BUY", "#d1fae5", "#059669"
        old_val = prev_map[key][1]
        if _rank(new_val) < _rank(old_val):
            return "ADD", "#dbeafe", "#2563eb"
        if _rank(new_val) > _rank(old_val):
            return "REDUCED", "#fef3c7", "#d97706"
        return "UNCHANGED", "#f5f5f7", "#6e6e73"

    # Verkaufte Positionen (im Vorjahr, nicht mehr im neuen Jahr)
    new_keys = {r[0].strip().upper() for r in new_rows}
    sold_out = []
    if prev_year:
        for asset_name, ticker, old_val in conn.execute("""
            SELECT asset_name, ticker, value_range FROM trump_278e_snapshot WHERE year=?
            ORDER BY asset_name
        """, (prev_year,)).fetchall():
            if asset_name.strip().upper() not in new_keys:
                sold_out.append((asset_name, ticker, old_val))

    # Zeilen bauen — sortiert nach Positionsgröße, dann Badge
    sorted_rows = sorted(new_rows, key=lambda r: _rank(r[2]))

    badge_order = {"NEW BUY": 0, "ADD": 1, "REDUCED": 2, "UNCHANGED": 3}
    sorted_rows = sorted(new_rows,
        key=lambda r: (_rank(r[2]), badge_order.get(
            _change_badge(r[0], r[2])[0], 9)))

    def _td(content, color="#1d1d1f", bold=False, small=False):
        fs = "10px" if small else "11px"
        fw = "700" if bold else "400"
        return (f'<td style="padding:5px 10px 5px 0;font-size:{fs};font-weight:{fw};'
                f'color:{color};vertical-align:top;border-bottom:1px solid #f5f5f7;">'
                f'{content}</td>')

    rows_html = ""
    for asset_name, ticker, value_range, income_type, income_amount in sorted_rows:
        label, bg, fg = _change_badge(asset_name, value_range)
        badge = (f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
                 f'padding:2px 5px;border-radius:3px;">{label}</span>')
        inc = f"{income_type}: {income_amount}" if income_type and income_amount else "–"
        rows_html += (
            f'<tr style="background:{"#fafffe" if label != "UNCHANGED" else "#fff"};">'
            + _td(ticker or "–", bold=True)
            + _td(asset_name[:50], "#6e6e73")
            + _td(value_range or "–")
            + _td(inc, "#6e6e73", small=True)
            + _td(badge)
            + "</tr>"
        )

    # Sold-out Zeilen
    sold_html = ""
    for asset_name, ticker, old_val in sold_out:
        badge = ('<span style="background:#fee2e2;color:#dc2626;font-size:9px;font-weight:700;'
                 'padding:2px 5px;border-radius:3px;">SOLD OUT</span>')
        sold_html += (
            '<tr style="background:#fff8f8;">'
            + _td(ticker or "–", "#9ca3af", bold=True)
            + _td(f'<s>{asset_name[:50]}</s>', "#9ca3af")
            + _td(f'<s>{old_val or "–"}</s>', "#9ca3af")
            + _td("–", "#9ca3af", small=True)
            + _td(badge)
            + "</tr>"
        )

    th = ('style="padding:0 10px 6px 0;font-size:10px;font-weight:600;color:#9ca3af;'
          'text-align:left;text-transform:uppercase;letter-spacing:0.05em;"')

    # Statistik
    n_new    = sum(1 for r in new_rows if _change_badge(r[0], r[2])[0] == "NEW BUY")
    n_add    = sum(1 for r in new_rows if _change_badge(r[0], r[2])[0] == "ADD")
    n_red    = sum(1 for r in new_rows if _change_badge(r[0], r[2])[0] == "REDUCED")
    n_unch   = sum(1 for r in new_rows if _change_badge(r[0], r[2])[0] == "UNCHANGED")
    n_sold   = len(sold_out)
    prev_label = f"vs. {prev_year}" if prev_year else "kein Vorjahr verfügbar"

    html_body = f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f7;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f7;padding:32px 0;">
<tr><td align="center">
<table width="680" cellpadding="0" cellspacing="0" style="max-width:680px;width:100%;">

  <tr><td style="background:#1d1d1f;border-radius:16px 16px 0 0;padding:28px 32px;">
    <p style="margin:0 0 4px;font-family:-apple-system,sans-serif;font-size:11px;
       font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      OGE Form 278e · Jährliche Finanz-Disclosure
    </p>
    <h1 style="margin:0;font-family:-apple-system,sans-serif;font-size:22px;
       font-weight:700;color:#f5f5f7;letter-spacing:-0.02em;">
      Trump Portfolio Update {year} <span style="color:#6e6e73;font-size:14px;font-weight:400;">{prev_label}</span>
    </h1>
  </td></tr>

  <tr><td style="background:#fff;padding:20px 32px 8px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        {''.join(f'<td style="text-align:center;padding:8px 12px;border-radius:8px;background:{bg};">'
                 f'<div style="font-size:20px;font-weight:700;color:{fg};">{n}</div>'
                 f'<div style="font-size:10px;font-weight:600;color:{fg};text-transform:uppercase;">{lbl}</div>'
                 f'</td>'
                 for n, lbl, bg, fg in [
                     (n_new, "New Buy", "#d1fae5", "#059669"),
                     (n_add, "Add", "#dbeafe", "#2563eb"),
                     (n_red, "Reduced", "#fef3c7", "#d97706"),
                     (n_sold, "Sold Out", "#fee2e2", "#dc2626"),
                     (n_unch, "Unchanged", "#f5f5f7", "#6e6e73"),
                     (len(new_rows), "Total", "#1d1d1f", "#f5f5f7"),
                 ])}
      </tr>
    </table>
  </td></tr>

  <tr><td style="background:#fff;padding:16px 32px 24px;">
    <table style="border-collapse:collapse;width:100%;">
      <thead><tr>
        <th {th}>Ticker</th><th {th}>Asset</th>
        <th {th}>Wert (Range)</th><th {th}>Einkommen</th>
        <th {th}>Änderung</th>
      </tr></thead>
      <tbody>
        {rows_html}
        {sold_html}
      </tbody>
    </table>
  </td></tr>

  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:16px 32px;
       border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-family:-apple-system,sans-serif;font-size:11px;color:#6e6e73;">
      OGE Form 278e · Jahr {year} · {len(new_rows)} Positionen ·
      <a href="{pdf_url}" style="color:#0071e3;text-decoration:none;">PDF öffnen ↗</a> ·
      Generiert: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

</table></td></tr></table>
</body></html>"""

    subject = (f"🏛️ Trump Portfolio 278e {year}: "
               f"{n_new} NEW · {n_add} ADD · {n_sold} SOLD OUT · {n_red} REDUCED")
    send_gmail(subject, html_body)
    log.info(f"  📨 278e Alert: {len(new_rows)} Positionen, {n_new} NEW, {n_add} ADD, "
             f"{n_sold} SOLD OUT, {n_red} REDUCED")


def parse_ptr_via_claude_vision(pdf_url: str, db_conn=None, pdf_bytes: bytes | None = None) -> list[dict]:
    """
    Parst ein OGE 278-T PTR-PDF via Claude Vision.
    Gibt Liste von Transaktions-Dicts zurück und speichert Aktien in trump_holdings.
    """
    try:
        import fitz
    except ImportError:
        log.warning("pymupdf nicht installiert — PTR Vision-Parsing übersprungen")
        return []

    try:
        if pdf_bytes is None:
            r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=30)
            r.raise_for_status()
            pdf_bytes = r.content
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        log.warning(f"  ⚠️  PTR Vision: PDF Download Fehler: {e}")
        return []

    all_transactions = []
    c    = db_conn or conn
    now  = now_utc().isoformat()
    pages = list(range(len(doc)))
    batches = [pages[i:i+_VISION_BATCH_SIZE]
               for i in range(0, len(pages), _VISION_BATCH_SIZE)]
    consecutive_errors = 0

    for batch_idx, batch in enumerate(batches):
        if consecutive_errors >= _VISION_MAX_ERRORS:
            log.warning(f"  ⛔ PTR Circuit-Breaker nach {_VISION_MAX_ERRORS} Fehlern → Abbruch")
            break
        try:
            content = []
            for pg_idx in batch:
                pix = doc[pg_idx].get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
                img_b64 = base64.standard_b64encode(pix.tobytes("png")).decode()
                content.append({"type": "text", "text": f"Page {pg_idx+1}:"})
                content.append({"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": img_b64
                }})
            content.append({"type": "text", "text": _PTR_EXTRACT_PROMPT})

            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=8192,
                messages=[{"role": "user", "content": content}],
            )
            raw  = resp.content[0].text.strip()
            m    = re.search(r'\[.*\]', raw, re.DOTALL)
            rows = json.loads(m.group(0)) if m else []
            consecutive_errors = 0

            for row in rows:
                asset       = str(row.get("asset", "")).strip()
                tx_type_raw = str(row.get("type", "")).strip().lower()
                date        = str(row.get("date", "")).strip()
                amount      = str(row.get("amount", "")).strip()
                if not asset:
                    continue
                tx_type = ("KAUF"    if "purchase" in tx_type_raw
                           else "VERKAUF" if "sale"     in tx_type_raw
                           else "TAUSCH"  if "exchange" in tx_type_raw else "?")
                tx = {"asset": asset, "tx_type": tx_type,
                      "tx_date": date, "amount": amount,
                      "row_text": f"{asset} | {tx_type} | {date} | {amount}"}
                all_transactions.append(tx)
                ticker = _ticker_from_asset_name(asset)
                if ticker:
                    exists = c.execute(
                        "SELECT 1 FROM trump_holdings WHERE ticker=? AND pdf_url=? AND tx_type=? AND tx_date=?",
                        (ticker, pdf_url, tx_type, date),
                    ).fetchone()
                    if not exists:
                        c.execute(
                            "INSERT INTO trump_holdings "
                            "(ticker,asset_name,tx_type,amount,tx_date,pdf_url,created_at) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (ticker, asset, tx_type, amount, date, pdf_url, now),
                        )
            c.commit()
            if rows:
                log.info(f"    PTR Batch {batch_idx+1}/{len(batches)}: {len(rows)} Tx")

        except json.JSONDecodeError:
            consecutive_errors += 1
            log.warning(f"    PTR Batch {batch_idx+1}: JSON-Fehler ({consecutive_errors}/{_VISION_MAX_ERRORS})")
        except Exception as e:
            consecutive_errors += 1
            log.warning(f"    PTR Batch {batch_idx+1}: {e} ({consecutive_errors}/{_VISION_MAX_ERRORS})")

    stocks_found = c.execute(
        "SELECT COUNT(*) FROM trump_holdings WHERE pdf_url=?", (pdf_url,)
    ).fetchone()[0]
    log.info(f"  ✅ PTR Vision: {len(all_transactions)} Transaktionen, {stocks_found} Aktien gespeichert")
    return all_transactions


def _is_278e_pdf(pdf_url: str) -> bool:
    """278e-PDF hat keinen 'Periodic-Transaction-Report' im Namen."""
    return "periodic-transaction-report" not in pdf_url.lower()


def reparse_oge_ptr(pdf_url: str) -> None:
    """
    Löscht einen bereits gesehenen PTR aus oge_ptrs, sodass er beim
    nächsten check_oge_alerts()-Lauf neu geparst und in trump_holdings
    gespeichert wird. Nützlich für PTRs die vor dem Holdings-Fix verarbeitet wurden.
    """
    conn.execute("DELETE FROM oge_ptrs WHERE pdf_url=?", (pdf_url,))
    conn.commit()
    log.info(f"  ♻️  OGE PTR zum Neuparsen freigegeben: {pdf_url[:80]}")


def _download_pdf(pdf_url: str) -> bytes | None:
    """Lädt ein PDF einmalig herunter (für Hash + Parser, kein Doppel-Download)."""
    try:
        r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=30)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log.warning(f"  ⚠️  PDF Download Fehler ({pdf_url[:70]}): {e}")
        return None


def _tx_fingerprint(transactions: list[dict]) -> str:
    """
    Reihenfolge-unabhängiger Fingerprint über die extrahierten Transaktionen.
    Erkennt denselben Bericht auch wenn die PDF-Bytes differieren
    (z.B. OGE-Portal-Scan vs. Whitehouse.gov-Scan derselben Filing).
    """
    keys = sorted(
        f"{(tx.get('asset') or '')[:40].upper()}|{tx.get('tx_type','')}|"
        f"{tx.get('tx_date','')}|{tx.get('amount','')}"
        for tx in transactions
    )
    return hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()


def _known_duplicate_ptr(pdf_url: str, content_hash: str | None,
                         fingerprint: str | None, db_conn=None) -> str | None:
    """
    Prüft ob derselbe PTR bereits unter anderer URL verarbeitet wurde
    (Byte-Hash oder Transaktions-Fingerprint). Gibt die Original-URL zurück.
    """
    c = db_conn or conn
    for col, val in (("content_hash", content_hash), ("tx_fingerprint", fingerprint)):
        if not val:
            continue
        row = c.execute(
            f"SELECT pdf_url FROM oge_ptrs WHERE {col}=? AND pdf_url!=?",
            (val, pdf_url),
        ).fetchone()
        if row:
            return row[0]
    return None


def _fuzzy_duplicate_ptr(pdf_url: str, transactions: list[dict], db_conn=None) -> str | None:
    """
    Fuzzy-Dedup gegen unterschiedliche Scan-/Parserversionen desselben Berichts:
    Wenn ≥90 % der erkannten Aktien-Transaktionen (Ticker|Typ|Datum|Betrag) mit
    einem früheren PTR übereinstimmen, ist es ein Duplikat.
    """
    c = db_conn or conn
    cand = set()
    for tx in transactions:
        t = _ticker_from_asset_name(tx.get("asset", ""))
        if t:
            cand.add(f"{t}|{tx.get('tx_type','')}|{tx.get('tx_date','')}|{tx.get('amount','')}")
    if len(cand) < 10:  # zu wenig Signal für einen belastbaren Vergleich
        return None
    for (other_url,) in c.execute(
        "SELECT DISTINCT pdf_url FROM trump_holdings WHERE pdf_url!=?", (pdf_url,)
    ).fetchall():
        other = {
            f"{r[0]}|{r[1]}|{r[2]}|{r[3]}"
            for r in c.execute(
                "SELECT ticker, tx_type, tx_date, amount FROM trump_holdings WHERE pdf_url=?",
                (other_url,),
            ).fetchall()
        }
        if len(other) < 10:
            continue
        overlap = len(cand & other) / min(len(cand), len(other))
        if overlap >= 0.9:
            return other_url
    return None


def check_oge_alerts() -> None:
    """
    Prüft auf neue OGE 278-T PDFs.
    - Datum-Filter: nur PDFs der letzten 90 Tage alertieren
    - Kein Alert bei 0 erkannten Transaktionen (PDF-Parser versagt)
    - Cross-Source-Dedup: Byte-Hash + Transaktions-Fingerprint verhindern
      Doppel-Alerts wenn dieselbe Filing auf OGE-Portal UND Whitehouse.gov liegt
    """
    log.info("🏛️  OGE 278-T …")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS oge_ptrs (
            pdf_url     TEXT PRIMARY KEY,
            source      TEXT,
            tx_count    INTEGER,
            detected_at TEXT
        )
    """)
    # Migration: Dedup-Spalten für ältere DBs nachziehen
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(oge_ptrs)").fetchall()}
    for col in ("content_hash", "tx_fingerprint"):
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE oge_ptrs ADD COLUMN {col} TEXT")
    conn.commit()

    links = fetch_oge_ptr_links()
    if not links:
        log.info("OGE: Keine PTR-Links gefunden")
        return

    alert_cutoff = (now_utc() - timedelta(days=OGE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    new_count = 0

    # Neue PDFs identifizieren
    new_items = []
    for item in links:
        exists = conn.execute(
            "SELECT 1 FROM oge_ptrs WHERE pdf_url=?", (item["pdf_url"],)
        ).fetchone()
        if not exists:
            new_items.append(item)

    if not new_items:
        log.info("OGE: Keine neuen PTR-Berichte")
        return

    # 278e sequenziell (einmalig, groß), PTRs parallel (viele, klein)
    ptr_items  = [i for i in new_items if not _is_278e_pdf(i["pdf_url"])]
    e278_items = [i for i in new_items if     _is_278e_pdf(i["pdf_url"])]

    # 278e zuerst (Basis für PTR-Kontext)
    for item in e278_items:
        pdf_url = item["pdf_url"]
        source  = item["source"]
        # Dedup: identisches 278e-PDF bereits unter anderer URL verarbeitet?
        pdf_bytes    = _download_pdf(pdf_url)
        content_hash = hashlib.sha256(pdf_bytes).hexdigest() if pdf_bytes else None
        dup = _known_duplicate_ptr(pdf_url, content_hash, None)
        if dup:
            conn.execute(
                "INSERT OR IGNORE INTO oge_ptrs (pdf_url,source,tx_count,detected_at,content_hash) "
                "VALUES (?,?,?,?,?)",
                (pdf_url, source, 0, now_utc().isoformat(), content_hash))
            conn.commit()
            log.info("  ♻️  278e-Duplikat von %s… — übersprungen", dup[:70])
            continue
        log.info("Neues 278e PDF: %s…", pdf_url[:80])
        year_m = re.search(r'/(\d{4})/', pdf_url)
        year   = int(year_m.group(1)) if year_m else now_utc().year
        n = parse_278e_via_claude_vision(pdf_url, year)
        conn.execute(
            "INSERT OR IGNORE INTO oge_ptrs (pdf_url,source,tx_count,detected_at,content_hash) "
            "VALUES (?,?,?,?,?)",
            (pdf_url, source, n, now_utc().isoformat(), content_hash))
        conn.commit()
        if n:
            new_count += 1

    def _process_ptr(item):
        """Verarbeitet ein PTR-PDF — läuft parallel mit eigener DB-Connection."""
        pdf_url    = item["pdf_url"]
        source     = item["source"]
        pdf_date   = _oge_date_from_url(pdf_url)
        send_alert = not (pdf_date and pdf_date < alert_cutoff)
        label      = "" if send_alert else " (historisch)"
        log.info("Neues PTR%s: %s…", label, pdf_url[:70])

        # Eigene DB-Connection pro Thread (SQLite ist nicht thread-safe)
        thread_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        thread_conn.row_factory = sqlite3.Row

        try:
            pdf_bytes    = _download_pdf(pdf_url)
            content_hash = hashlib.sha256(pdf_bytes).hexdigest() if pdf_bytes else None

            # Dedup Stufe 1: byte-identisches PDF bereits unter anderer URL gesehen
            dup = _known_duplicate_ptr(pdf_url, content_hash, None, db_conn=thread_conn)
            if dup:
                thread_conn.execute(
                    "INSERT OR IGNORE INTO oge_ptrs "
                    "(pdf_url,source,tx_count,detected_at,content_hash) VALUES (?,?,?,?,?)",
                    (pdf_url, source, 0, now_utc().isoformat(), content_hash))
                thread_conn.commit()
                log.info("  ♻️  PTR-Duplikat (Byte-identisch) von %s… — kein Alert", dup[:70])
                return 0

            transactions = parse_oge_ptr_pdf(pdf_url, db_conn=thread_conn, pdf_bytes=pdf_bytes)
            if not transactions:
                transactions = parse_ptr_via_claude_vision(pdf_url, db_conn=thread_conn,
                                                           pdf_bytes=pdf_bytes)

            # Dedup Stufe 2: gleicher Inhalt trotz anderer PDF-Bytes
            # (exakter Tx-Fingerprint oder ≥90 % Überlappung mit früherem PTR)
            fingerprint = _tx_fingerprint(transactions) if transactions else None
            dup = (_known_duplicate_ptr(pdf_url, None, fingerprint, db_conn=thread_conn)
                   or _fuzzy_duplicate_ptr(pdf_url, transactions, db_conn=thread_conn))

            thread_conn.execute(
                "INSERT OR IGNORE INTO oge_ptrs "
                "(pdf_url,source,tx_count,detected_at,content_hash,tx_fingerprint) "
                "VALUES (?,?,?,?,?,?)",
                (pdf_url, source, len(transactions), now_utc().isoformat(),
                 content_hash, fingerprint))
            thread_conn.commit()

            if not transactions:
                log.warning("OGE PTR: 0 Tx nach Vision — übersprungen: %s", pdf_url)
                return 0

            if dup:
                log.info("  ♻️  PTR-Duplikat (inhaltsgleich) von %s… — kein Alert", dup[:70])
                return 0

            if send_alert:
                send_oge_alert(pdf_url, source, transactions)
                return 1
            else:
                log.info("  📥 Historischer PTR geparst (%d Tx) — kein Alert", len(transactions))
                return 0
        finally:
            thread_conn.close()

    # PTRs parallel mit max 3 gleichzeitigen Threads
    if ptr_items:
        log.info("OGE: %d PTRs parallel verarbeiten (max 3 gleichzeitig)…", len(ptr_items))
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_process_ptr, item): item for item in ptr_items}
            for fut in as_completed(futures):
                try:
                    new_count += fut.result()
                except Exception as e:
                    log.warning("PTR Worker Fehler: %s", e)

    conn.commit()
    if new_count == 0:
        log.info("OGE: Keine neuen PTR-Berichte (nur historische geparst)")

# ─────────────────────────────────────────────────────────────────────────────
# TRUMP HOLDINGS  (OGE Form 278 – öffentlich)
# ─────────────────────────────────────────────────────────────────────────────
TRUMP_HOLDINGS_STATIC = {
    "DJT": "JA – Trump hält ~57 % an Trump Media & Technology Group (DJT), Quelle: SEC Form 4 / OGE 2024",
}

def trump_holding_info(ticker: str) -> str:
    """
    Gibt aktuellen Netto-Bestand zurück.
    KAUF = +1, VERKAUF = -1, TAUSCH = 0.
    Wenn alle Käufe durch Verkäufe aufgehoben → 'nicht mehr gehalten'.
    """
    t = ticker.upper()
    if t in TRUMP_HOLDINGS_STATIC:
        return TRUMP_HOLDINGS_STATIC[t]

    rows = conn.execute(
        "SELECT tx_type, asset_name, tx_date, pdf_url FROM trump_holdings "
        "WHERE ticker=? ORDER BY tx_date ASC, created_at ASC",
        (t,),
    ).fetchall()
    if not rows:
        return "Nicht aus öffentlichen OGE-Filings (Form 278) bekannt – keine Annahmen."

    net = 0
    last_asset = rows[-1][1]
    last_date  = rows[-1][2]
    last_pdf   = rows[-1][3]
    for tx_type, *_ in rows:
        if tx_type == "KAUF":    net += 1
        elif tx_type == "VERKAUF": net -= 1

    if net <= 0:
        return (
            f"NEIN – laut OGE 278-T PTR vollständig verkauft "
            f"(letzter Eintrag: {last_date}, {last_asset})"
        )
    buys  = sum(1 for r in rows if r[0] == "KAUF")
    sells = sum(1 for r in rows if r[0] == "VERKAUF")
    return (
        f"JA – OGE 278-T PTR: {buys}× KAUF, {sells}× VERKAUF → aktuell gehalten "
        f"(letzter Eintrag: {last_date}, Quelle: {last_pdf[:60]}…)"
    )


_POSITION_PERF_CACHE: dict[str, str] = {}

def trump_position_performance(ticker: str) -> str:
    """
    Kursentwicklung seit Trumps erstem bekannten Kauf (frühestes KAUF-Datum
    aus trump_holdings). Beispiel: '+141% (seit Kauf am 2025-05-27)'.
    Leerstring wenn kein Kaufdatum oder keine Kursdaten verfügbar.
    """
    t = ticker.upper()
    if t in _POSITION_PERF_CACHE:
        return _POSITION_PERF_CACHE[t]

    rows = conn.execute(
        "SELECT tx_date FROM trump_holdings "
        "WHERE ticker=? AND tx_type='KAUF' AND tx_date IS NOT NULL AND tx_date != ''",
        (t,),
    ).fetchall()
    buy_dates = []
    for (raw,) in rows:
        raw = (raw or "").strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                buy_dates.append(datetime.strptime(raw[:10], fmt).date())
                break
            except ValueError:
                continue
    if not buy_dates:
        _POSITION_PERF_CACHE[t] = ""
        return ""

    first_buy = min(buy_dates)
    result = ""
    try:
        yf_sym = YF_TICKER_MAP.get(t, t)
        hist = yf.Ticker(yf_sym).history(
            start=first_buy.isoformat(), auto_adjust=True, timeout=15
        )
        if not hist.empty and float(hist["Close"].iloc[0]) > 0:
            pct = (float(hist["Close"].iloc[-1]) / float(hist["Close"].iloc[0]) - 1) * 100
            result = f"{pct:+.0f}% (seit Kauf am {first_buy.isoformat()})"
    except Exception as e:
        log.warning(f"  ⚠️  Position-Performance ({ticker}): {e}")
    _POSITION_PERF_CACHE[t] = result
    return result

# ─────────────────────────────────────────────────────────────────────────────
# YAHOO FINANCE  –  Marktdaten
# ─────────────────────────────────────────────────────────────────────────────
YF_TICKER_MAP = {
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "BRK":  "BRK-B",
    "GOOGL": "GOOGL",
    "GOOG":  "GOOG",
}

_YF_LAST_CALL: float = 0.0
_YF_MIN_INTERVAL = 3.0  # Sekunden zwischen yfinance-Calls (Rate-Limit-Schutz)

_YF_CACHE: dict[str, dict] = {}  # nur Erfolge werden gecacht

def fetch_market_data(ticker: str) -> dict:
    """Holt 1-Monats-History von Yahoo Finance. Bei Fehler leeres Dict."""
    global _YF_LAST_CALL
    t_upper = ticker.upper()
    if t_upper in _YF_CACHE:
        return _YF_CACHE[t_upper]

    wait = _YF_MIN_INTERVAL - (time.time() - _YF_LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    _YF_LAST_CALL = time.time()

    yf_sym = YF_TICKER_MAP.get(t_upper, t_upper)
    for attempt in range(3):
        try:
            hist = yf.Ticker(yf_sym).history(period="1mo", auto_adjust=True,
                                              timeout=15)
            if hist.empty or len(hist) < 2:
                return {}
            close      = hist["Close"]
            current    = round(float(close.iloc[-1]), 2)
            prev_close = round(float(close.iloc[-2]), 2)
            week_ago   = round(float(close.iloc[-6]) if len(close) >= 6 else float(close.iloc[0]), 2)
            month_ago  = round(float(close.iloc[0]), 2)
            result = {
                "price":   current,
                "chg_1d":  round((current / prev_close - 1) * 100, 2),
                "chg_1w":  round((current / week_ago   - 1) * 100, 2),
                "chg_1m":  round((current / month_ago  - 1) * 100, 2),
            }
            _YF_CACHE[t_upper] = result  # nur Erfolge cachen
            return result
        except Exception as e:
            log.warning(f"  ⚠️  Yahoo Finance ({ticker}) attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return {}

def format_market_block(ticker: str) -> str:
    d = fetch_market_data(ticker)
    if not d:
        return "Marktdaten: nicht verfügbar"
    def arrow(v): return "▲" if v >= 0 else "▼"
    return (
        f"Letzter Schlusskurs:      {d['price']:.2f} USD\n"
        f"Ggü. Vortag:              {arrow(d['chg_1d'])} {d['chg_1d']:+.2f}%\n"
        f"5 Handelstage:            {arrow(d['chg_1w'])} {d['chg_1w']:+.2f}%\n"
        f"1 Monat:                  {arrow(d['chg_1m'])} {d['chg_1m']:+.2f}%"
    )

# ─────────────────────────────────────────────────────────────────────────────
# TURBO-ZERTIFIKAT-EMPFEHLUNG  –  onvista-basiert
# ─────────────────────────────────────────────────────────────────────────────
# Puffer-Zonen je Risikobereitschaft (KO-Abstand zum aktuellen Kurs)
_TURBO_PROFILES = {
    # Der KO-Puffer legt den Hebel mathematisch fest: Hebel ≈ 1 / KO-Abstand
    # (Open-End-Turbo: KO ≈ Strike). Mehr Gap-Schutz = zwangsläufig weniger Hebel.
    "konservativ": {"puffer": 0.20},   # ⇒ Hebel ~5x
    "mittel":      {"puffer": 0.16},   # ⇒ Hebel ~6x
    "aggressiv":   {"puffer": 0.13},   # ⇒ Hebel ~8x
}
TURBO_RISIKO = "mittel"   # global konfigurierbar

# Der onvista-KO-Finder akzeptiert KEINE URL-Parameter (verifiziert: alle
# Query-Params und Slug-Varianten werden ignoriert bzw. redirecten zurück).
# Deshalb: generischer Finder-Link + Deep-Link auf die Basiswert-Seite.
ONVISTA_KO_FINDER = "https://www.onvista.de/derivate/Knock-Outs"

_ONVISTA_PAGE_CACHE: dict = {}

def _onvista_underlying_url(ticker: str) -> str | None:
    """
    Löst den US-Ticker über die öffentliche onvista-Such-API zur deutschen
    Basiswert-Seite auf (enthält die Knock-Out-Box mit handelbaren Turbos).
    Fallback: Suche per Firmenname aus entities.json (z.B. BA → 'Boeing',
    da onvista US-Heimatsymbole nicht immer direkt kennt).
    """
    t = ticker.upper()
    if t in _ONVISTA_PAGE_CACHE:
        return _ONVISTA_PAGE_CACHE[t]

    search_terms = [t]
    company = (ENTITIES.get(t, {}).get("company") or [None])[0]
    if company:
        search_terms.append(company)

    url = None
    for term in search_terms:
        try:
            r = requests.get(
                "https://api.onvista.de/api/v1/instruments/query",
                params={"searchValue": term},
                headers={"User-Agent": FEED_AGENT}, timeout=10,
            )
            r.raise_for_status()
            for item in r.json().get("list", []):
                if item.get("entityType") != "STOCK":
                    continue
                sym = (item.get("homeSymbol") or item.get("symbol") or "").upper()
                # Ticker-Suche: Symbol muss exakt passen.
                # Namens-Suche: erster Aktien-Treffer gilt.
                if sym == t or term != t:
                    url = item.get("urls", {}).get("WEBSITE")
                    break
            if url:
                break
        except Exception as e:
            log.warning(f"  ⚠️  onvista-Suche ({term}): {e}")
    _ONVISTA_PAGE_CACHE[t] = url
    return url


# ─────────────────────────────────────────────────────────────────────────────
# VONTOBEL PRODUKT-API  –  konkretes, in DE handelbares Turbo-Zertifikat (WKN)
# Öffentliche JSON-API des Emittenten-Finders, kostenlos, kein Key.
# Verifiziert: culture via Query-Param "c=de-de"; Range-Filter via
# "selectedItem", Listen-Filter via "selectedItems"; der Richtungsfilter
# (property 4) wird von der API ignoriert → Richtung client-seitig filtern.
# productType 5 = Open End Turbo · property 1 = Basiswert · 14 = Hebel · 58 = Preis €
# ─────────────────────────────────────────────────────────────────────────────
VONTOBEL_API  = "https://markets.vontobel.com/api/v1"
VONTOBEL_PURL = ("https://markets.vontobel.com/de-de/produkte/hebel/"
                 "turbo-optionsscheine-open-end/")
_VT_HEADERS   = {"User-Agent": FEED_AGENT, "Accept": "application/json"}
_VT_KEY_CACHE: dict = {}


def _vontobel_underlying_key(ticker: str):
    """Löst Ticker → Vontobel-Underlying-Key (Firmenname zuerst, dann Ticker)."""
    t = ticker.upper()
    if t in _VT_KEY_CACHE:
        return _VT_KEY_CACHE[t]
    company = (ENTITIES.get(t, {}).get("company") or [t])[0]
    key = None
    for query in (company, t):
        try:
            r = requests.get(
                f"{VONTOBEL_API}/underlyings/search",
                params={"Query": query, "Page": 0, "PageSize": 5,
                        "ProductType": 5, "c": "de-de"},
                headers=_VT_HEADERS, timeout=12,
            )
            r.raise_for_status()
            for it in r.json().get("payload", {}).get("items", []):
                # Treffer validieren: erstes Wort des Suchbegriffs muss im Namen stecken
                if query.lower().split()[0] in it.get("text", "").lower():
                    key = it.get("key")
                    break
            if key:
                break
        except Exception as e:
            log.warning(f"  ⚠️  Vontobel Underlying-Suche ({query}): {e}")
    _VT_KEY_CACHE[t] = key
    return key


def find_best_turbo(ticker: str, direction: str,
                    hebel_lo: float, hebel_hi: float, hebel_target: float):
    """
    Sucht das beste in Deutschland handelbare Open-End-Turbo-Zertifikat:
    - Server-Filter: Basiswert, Hebel im Suchband, Preis 4–25 €
    - Client-Filter: Richtung + KO-Plausibilität (Long: KO < Spot, Short: KO > Spot)
    - 'Bestes' = Hebel am nächsten am Zielhebel des Risikoprofils
    - Spread per Detail-Abfrage (Bid/Ask) ergänzt
    Gibt None zurück wenn nichts gefunden → Aufrufer nutzt Parameter-Fallback.
    Hinweis: durchsucht nur Vontobel (einzige stabil zugängliche Gratis-API);
    SG/HSBC/BNP-Alternativen bleiben dem manuellen Finder vorbehalten.
    """
    key = _vontobel_underlying_key(ticker)
    if not key:
        log.info(f"  ℹ️  Vontobel: kein Underlying für {ticker} → kein konkretes Zertifikat")
        return None

    want_dir = 1 if direction == "LONG" else 2
    try:
        body = {"productType": 5, "page": 0, "pageSize": 50, "filters": [
            {"property": 1,  "selectedItems": [{"key": key}]},
            {"property": 14, "selectedItem": {"min": hebel_lo, "max": hebel_hi}},
            {"property": 58, "selectedItem": {"min": 4, "max": 25}},
        ]}
        r = requests.post(f"{VONTOBEL_API}/products/search", json=body,
                          params={"c": "de-de"}, headers=_VT_HEADERS, timeout=15)
        r.raise_for_status()
        candidates = []
        for it in r.json().get("payload", {}).get("items", []):
            if it.get("direction") != want_dir or not it.get("leverage"):
                continue
            ko, spot = it.get("knockOut"), it.get("spotPrice")
            if not ko or not spot:
                continue
            # KO muss zur Richtung passen (Schutz falls direction-Feld fehlerhaft)
            if (want_dir == 1 and ko >= spot) or (want_dir == 2 and ko <= spot):
                continue
            candidates.append(it)
        if not candidates:
            log.info(f"  ℹ️  Vontobel: kein {direction}-Turbo im Hebel-Band für {ticker}")
            return None

        # Spread-bewusste Auswahl: Top 3 nach Hebel-Nähe, Spread per Detail-
        # Abfrage prüfen. Erstes Papier mit Spread ≤ 0.8% gewinnt (Reihenfolge
        # = Hebel-Nähe); erfüllt keines das Kriterium → kleinster Spread.
        candidates.sort(key=lambda it: abs(it["leverage"] - hebel_target))
        scored = []
        for it in candidates[:3]:
            isin = next((f["isin"] for f in it.get("primaryFeatures", [])
                         if f.get("isin")), "")
            if not isin:
                continue
            entry = {
                "isin":       isin,
                "wkn":        isin[5:11],
                "leverage":   it["leverage"],
                "ko":         it["knockOut"],
                "buffer_pct": round(it.get("riskBuffer", 0) * 100, 1),
                "bid":        it.get("price", {}).get("bid"),
                "ask":        None,
                "spread_pct": None,
                "url":        VONTOBEL_PURL + isin,
            }
            try:
                rd = requests.get(f"{VONTOBEL_API}/products/{isin}",
                                  params={"c": "de-de"}, headers=_VT_HEADERS, timeout=12)
                rd.raise_for_status()
                price = rd.json().get("payload", {}).get("price", {})
                bid, ask = price.get("bid"), price.get("ask")
                if bid and ask and ask > 0:
                    entry["bid"], entry["ask"] = bid, ask
                    entry["spread_pct"] = round((ask - bid) / ask * 100, 2)
            except Exception:
                pass  # Spread optional — Kandidat bleibt gültig
            scored.append(entry)
            if entry["spread_pct"] is not None and entry["spread_pct"] <= 0.8:
                return entry  # Hebel-nächstes Papier mit gutem Spread → fertig

        if not scored:
            return None
        # Kein Kandidat unter 0.8% → den mit dem kleinsten bekannten Spread
        return min(scored, key=lambda e: e["spread_pct"] if e["spread_pct"] is not None else 99.0)
    except Exception as e:
        log.warning(f"  ⚠️  Vontobel Produktsuche ({ticker}): {e}")
        return None


def _calc_hv30(ticker: str) -> float | None:
    """
    30-Tage historische Volatilität (annualisiert) als IV-Proxy — kostenlose
    Echtzeit-IV gibt es nicht. Respektiert das globale yfinance-Rate-Limit.
    """
    global _YF_LAST_CALL
    wait = _YF_MIN_INTERVAL - (time.time() - _YF_LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    _YF_LAST_CALL = time.time()
    try:
        hist = yf.Ticker(YF_TICKER_MAP.get(ticker.upper(), ticker.upper())).history(
            period="2mo", auto_adjust=True, timeout=15
        )
        if len(hist) < 22:
            return None
        returns = hist["Close"].pct_change().dropna()
        hv30 = float(returns.tail(22).std() * (252 ** 0.5))  # annualisiert
        return round(hv30, 4)
    except Exception:
        return None


def turbo_recommendation(ticker: str, direction: str) -> str:
    """
    Berechnet KONSISTENTE Turbo-Parameter für einen News-Trade:
    - KO-Puffer nach Risikoprofil, bei hoher Volatilität vergrößert (Gap-Risiko
      bei Trump-News: Überschrift kann sich über Nacht drehen)
    - Hebel wird aus dem Puffer ABGELEITET (Hebel ≈ 1/KO-Abstand) statt
      unabhängig vorgegeben — KO-Abstand und Hebel können sich bei einem
      Turbo nicht unabhängig voneinander wählen lassen
    - Auswahlkriterien fürs konkrete Papier: Emittent, Spread, Preisbereich

    Konkretes Zertifikat: find_best_turbo() sucht über die Vontobel-API das
    am besten passende, in DE handelbare Papier (WKN/ISIN, Spread-Check).
    Schlägt das fehl, bleibt der Parameter-Block mit Finder-Links als Fallback.
    """
    if direction == "UNKLAR":
        return "⛔ Keine Empfehlung – Trade-Richtung unklar"

    data = fetch_market_data(ticker)
    if not data:
        return "⛔ Keine Empfehlung – Marktdaten nicht verfügbar"

    price = data["price"]
    hv    = _calc_hv30(ticker)

    # Puffer dynamisch: bei hoher Volatilität größerer KO-Abstand
    puffer = _TURBO_PROFILES[TURBO_RISIKO]["puffer"]
    if hv is not None and hv > 0.45:   # >45 % annualisierte Vol → konservativer
        puffer = min(puffer + 0.04, 0.25)
    elif hv is not None and hv < 0.20: # <20 % Vol → etwas aggressiver okay
        puffer = max(puffer - 0.02, 0.13)

    is_long  = direction == "LONG"
    ko       = round(price * (1 - puffer if is_long else 1 + puffer), 2)
    abstand  = round(puffer * 100, 1)
    hebel    = round(1 / puffer, 1)        # Turbo-Mathematik: Hebel ≈ 1/Abstand
    hebel_lo = round(hebel * 0.85, 1)      # ±15% Suchband um den implizierten
    hebel_hi = round(hebel * 1.15, 1)      # Hebel (reale KOs liegen nie exakt)
    ko_op    = "≤" if is_long else "≥"
    emoji    = "📈" if is_long else "📉"
    typ      = "Long" if is_long else "Short"
    hv_info  = f"{hv*100:.1f}% (30T HV als IV-Proxy)" if hv else "k.A."
    company  = (ENTITIES.get(ticker.upper(), {}).get("company") or [ticker])[0]

    # ── Konkretes Zertifikat über Vontobel-API suchen ────────────────────────
    best = find_best_turbo(ticker, direction, hebel_lo, hebel_hi, hebel)
    if not best:
        # Zweiter Versuch: Suchband nur nach UNTEN erweitern (weniger Hebel =
        # mehr KO-Puffer = sicherer). Nach oben nie — das hieße mehr KO-Risiko
        # als das Risikoprofil erlaubt.
        best = find_best_turbo(ticker, direction,
                               max(1.5, round(hebel * 0.5, 1)), hebel_hi, hebel)

    header = (
        f"{emoji} {typ} Open End Turbo auf {ticker}\n"
        f"\n"
        f"   Kurs aktuell:      {price:.2f} USD\n"
        f"   Volatilität:       {hv_info}\n"
        f"   Ziel-Profil:       KO {ko_op} {ko:.2f} USD ({abstand}% Abstand) · Hebel ~{hebel}x\n"
        f"   Hinweis: Hebel ≈ 1/KO-Abstand — mehr Puffer = weniger Hebel\n"
    )

    if best:
        if best["spread_pct"] is None:
            spread_txt = "k.A."
        else:
            spread_txt = f"{best['spread_pct']:.2f}%"
            if best["spread_pct"] > 0.8:
                spread_txt += "  ⚠️ über 0.8%-Kriterium"
        preis_txt = (f"Bid {best['bid']:.2f} € / Ask {best['ask']:.2f} €"
                     if best.get("ask") else
                     f"Bid {best['bid']:.2f} €" if best.get("bid") else "k.A.")
        return (
            header
            + f"   ──────────────────────────────────\n"
            + f"   ✅ BESTES ZERTIFIKAT (in DE handelbar, Vontobel):\n"
            + f"   WKN:               {best['wkn']}\n"
            + f"   ISIN:              {best['isin']}\n"
            + f"   Hebel:             {best['leverage']:.2f}x\n"
            + f"   Knock-Out:         {best['ko']:.2f} USD  ({best['buffer_pct']}% Abstand)\n"
            + f"   Preis:             {preis_txt}\n"
            + f"   Spread:            {spread_txt}\n"
            + f"   Typ:               Open End Turbo (keine Laufzeit)\n"
            + f"\n"
            + f"   📄 Produktseite:\n"
            + f"   {best['url']}\n"
            + f"\n"
            + f"   Auswahl: Hebel am nächsten am Ziel ({hebel}x), Preis 4–25 €,\n"
            + f"   KO-Richtung geprüft. Alternativen anderer Emittenten (SG/HSBC/BNP):\n"
            + f"   {ONVISTA_KO_FINDER}"
        )

    # ── Fallback: kein API-Treffer → Parameter + manuelle Finder-Links ───────
    links = []
    page_url = _onvista_underlying_url(ticker)
    if page_url:
        links.append(f"   Basiswert-Seite (mit Knock-Out-Box):\n   {page_url}")
    links.append(f"   KO-Finder (Filter unten manuell setzen):\n   {ONVISTA_KO_FINDER}")
    links_block = "\n".join(links)

    return (
        header
        + f"   ──────────────────────────────────\n"
        + f"   Kein automatischer Zertifikat-Treffer — manuell auswählen:\n"
        + f"   Finder-Filter:     Basiswert „{company}“ · {typ} · KO {ko_op} {ko:.2f}\n"
        + f"   Hebel-Suchband:    {hebel_lo}–{hebel_hi}x\n"
        + f"   Emittenten:        Vontobel · SG · HSBC · BNP\n"
        + f"   Max. Spread:       < 0.8 %\n"
        + f"   Preis-Ziel:        4–25 €\n"
        + f"   Typ:               Open End (keine Laufzeit)\n"
        + f"\n"
        + f"   🔍 Zertifikat auswählen:\n"
        + f"{links_block}"
    )


def parse_trade_direction(alert_text: str) -> str:
    """Extrahiert LONG / SHORT / UNKLAR aus dem Claude-Output."""
    for line in alert_text.splitlines():
        if "trade-richtung:" in line.lower() or "trade_direction:" in line.lower():
            upper = line.upper()
            if "LONG"  in upper: return "LONG"
            if "SHORT" in upper: return "SHORT"
    return "UNKLAR"

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SEKTOR-ERKENNUNG  (Truth Social ohne direkten Ticker-Match)
# ─────────────────────────────────────────────────────────────────────────────
def discover_tickers_via_claude(text: str) -> list[tuple[str, str]]:
    """
    Fragt Claude welche börsennotierten Unternehmen durch den Text am meisten profitieren.
    Gibt max. 3 (ticker, 'claude') Tupel zurück, sortiert nach erwartetem Impact, oder [].
    """
    prompt = (
        "Analyze this Trump-related news text and identify the US-listed companies "
        "that are the BIGGEST BENEFICIARIES or most SEVERELY HURT by this specific event.\n\n"
        f"TEXT: {text[:1200]}\n\n"
        "Rules:\n"
        "- Rank by expected price impact magnitude, biggest first\n"
        "- For sector-wide announcements (subsidies, tariffs, bans), pick the largest "
        "pure-play company in that sector, not diversified conglomerates\n"
        "- If the text involves a PRIVATE company (SpaceX, OpenAI, Stripe, etc.), "
        "identify the closest publicly traded proxies and government contract beneficiaries "
        "(e.g. SpaceX → RKLB, ASTS, LMT; OpenAI → MSFT, NVDA)\n"
        "- If the text involves a US government stake, contract, or partnership with a sector, "
        "include the largest government contract winners in that sector (e.g. LMT, NOC, BA for aerospace)\n"
        "- If the text signals a likely IPO or public listing, include the sector's pure-plays "
        "that would be most displaced or boosted by the new entrant\n"
        "- Only include companies with realistic near-term price reaction\n"
        "- Reply ONLY with comma-separated ticker symbols, max 3 (e.g. BTU,ARCH,CEIX)\n"
        "- If no concrete market impact is identifiable: reply only with NONE"
    )
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",  # Ticker-Erkennung braucht kein Sonnet
            max_tokens=30,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip().upper()
        if raw == "NONE" or not raw:
            return []
        tickers = [t.strip() for t in raw.split(",") if re.match(r'^[A-Z]{1,5}$', t.strip())]
        if tickers:
            log.info(f"  🔍 Claude Sektor-Erkennung: {tickers}")
        return [(t, "claude") for t in tickers[:3]]
    except Exception as e:
        log.warning(f"  ⚠️  Sektor-Erkennung Fehler: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# HAUPTANALYSE  –  LLM + Alert + E-Mail
# ─────────────────────────────────────────────────────────────────────────────
# Statischer Prompt-Block — wird gecacht (Anthropic Prompt Caching)
_ANALYSIS_SYSTEM = """You are a quantitative political-risk analyst specializing in Trump-driven market dislocations. Be precise, factual, and calibrated. Never speculate beyond what the source text directly supports.

COMPARABLE PRECEDENTS (use for magnitude calibration):
- Trump tariff tweet on steel (Mar 2018): NUE +8%, X +6% intraday
- Trump Truth Post attacking Amazon (Apr 2018): AMZN -5% within 2h
- Trump executive order on TikTok (Aug 2020): SNAP +8%, META +2%
- Trump China chip export ban (Oct 2022): NVDA -15% over 3 days

Respond ONLY in the exact format requested. No preamble. No markdown."""

_HAIKU_TRADE_PROMPT = (
    "You are a trading signal pre-screener. Analyze this text about {ticker}.\n\n"
    "TEXT: {text}\n\n"
    "Answer with EXACTLY one of:\n"
    "ACTIONABLE — clear bullish or bearish signal for {ticker}, price reaction likely\n"
    "NO_TRADE — neutral, unclear, or no meaningful impact on {ticker}\n\n"
    "One word answer only."
)


def _haiku_tradeable(ticker: str, text: str) -> bool:
    """
    Stufe 1 — Haiku entscheidet ob der Signal handelbar ist (~$0.0002).
    Nur bei JA folgt der teure Sonnet-Call.
    """
    try:
        prompt = _HAIKU_TRADE_PROMPT.format(
            ticker=ticker,
            text=text[:800],
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = resp.content[0].text.strip().upper()
        tradeable = "ACTIONABLE" in answer
        if not tradeable:
            log.info(f"  ⏭️  {ticker} Haiku: NO_TRADE → Sonnet-Call gespart")
        return tradeable
    except Exception as e:
        log.warning(f"  ⚠️  Haiku Pre-Screen Fehler ({ticker}): {e}")
        return True  # im Zweifel Sonnet ran lassen


def analyze_and_alert(
    source:     str,
    published,
    raw_text:   str,
    ticker:     str,
    url:        str,
    confidence: str = "hoch",
):
    # ── Priorität bestimmen (aus config.yml Watch-List) ──────────────────────
    t_upper = ticker.upper()
    if t_upper in WATCHLIST_HIGH:
        priority = "high"
    elif t_upper in WATCHLIST_MEDIUM:
        priority = "medium"
    elif t_upper in WATCHLIST_LOW:
        priority = "low"
    else:
        priority = "unknown"

    # Low/Unknown-Priorität: Tier-3/Claude-Inferenz-Treffer verwerfen
    # Medium-Priorität mit claude-Konfidenz: durchlassen (z.B. FNMA/FMCC)
    if priority in ("low", "unknown") and confidence in ("niedrig", "claude"):
        log.info(f"  ⏭️  {ticker} [{priority}] + {confidence} Konfidenz → übersprungen")
        return

    # ── Rate-Limit-Check (vor jeder weiteren Arbeit) ─────────────────────────
    if not _rate_limit_ok(ticker):
        return

    # ── Stufe 1: Haiku-Tradability-Screen (nur Medium/Low/Unknown) ───────────
    if priority != "high" and not _haiku_tradeable(ticker, raw_text):
        return  # High-Priority-Ticker überspringen diesen Screen

    holding_info = trump_holding_info(ticker)
    holding_perf = trump_position_performance(ticker)
    if holding_perf:
        holding_info += f" | Performance seit Trump-Kauf: {holding_perf}"

    # ── Marktdaten erst NACH Haiku-Screen — spart ~70% der yfinance-Calls ────
    market_data  = fetch_market_data(ticker)
    market_block = format_market_block(ticker)

    # Konfidenz-Beschreibung für Sonnet-Prompt
    if confidence == "niedrig":
        conf_desc = (
            f"LOW — {ticker} matched only via product/brand keyword, "
            f"not by ticker symbol or company name directly."
        )
    elif confidence == "claude":
        conf_desc = (
            f"CLAUDE-INFERRED — {ticker} not explicitly named in text; "
            f"Claude identified this company as likely affected."
        )
    else:
        conf_desc = "HIGH — ticker symbol or company name found directly in text."

    price      = market_data.get("price", 0)
    chg_1d     = market_data.get("chg_1d", 0)
    stop_long  = round(price * 0.92, 2) if price else 0
    stop_short = round(price * 1.08, 2) if price else 0

    # Dynamischer Teil des Prompts (variabel pro Call)
    dynamic_prompt = f"""SOURCE TEXT:
{raw_text}

SOURCE: {source} | PUBLISHED: {published}

MARKET DATA ({ticker}):
{market_block}

DETECTION CONFIDENCE: {conf_desc}
TRUMP FINANCIAL INTEREST: {holding_info}

ANALYSIS FORMAT — respond exactly:

RELEVANCE: [YES / NO] — {ticker} is [directly named / sector-affected / tangentially mentioned]
COMPANY: [Full legal name] ({ticker})
EVENT_DATE: [YYYY-MM-DD of when the described event actually occurred, NOT the article publish date — if unknown write UNKNOWN]
EVENT_SUMMARY: [One sentence: what Trump said/did]
SENTIMENT: [BULLISH / BEARISH / NEUTRAL] for {ticker}
SENTIMENT_BASIS: [Quote from text — max 15 words]
PRICE_ALREADY_REACTED: [YES ({chg_1d:+.1f}% today) / NO / UNCLEAR]
MAGNITUDE_ESTIMATE: [SMALL <3% / MEDIUM 3-10% / LARGE >10%] — [one-sentence rationale]
TIME_TO_IMPACT: [IMMEDIATE / SHORT 1-5 days / MEDIUM 1-4 weeks / UNCLEAR]
TRUMP_CONFLICT_OF_INTEREST: [YES / NO / UNKNOWN]
SUMMARY: [Max 2 sentences. Facts only.]
TRADE_DIRECTION: [LONG / SHORT / NO_TRADE]
TRADE_RATIONALE: [Evidence + price {price:.2f} in one sentence]
STOP_LEVEL: [LONG: {stop_long:.2f} (−8%) / SHORT: {stop_short:.2f} (+8%) / N/A]
CONFIDENCE_SCORE: [HIGH / MEDIUM / LOW] — [limiting factor, max 5 words]"""

    # ── Stufe 2: Sonnet-Analyse mit Prompt Caching ────────────────────────────
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=550,      # war 900 — Antworten sind ~350-500 Token
            temperature=0,
            messages=[{
                "role": "user",
                "content": [
                    {   # Statischer Block → wird gecacht (90% günstiger ab 2. Call)
                        "type": "text",
                        "text": _ANALYSIS_SYSTEM,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {   # Dynamischer Block → nie gecacht
                        "type": "text",
                        "text": dynamic_prompt,
                    },
                ],
            }],
        )
        alert_text = response.content[0].text.strip()
        _rate_limit_record(ticker)   # Sonnet-Call zählen + Cooldown setzen
    except Exception as e:
        log.error(f"  ❌ Claude-API Fehler ({ticker}): {e}")
        return

    # ── Relevanz-Gate ────────────────────────────────────────────────────────
    first_line = alert_text.splitlines()[0].upper()
    if "RELEVANCE:" in first_line and "NO" in first_line:
        log.info(f"  ⏭️  {ticker} übersprungen – kein konkreter Unternehmensbezug")
        return

    # ── Trade-Richtung, Confidence, Magnitude aus Claude-Output ─────────────
    direction  = "UNKLAR"
    conf_score = "LOW"
    magnitude  = "SMALL"
    event_date_str = "UNKNOWN"
    for line in alert_text.splitlines():
        u = line.upper()
        if u.startswith("TRADE_DIRECTION:"):
            if "LONG"     in u: direction = "LONG"
            elif "SHORT"  in u: direction = "SHORT"
            elif "NO_TRADE" in u: direction = "NO_TRADE"
        elif u.startswith("CONFIDENCE_SCORE:"):
            if "HIGH"   in u: conf_score = "HIGH"
            elif "MEDIUM" in u: conf_score = "MEDIUM"
            else: conf_score = "LOW"
        elif u.startswith("MAGNITUDE_ESTIMATE:"):
            if "LARGE"  in u: magnitude = "LARGE"
            elif "MEDIUM" in u: magnitude = "MEDIUM"
            else: magnitude = "SMALL"
        elif line.upper().startswith("EVENT_DATE:"):
            event_date_str = line.split(":", 1)[1].strip()

    # ── Stale-Event-Gate: kein Trade wenn Event > 7 Tage alt ─────────────────
    if event_date_str and event_date_str.upper() != "UNKNOWN":
        try:
            event_dt = datetime.strptime(event_date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            age_days = (now_utc() - event_dt).days
            if age_days > 7:
                log.info(f"  ⏭️  {ticker} Event vom {event_date_str} ist {age_days} Tage alt → NO_TRADE (stale)")
                alert_text = re.sub(
                    r"(TRADE_DIRECTION:)[^\n]*",
                    f"\\1 NO_TRADE  ⚠️ Event {age_days} Tage alt – Markt hat bereits reagiert",
                    alert_text,
                )
                direction = "NO_TRADE"
        except ValueError:
            pass

    # ── Schwellenwert-Gate (aus config.yml) ───────────────────────────────────
    if not confidence_ok(conf_score):
        log.info(f"  ⏭️  {ticker} Konfidenz {conf_score} < {MIN_CONFIDENCE} → kein Alert")
        return
    if not magnitude_ok(magnitude):
        log.info(f"  ⏭️  {ticker} Magnitude {magnitude} < {MIN_MAGNITUDE} → kein Alert")
        return

    # ── Turbo-Empfehlung ─────────────────────────────────────────────────────
    turbo_dir   = "UNKLAR" if direction == "NO_TRADE" else direction
    turbo_block = turbo_recommendation(ticker, turbo_dir)
    # URLs im Turbo-Block klickbar machen (Text bleibt sonst unverändert)
    turbo_html  = re.sub(
        r'(https?://[^\s<]+)',
        r'<a href="\1" style="color:#0071e3;">\1</a>',
        html.escape(turbo_block),
    )

    # ── SQLite-Dedup ─────────────────────────────────────────────────────────
    h = event_hash(ticker, raw_text)
    try:
        conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
            (h, source, str(published), raw_text, h, ticker, now_utc().isoformat()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        log.info(f"  ⏭️  {ticker} bereits in DB – kein doppelter Alert")
        return

    # ── Konfidenz-Badge ──────────────────────────────────────────────────────
    if confidence == "niedrig":
        badge = (
            '<span style="background:#e67e22;color:#fff;padding:2px 8px;'
            'border-radius:4px;font-size:11px;">⚠️ Konfidenz: niedrig</span><br><br>'
        )
    elif confidence == "claude":
        badge = (
            '<span style="background:#8e44ad;color:#fff;padding:2px 8px;'
            'border-radius:4px;font-size:11px;">🤖 Claude-Inferenz</span><br><br>'
        )
    else:
        badge = ""

    # ── Analyse-Zeilen als HTML-Tabelle rendern ──────────────────────────────
    def _render_analysis(text: str) -> str:
        rows = []
        for line in text.splitlines():
            if ":" in line:
                key, _, val = line.partition(":")
                rows.append(
                    f'<tr><td style="padding:8px 12px 8px 0;color:#6e6e73;'
                    f'font-size:13px;white-space:nowrap;vertical-align:top;'
                    f'font-weight:500;">{key.strip()}</td>'
                    f'<td style="padding:8px 0;font-size:13px;color:#1d1d1f;'
                    f'vertical-align:top;">{val.strip()}</td></tr>'
                )
            elif line.strip():
                rows.append(
                    f'<tr><td colspan="2" style="padding:6px 0;font-size:13px;'
                    f'color:#1d1d1f;">{line}</td></tr>'
                )
        return f'<table style="border-collapse:collapse;width:100%">{"".join(rows)}</table>'

    analysis_html = _render_analysis(alert_text)

    # ── Trump-Positions-Box (nur wenn Trump die Aktie selbst hält) ───────────
    perf_box = ""
    if holding_perf:
        perf_box = f"""
  <tr><td style="background:#ffffff;padding:0 32px 16px;">
    <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:10px;padding:12px 18px;">
      <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
         font-size:13px;line-height:1.5;color:#92400e;">
        💼 <strong>Trump hält {ticker}</strong> &nbsp;·&nbsp; {holding_perf}
      </p>
    </div>
  </td></tr>"""
    html_body = f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#f5f5f7;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f7;padding:32px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:#1d1d1f;border-radius:16px 16px 0 0;padding:28px 32px;">
    <p style="margin:0 0 4px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Trump Impact Monitor
    </p>
    <h1 style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display',Helvetica,Arial,sans-serif;
       font-size:26px;font-weight:700;color:#f5f5f7;letter-spacing:-0.02em;">
      {ticker}
    </h1>
    <p style="margin:8px 0 0 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:13px;color:#6e6e73;">
      {source} &nbsp;·&nbsp; {published}
    </p>
  </td></tr>

  <!-- BADGE (nur bei niedrig/claude) -->
  {"" if not badge.strip() else f'<tr><td style="background:#ffffff;padding:16px 32px 0;">' + badge + "</td></tr>"}

  <!-- QUELLTEXT -->
  <tr><td style="background:#ffffff;padding:24px 32px 16px;">
    <p style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Quelltext
    </p>
    <div style="background:#f5f5f7;border-radius:10px;padding:16px 18px;">
      <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
         font-size:14px;line-height:1.6;color:#1d1d1f;white-space:pre-wrap;">{raw_text}</p>
    </div>
    <p style="margin:10px 0 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:12px;color:#6e6e73;">
      <a href="{url}" style="color:#0071e3;text-decoration:none;">Original öffnen ↗</a>
    </p>
  </td></tr>

  <!-- TRENNLINIE -->
  <tr><td style="background:#ffffff;padding:0 32px;">
    <div style="border-top:1px solid #e5e5ea;"></div>
  </td></tr>

  <!-- CLAUDE-ANALYSE -->
  <tr><td style="background:#ffffff;padding:20px 32px 16px;">
    <p style="margin:0 0 12px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Claude-Analyse
    </p>
    <div style="font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;">
      {analysis_html}
    </div>
  </td></tr>

  <!-- TRENNLINIE -->
  <tr><td style="background:#ffffff;padding:0 32px;">
    <div style="border-top:1px solid #e5e5ea;"></div>
  </td></tr>

  <!-- MARKTDATEN -->
  <tr><td style="background:#ffffff;padding:20px 32px 16px;">
    <p style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Marktdaten – {ticker}
    </p>
    <div style="background:#f5f5f7;border-radius:10px;padding:14px 18px;">
      <pre style="margin:0;font-family:'SF Mono',Menlo,monospace;font-size:12px;
           line-height:1.6;color:#1d1d1f;white-space:pre-wrap;">{market_block}</pre>
    </div>
  </td></tr>
{perf_box}
  <!-- TURBO-EMPFEHLUNG -->
  <tr><td style="background:#ffffff;padding:0 32px 24px;">
    <p style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Turbo-Zertifikat-Empfehlung
    </p>
    <div style="background:#f0fdf4;border-radius:10px;padding:14px 18px;border:1px solid #d1fae5;">
      <pre style="margin:0;font-family:'SF Mono',Menlo,monospace;font-size:12px;
           line-height:1.6;color:#1d1d1f;white-space:pre-wrap;">{turbo_html}</pre>
    </div>
  </td></tr>

  <!-- TRUMP POSITIONEN -->
  <tr><td style="background:#ffffff;padding:0 32px;">
    <div style="border-top:1px solid #e5e5ea;"></div>
  </td></tr>
  <tr><td style="background:#ffffff;padding:20px 32px 24px;">
    <p style="margin:0 0 12px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Trumps bekannte Positionen
    </p>
    <div style="font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;">
      {holdings_html_block()}
    </div>
  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:20px 32px;
       border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;color:#6e6e73;line-height:1.6;">
      Modell: {MODEL} &nbsp;·&nbsp;
      Zeitfenster: letzte {LOOKBACK_HOURS}h<br>
      Generiert: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>
"""
    if direction == "NO_TRADE" and not SEND_NO_TRADE:
        log.info(f"  ⏭️  {ticker} NO_TRADE → kein Alert (config: send_no_trade=false)")
        return False  # analysiert, aber keine E-Mail

    dir_emoji = {"LONG": "📈", "SHORT": "📉"}.get(direction, "❓")
    conf_tag  = {"niedrig": " ⚠️", "claude": " 🤖"}.get(confidence, "")
    hold_tag  = " 💼" if holding_perf else ""  # Trump hält diese Aktie selbst
    subject   = f"{dir_emoji} Trump-Impact – {ticker}{conf_tag}{hold_tag} [{direction}] – {source}"
    sent = send_gmail(subject, html_body)
    if sent:
        log.info(f"  🎯 Alert gesendet: {ticker} | {direction} | {source}")
    return sent

# ─────────────────────────────────────────────────────────────────────────────
# BACKTESTING  –  Alert-Outcome nach 24h / 7d nachfüllen
# ─────────────────────────────────────────────────────────────────────────────
def record_outcomes():
    """
    Lädt Events ohne vollständige Outcome-Daten und füllt price_24h / price_7d nach.
    Limit 5 pro Run — verhindert yfinance Rate-Limit-Spam.
    Tickers mit 5+ Fehlschlägen werden für 24h übersprungen (yf_fail_count).
    """
    # Fehlerzähler-Tabelle sicherstellen
    conn.execute("""
        CREATE TABLE IF NOT EXISTS yf_failures (
            ticker      TEXT PRIMARY KEY,
            fail_count  INTEGER DEFAULT 0,
            last_fail   TEXT
        )
    """)
    conn.commit()

    rows = conn.execute("""
        SELECT e.event_id, e.ticker, e.processed_at,
               o.price_alert, o.price_24h, o.price_7d
        FROM events e
        LEFT JOIN outcomes o ON e.event_id = o.event_id
        LEFT JOIN yf_failures f ON e.ticker = f.ticker
        WHERE (o.event_id IS NULL
           OR (o.price_24h IS NULL AND e.processed_at < datetime('now', '-25 hours'))
           OR (o.price_7d  IS NULL AND e.processed_at < datetime('now', '-8 days')))
          AND (f.fail_count IS NULL OR f.fail_count < 5
               OR f.last_fail < datetime('now', '-24 hours'))
        LIMIT 5
    """).fetchall()

    if not rows:
        return

    log.info("Backtesting: %d Outcomes zu aktualisieren …", len(rows))
    seen_tickers: set[str] = set()
    for event_id, ticker, processed_at, price_alert, price_24h, price_7d in rows:
        if ticker in seen_tickers:
            continue  # gleichen Ticker nicht zweimal pro Run fetchen
        seen_tickers.add(ticker)
        data = fetch_market_data(ticker)
        if not data:
            # Fehlschlag zählen
            conn.execute("""
                INSERT INTO yf_failures (ticker, fail_count, last_fail) VALUES (?, 1, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    fail_count = fail_count + 1,
                    last_fail  = excluded.last_fail
            """, (ticker, now_utc().isoformat()))
            conn.commit()
            log.warning("  ⚠️  yfinance Fehlschlag #%d für %s",
                conn.execute("SELECT fail_count FROM yf_failures WHERE ticker=?",
                             (ticker,)).fetchone()[0], ticker)
            continue
        # Erfolg: Fehlerzähler zurücksetzen
        conn.execute("""
            INSERT INTO yf_failures (ticker, fail_count, last_fail) VALUES (?, 0, ?)
            ON CONFLICT(ticker) DO UPDATE SET fail_count=0, last_fail=excluded.last_fail
        """, (ticker, now_utc().isoformat()))
        conn.commit()
        current = data.get("price")

        # Alert-Preis beim ersten Mal setzen
        if price_alert is None:
            price_alert = current

        # 24h-Preis: nur setzen wenn >25h vergangen
        if price_24h is None:
            try:
                alert_dt = datetime.fromisoformat(processed_at.replace("Z", "+00:00"))
                if (now_utc() - alert_dt).total_seconds() > 90000:
                    price_24h = current
            except Exception:
                pass

        # 7d-Preis: nur setzen wenn >8 Tage vergangen
        if price_7d is None:
            try:
                alert_dt = datetime.fromisoformat(processed_at.replace("Z", "+00:00"))
                if (now_utc() - alert_dt).total_seconds() > 691200:
                    price_7d = current
            except Exception:
                pass

        conn.execute("""
            INSERT INTO outcomes (event_id, ticker, direction, price_alert, price_24h, price_7d, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                price_alert = COALESCE(excluded.price_alert, price_alert),
                price_24h   = COALESCE(excluded.price_24h,   price_24h),
                price_7d    = COALESCE(excluded.price_7d,    price_7d),
                checked_at  = excluded.checked_at
        """, (event_id, ticker, None, price_alert, price_24h, price_7d, now_utc().isoformat()))

    conn.commit()
    log.info("  ✅ Outcomes aktualisiert")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global CUTOFF
    CUTOFF = now_utc() - timedelta(hours=LOOKBACK_HOURS)

    log.info(f"\n{'═'*62}")
    log.info(f"  Trump-Impact Monitor  –  {now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log.info(f"  Zeitfenster: ab {CUTOFF.strftime('%Y-%m-%d %H:%M UTC')}  (letzte {LOOKBACK_HOURS}h)")
    log.info(f"  Modell: {MODEL}")
    log.info(f"{'═'*62}\n")

    processed   = 0   # Anzahl analysierter Events
    emails_sent = 0   # Anzahl tatsächlich versendeter Alert-E-Mails
    analyzed_log: list[dict] = []  # Für tägliche Summary
    seen_urls: set[str] = set()

    def _cap_reached() -> bool:
        if emails_sent >= MAX_ALERTS_PER_RUN:
            log.warning(f"  ⚠️  Alert-Cap ({MAX_ALERTS_PER_RUN}) erreicht – verbleibende Artikel übersprungen.")
            return True
        return False

    def _run_analysis(source, ts, text, ticker, url, confidence):
        nonlocal processed, emails_sent
        processed += 1
        sent = bool(analyze_and_alert(source, ts, text, ticker, url, confidence))
        if sent:
            emails_sent += 1
        analyzed_log.append({"ticker": ticker, "source": source, "sent": sent})

    def _sorted_tickers(tickers: list) -> list:
        """Sortiert hoch vor niedrig/claude, begrenzt auf MAX_TICKERS_PER_ART."""
        high = [(t, c) for t, c in tickers if c == "hoch"]
        rest = [(t, c) for t, c in tickers if c != "hoch"]
        return (high + rest)[:MAX_TICKERS_PER_ART]

    # ── Truth Social ──────────────────────────────────────────────────────────
    log.info("📡 Truth Social …")
    for post in (fetch_truth_social() if SRC_TRUTH else []):
        if _cap_reached():
            break
        text = clean_text(post.get("text", post.get("content", "")))
        if not text:
            continue
        if text.startswith("RT @") and not INCLUDE_RETWEETS:
            continue
        ts = post.get("created_at", post.get("published"))
        if not is_recent(ts):
            continue
        if not is_financially_relevant(text, truth_social=True):
            continue
        tickers = find_all_tickers(text)
        if not tickers:
            tickers = discover_tickers_via_claude(text)   # Sektor-Inferenz als Fallback
        if not tickers:
            continue
        post_url = post.get("url", post.get("uri", "https://truthsocial.com/@realDonaldTrump"))
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            if already_seen(event_hash(ticker, text)):
                continue
            _run_analysis("Truth Social", ts, text, ticker, post_url, confidence)

    # ── News-RSS (Google News + Finanz-Feeds) ────────────────────────────────
    log.info("\n📰 Nachrichten-RSS …")
    for article in (fetch_financial_rss() if SRC_RSS else []):
        if _cap_reached():
            break
        art_url = article.get("url", "")
        if art_url and art_url in seen_urls:
            continue
        if art_url:
            seen_urls.add(art_url)
        text = clean_text(
            (article.get("title") or "") + " " + (article.get("description") or "")
        )
        if not text:
            continue
        if not is_recent(article.get("publishedAt")):
            continue
        if not mentions_trump(text):
            continue
        if not is_financially_relevant(text):
            continue
        tickers = find_all_tickers(text)
        if not tickers:
            tickers = discover_tickers_via_claude(text)  # Fallback wie bei Truth Social
        if not tickers:
            continue
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            if already_seen(event_hash(ticker, text)):
                continue
            _run_analysis(
                article.get("_source", "RSS"),
                article.get("publishedAt", ""),
                text, ticker, art_url, confidence,
            )

    # ── White House RSS ───────────────────────────────────────────────────────
    log.info("\n🏛️  White House RSS …")
    for entry in (fetch_whitehouse() if SRC_WHITEHOUSE else []):
        if _cap_reached():
            break
        text = clean_text(entry.get("title", "") + " " + entry.get("summary", ""))
        if not text:
            continue
        ts = entry.get("published_parsed") or entry.get("updated_parsed")
        if not is_recent(ts):
            continue
        if not is_financially_relevant(text):
            continue
        tickers = find_all_tickers(text)
        if not tickers:
            continue
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            if already_seen(event_hash(ticker, text)):
                continue
            _run_analysis(
                "White House", entry.get("published", ""),
                text, ticker,
                entry.get("link", "https://www.whitehouse.gov"), confidence,
            )

    # ── Federal Register (Executive Orders, Proklamationen) ──────────────────
    log.info("\n📜 Federal Register …")
    for doc in (fetch_federal_register() if SRC_FEDREGISTER else []):
        if _cap_reached():
            break
        doc_url = doc.get("url", "")
        if doc_url and doc_url in seen_urls:
            continue
        if doc_url:
            seen_urls.add(doc_url)
        text = clean_text(
            (doc.get("title") or "") + " " + (doc.get("description") or "")
        )
        if not text:
            continue
        if not is_recent(doc.get("publishedAt")):
            continue
        if not is_financially_relevant(text):
            tickers = discover_tickers_via_claude(text)  # EOs haben oft keinen direkten Ticker
        else:
            tickers = find_all_tickers(text)
            if not tickers:
                tickers = discover_tickers_via_claude(text)
        if not tickers:
            continue
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            if already_seen(event_hash(ticker, text)):
                continue
            _run_analysis(
                "Federal Register", doc.get("publishedAt", ""),
                text, ticker, doc_url, confidence,
            )

    if SRC_EDGAR:
        check_edgar_alerts()
    if SRC_OGE:
        check_oge_alerts()
    record_outcomes()

    log.info(f"\n{'═'*62}")
    log.info(f"  ✅ Durchlauf beendet – {emails_sent} E-Mail(s) verschickt, {processed} analysiert")
    log.info(f"{'═'*62}\n")

    # Tägliche Summary (nur einmal pro Tag, wenn keine Alerts verschickt wurden)
    _maybe_send_daily_summary(analyzed_log, emails_sent)

    # WAL explizit in alerts.db zurückschreiben — der Workflow committet nur
    # alerts.db, nicht alerts.db-wal. Ohne Checkpoint ginge der Dedup-Stand
    # verloren → doppelte E-Mails im nächsten Run.
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"  ⚠️  WAL-Checkpoint fehlgeschlagen: {e}")


def _maybe_send_daily_summary(analyzed_log: list[dict], emails_sent: int) -> None:
    """
    Schickt einmal täglich (UTC 20:00-21:00) eine Summary-E-Mail wenn
    in diesem Run keine Alerts verschickt wurden — damit du weißt dass
    das System aktiv ist und was analysiert wurde.
    """
    now = now_utc()
    # Nur im Abend-Run (20-21 UTC) und nur wenn keine Alerts heute
    if not (20 <= now.hour < 21):
        return
    today = now.strftime("%Y-%m-%d")
    # Prüfen ob heute schon eine Summary geschickt wurde
    sent_today = conn.execute(
        "SELECT 1 FROM events WHERE source='DAILY_SUMMARY' AND DATE(processed_at)=?",
        (today,)
    ).fetchone()
    if sent_today:
        return
    # Nur senden wenn heute keine Alert-E-Mails rausgingen
    alerts_today = conn.execute(
        "SELECT COUNT(*) FROM events WHERE source != 'DAILY_SUMMARY' AND DATE(processed_at)=?",
        (today,)
    ).fetchone()[0]
    if alerts_today > 0:
        return

    # Summary bauen
    analyzed_tickers = [e["ticker"] for e in analyzed_log if not e["sent"]]
    ticker_counts: dict = {}
    for t in analyzed_tickers:
        ticker_counts[t] = ticker_counts.get(t, 0) + 1
    top = sorted(ticker_counts.items(), key=lambda x: -x[1])[:10]

    rows_html = "".join(
        f'<tr><td style="padding:4px 12px 4px 0;font-size:12px;font-weight:600;">{t}</td>'
        f'<td style="padding:4px 0;font-size:12px;color:#6e6e73;">{n}× analysiert — kein Signal</td></tr>'
        for t, n in top
    ) or '<tr><td colspan="2" style="padding:8px 0;font-size:12px;color:#9ca3af;">Keine Analysen heute</td></tr>'

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#f5f5f7;margin:0;padding:32px 0;">
<table width="600" cellpadding="0" cellspacing="0" style="margin:0 auto;max-width:600px;">
  <tr><td style="background:#1d1d1f;border-radius:16px 16px 0 0;padding:24px 32px;">
    <p style="margin:0 0 4px;font-size:11px;font-weight:600;color:#6e6e73;text-transform:uppercase;letter-spacing:.08em;">
      Trump Impact Monitor · Tages-Summary
    </p>
    <h1 style="margin:0;font-size:20px;font-weight:700;color:#f5f5f7;">
      Heute keine Kauf-/Verkaufs-Signale
    </h1>
    <p style="margin:6px 0 0;font-size:12px;color:#6e6e73;">{today} · {len(analyzed_log)} Events analysiert · 0 Alerts</p>
  </td></tr>
  <tr><td style="background:#fff;padding:20px 32px 24px;">
    <p style="margin:0 0 10px;font-size:11px;font-weight:700;color:#6e6e73;text-transform:uppercase;letter-spacing:.06em;">
      Analysierte Ticker (kein actionable Signal)
    </p>
    <table style="border-collapse:collapse;width:100%;">{rows_html}</table>
    <p style="margin:16px 0 0;font-size:11px;color:#9ca3af;">
      Das System läuft normal. Du erhältst eine E-Mail sobald ein konkretes Signal erkannt wird.
    </p>
  </td></tr>
  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:12px 32px;border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-size:11px;color:#9ca3af;">{now.strftime('%Y-%m-%d %H:%M UTC')}</p>
  </td></tr>
</table></body></html>"""

    send_gmail(f"📊 Trump Monitor – Kein Signal heute ({today})", html)
    # Als Sentinel in DB speichern
    conn.execute(
        "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?)",
        (f"daily-summary-{today}", "DAILY_SUMMARY", today, "", f"daily-summary-{today}", "", now.isoformat()),
    )
    conn.commit()
    log.info("  📊 Tages-Summary verschickt")


if __name__ == "__main__":
    main()
