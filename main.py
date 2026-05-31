import os
import re
import json
import sqlite3
import hashlib
import socket
import html
import feedparser
import requests
import yfinance as yf
from functools import lru_cache
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
    "SCRAPE_CREATORS_API_KEY": SCRAPE_KEY,
    "GMAIL_EMAIL":             GMAIL_EMAIL,
    "GMAIL_APP_PASSWORD":      GMAIL_PASS,
    "RECIPIENT_EMAIL":         RECIPIENT,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    log.error(f"❌ Fehlende Secrets: {', '.join(missing)}")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# SQLITE  –  Dedup-Datenbank
# ─────────────────────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
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
# FINBERT  –  lokales Finanz-Sentiment (lazy-loaded)
# ─────────────────────────────────────────────────────────────────────────────
_finbert_pipeline = None

def get_finbert_sentiment(text: str) -> str:
    """
    Lädt ProsusAI/finbert beim ersten Aufruf und gibt
    'positiv (92.3%)'  /  'negativ (87.1%)'  /  'neutral (76.0%)'  zurück.
    Bei Fehler: 'nicht verfügbar'.
    """
    global _finbert_pipeline
    try:
        if _finbert_pipeline is None:
            from transformers import pipeline as hf_pipeline
            log.info("  🧠 FinBERT wird geladen …")
            _finbert_pipeline = hf_pipeline(
                "text-classification",
                model="ProsusAI/finbert",
                truncation=True,
                max_length=512,
            )
        result = _finbert_pipeline(text[:512])[0]
        label_map = {"positive": "positiv", "negative": "negativ", "neutral": "neutral"}
        label = label_map.get(result["label"].lower(), result["label"].lower())
        score = round(result["score"] * 100, 1)
        return f"{label} ({score}%)"
    except Exception as e:
        log.warning(f"  ⚠️  FinBERT Fehler: {e}")
        return "nicht verfügbar"

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
    text = re.sub(r'https?://\S+', '', text)   # URLs weg
    text = re.sub(r'<[^>]+>', ' ', text)        # HTML-Tags weg
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

def find_all_tickers(text: str) -> list[tuple[str, str]]:
    """
    Tier 1 – symbol  : case-sensitiv,   immer          → "hoch"
    Tier 2 – company : case-insensitiv, auch Plural     → "hoch"
    Tier 3 – weak    : case-insensitiv, Finanzkontext,
                       Alias-Mindestlänge ≥5            → "niedrig"
    Tier 4 – Plural-Normalisierung: "Apples" → "Apple"
    """
    results:     list[tuple[str, str]] = []
    seen:        set[str]              = set()
    has_finance: bool                  = is_financially_relevant(text)

    # Tier 4: Plural/Possessiv normalisieren vor Tier-2-Matching
    normalized = re.sub(r"'s\b", "", text)                    # Apple's → Apple
    normalized = re.sub(r"(\b[A-Za-z]{3,})(s)\b",            # Apples → Apple
                        lambda m: m.group(1), normalized)

    for ticker, tiers in ENTITIES.items():
        t = ticker.upper()
        if t in seen:
            continue
        matched = False

        # Tier 1 — Ticker-Symbol, case-sensitiv
        for alias in tiers.get("symbol", []):
            if alias and re.search(r'\b' + re.escape(alias) + r'\b', text):
                results.append((t, "hoch"))
                seen.add(t)
                matched = True
                break
        if matched:
            continue

        # Tier 2 — Firmenname/CEO, auch auf normalisiertem Text
        for alias in tiers.get("company", []):
            if re.search(r'\b' + re.escape(alias) + r'\b', normalized, re.IGNORECASE):
                results.append((t, "hoch"))
                seen.add(t)
                matched = True
                break
        if matched:
            continue

        # Tier 3 — schwache Aliases, nur mit Finanzkontext, Mindestlänge ≥5
        if has_finance:
            for alias in tiers.get("weak", []):
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
    """Fallback: ScapeCreators API (kostenpflichtig, aber zuverlässig)."""
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
    """CNN-Archiv zuerst, ScapeCreators als Fallback."""
    try:
        posts = _fetch_truth_cnn()
        log.info(f"  Truth Social (CNN-Archiv): {len(posts)} Posts")
        if posts:
            return posts
    except Exception as e:
        log.warning(f"  ⚠️  CNN-Archiv nicht verfügbar: {e} → ScapeCreators Fallback")
    posts = _fetch_truth_scrapecreators()
    log.info(f"  Truth Social (ScapeCreators): {len(posts)} Posts")
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


def holdings_html_block() -> str:
    """Generiert HTML-Tabelle aller bekannten Trump-Positionen für E-Mail-Footer."""
    rows = ""
    for h in TRUMP_KNOWN_HOLDINGS:
        rows += (
            f'<tr>'
            f'<td style="padding:6px 12px 6px 0;font-size:12px;color:#1d1d1f;'
            f'vertical-align:top;border-bottom:1px solid #f0f0f0;">'
            f'<a href="{h["url"]}" style="color:#0071e3;text-decoration:none;">'
            f'{h["asset"]}</a></td>'
            f'<td style="padding:6px 12px 6px 0;font-size:12px;color:#6e6e73;'
            f'vertical-align:top;border-bottom:1px solid #f0f0f0;">{h["type"]}</td>'
            f'<td style="padding:6px 12px 6px 0;font-size:12px;color:#1d1d1f;'
            f'vertical-align:top;border-bottom:1px solid #f0f0f0;">{h["stake"]}</td>'
            f'<td style="padding:6px 0 6px 0;font-size:11px;color:#6e6e73;'
            f'vertical-align:top;border-bottom:1px solid #f0f0f0;white-space:nowrap;">'
            f'{h["disclosed"]}<br><span style="font-size:10px;">{h["source"]}</span></td>'
            f'</tr>'
        )
    return f"""
<table style="border-collapse:collapse;width:100%;margin-top:4px;">
  <thead>
    <tr>
      <th style="padding:0 12px 8px 0;font-size:11px;font-weight:600;color:#6e6e73;
          text-align:left;text-transform:uppercase;letter-spacing:0.06em;">Position</th>
      <th style="padding:0 12px 8px 0;font-size:11px;font-weight:600;color:#6e6e73;
          text-align:left;text-transform:uppercase;letter-spacing:0.06em;">Typ</th>
      <th style="padding:0 12px 8px 0;font-size:11px;font-weight:600;color:#6e6e73;
          text-align:left;text-transform:uppercase;letter-spacing:0.06em;">Beteiligung</th>
      <th style="padding:0 0 8px 0;font-size:11px;font-weight:600;color:#6e6e73;
          text-align:left;text-transform:uppercase;letter-spacing:0.06em;">Offengelegt</th>
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"""


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
            feed = feedparser.parse(url)
            arts = [_rss_to_dict(e, name) for e in feed.entries[:20]]
            results.extend(arts)
            log.info(f"  {name}: {len(arts)} Artikel")
        except Exception as ex:
            log.warning(f"  ⚠️  {name} Fehler: {ex}")
    return results

def fetch_whitehouse() -> list:
    try:
        feed    = feedparser.parse("https://www.whitehouse.gov/feed/")
        entries = feed.entries[:30]
        log.info(f"  White House RSS: {len(entries)} Einträge")
        return entries
    except Exception as e:
        log.warning(f"  ⚠️  White House RSS Fehler: {e}")
        return []


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
    """
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
            feed = feedparser.parse(rss_url)
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


def parse_oge_ptr_pdf(pdf_url: str) -> list[dict]:
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
        r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=30)
        r.raise_for_status()
        transactions = []
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
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
        return transactions
    except Exception as e:
        log.warning(f"  ⚠️  OGE PDF Parse Fehler ({pdf_url}): {e}")
        return []


def send_oge_alert(pdf_url: str, source: str, transactions: list[dict]) -> None:
    """Sendet E-Mail-Alert für neuen OGE 278-T Periodic Transaction Report."""
    tx_rows = ""
    for tx in transactions[:20]:  # max 20 Zeilen
        color = "#d1fae5" if tx["tx_type"] == "KAUF" else \
                "#fee2e2" if tx["tx_type"] == "VERKAUF" else "#f5f5f7"
        emoji = "📈" if tx["tx_type"] == "KAUF" else \
                "📉" if tx["tx_type"] == "VERKAUF" else "🔄"
        tx_rows += (
            f'<tr style="background:{color};">'
            f'<td style="padding:8px 12px;font-size:13px;color:#1d1d1f;">'
            f'{emoji} {tx["asset"]}</td>'
            f'<td style="padding:8px 12px;font-size:13px;font-weight:600;color:#1d1d1f;">'
            f'{tx["tx_type"]}</td>'
            f'<td style="padding:8px 12px;font-size:11px;color:#6e6e73;">'
            f'{tx["row_text"][:120]}</td>'
            f'</tr>'
        )

    if not tx_rows:
        tx_rows = (
            '<tr><td colspan="3" style="padding:12px;font-size:13px;color:#6e6e73;">'
            'Keine Transaktionszeilen automatisch erkannt — bitte PDF manuell prüfen.'
            '</td></tr>'
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
      OGE Form 278-T · Periodic Transaction Report
    </p>
    <h1 style="margin:0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:24px;font-weight:700;color:#f5f5f7;letter-spacing:-0.02em;">
      🏛️ Neuer Trump-Transaktionsbericht
    </h1>
    <p style="margin:8px 0 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:13px;color:#6e6e73;">
      Quelle: {source} · Entdeckt: {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

  <tr><td style="background:#ffffff;padding:24px 32px 8px;">
    <p style="margin:0 0 10px;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Erkannte Transaktionen ({len(transactions)})
    </p>
    <table style="border-collapse:collapse;width:100%;border-radius:10px;overflow:hidden;">
      <thead>
        <tr style="background:#f5f5f7;">
          <th style="padding:8px 12px;font-size:11px;color:#6e6e73;text-align:left;">Asset</th>
          <th style="padding:8px 12px;font-size:11px;color:#6e6e73;text-align:left;">Typ</th>
          <th style="padding:8px 12px;font-size:11px;color:#6e6e73;text-align:left;">Details</th>
        </tr>
      </thead>
      <tbody>{tx_rows}</tbody>
    </table>
    <p style="margin:16px 0 0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:13px;">
      <a href="{pdf_url}" style="color:#0071e3;text-decoration:none;">
        📄 Original-PDF öffnen ↗
      </a>
    </p>
  </td></tr>

  <tr><td style="background:#ffffff;padding:0 32px;">
    <div style="border-top:1px solid #e5e5ea;margin-top:16px;"></div>
  </td></tr>

  <tr><td style="background:#ffffff;padding:20px 32px 24px;">
    <p style="margin:0 0 12px;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Trumps bekannte Positionen
    </p>
    {holdings_html_block()}
  </td></tr>

  <tr><td style="background:#f5f5f7;border-radius:0 0 16px 16px;padding:16px 32px;
       border-top:1px solid #e5e5ea;">
    <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;
       font-size:11px;color:#6e6e73;">
      OGE Form 278-T · Automatisch erkannt via {source} ·
      {now_utc().strftime('%Y-%m-%d %H:%M UTC')}
    </p>
  </td></tr>

</table></td></tr></table>
</body></html>"""

    subject = f"🏛️ OGE 278-T: Neuer Trump-Transaktionsbericht ({len(transactions)} Positionen)"
    send_gmail(subject, html_body)
    log.info(f"  📨 OGE Alert gesendet: {len(transactions)} Transaktionen | {source}")


OGE_LOOKBACK_DAYS = 90  # Nur PDFs der letzten 90 Tage alertieren (wie EDGAR)

def _oge_date_from_url(pdf_url: str) -> str:
    """Extrahiert YYYY/MM aus whitehouse.gov-URL z.B. /2025/06/..."""
    m = re.search(r'/(\d{4})/(\d{2})/', pdf_url)
    return f"{m.group(1)}-{m.group(2)}-01" if m else ""


def check_oge_alerts() -> None:
    """
    Prüft auf neue OGE 278-T PDFs.
    - Datum-Filter: nur PDFs der letzten 90 Tage alertieren
    - Kein Alert bei 0 erkannten Transaktionen (PDF-Parser versagt)
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
    conn.commit()

    links = fetch_oge_ptr_links()
    if not links:
        log.info("OGE: Keine PTR-Links gefunden")
        return

    cutoff = (now_utc() - timedelta(days=OGE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    new_count = 0

    for item in links:
        pdf_url = item["pdf_url"]
        source  = item["source"]

        # Datum-Filter: historische PDFs in DB speichern ohne Alert
        pdf_date = _oge_date_from_url(pdf_url)
        if pdf_date and pdf_date < cutoff:
            conn.execute(
                "INSERT OR IGNORE INTO oge_ptrs VALUES (?,?,?,?)",
                (pdf_url, source, -1, "HISTORICAL-NO-ALERT"),
            )
            continue

        exists = conn.execute(
            "SELECT 1 FROM oge_ptrs WHERE pdf_url=?", (pdf_url,)
        ).fetchone()
        if exists:
            continue

        log.info("Neues OGE PDF: %s…", pdf_url[:80])
        transactions = parse_oge_ptr_pdf(pdf_url)

        conn.execute(
            "INSERT OR IGNORE INTO oge_ptrs VALUES (?,?,?,?)",
            (pdf_url, source, len(transactions), now_utc().isoformat()),
        )
        conn.commit()

        # Bug 2 fix: kein Alert bei 0 erkannten Transaktionen
        if not transactions:
            log.warning(
                "OGE PDF geparst aber 0 Transaktionen erkannt — kein Alert: %s", pdf_url
            )
            continue

        send_oge_alert(pdf_url, source, transactions)
        new_count += 1

    conn.commit()
    if new_count == 0:
        log.info("OGE: Keine neuen PTR-Berichte")

# ─────────────────────────────────────────────────────────────────────────────
# TRUMP HOLDINGS  (OGE Form 278 – öffentlich)
# ─────────────────────────────────────────────────────────────────────────────
TRUMP_HOLDINGS = {
    "DJT": "JA – Trump hält ~57 % an Trump Media & Technology Group (DJT), Quelle: SEC Form 4 / OGE 2024",
}

def trump_holding_info(ticker: str) -> str:
    return TRUMP_HOLDINGS.get(
        ticker.upper(),
        "Nicht aus öffentlichen OGE-Filings (Form 278) bekannt – keine Annahmen."
    )

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

@lru_cache(maxsize=512)
def fetch_market_data(ticker: str) -> dict:
    """Holt 1-Monats-History von Yahoo Finance. Bei Fehler leeres Dict."""
    yf_sym = YF_TICKER_MAP.get(ticker.upper(), ticker.upper())
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
        return {
            "price":   current,
            "chg_1d":  round((current / prev_close - 1) * 100, 2),
            "chg_1w":  round((current / week_ago   - 1) * 100, 2),
            "chg_1m":  round((current / month_ago  - 1) * 100, 2),
        }
    except Exception as e:
        log.warning(f"  ⚠️  Yahoo Finance ({ticker}): {e}")
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
    "konservativ": {"puffer": 0.20, "hebel_min": 5,  "hebel_max": 9},
    "mittel":      {"puffer": 0.16, "hebel_min": 8,  "hebel_max": 14},
    "aggressiv":   {"puffer": 0.13, "hebel_min": 12, "hebel_max": 18},
}
TURBO_RISIKO = "mittel"   # global konfigurierbar


def _onvista_url(ticker: str, direction: str) -> str:
    """
    Erzeugt einen direkten onvista Knock-Out-Finder-Link.
    onvista kennt US-Tickers nicht direkt — wir landen auf der Suchergebnisseite.
    """
    richtung = "call" if direction == "LONG" else "put"
    # Direkt-URL zum Knock-Out-Finder mit Suchbegriff
    return (
        f"https://www.onvista.de/derivate/Knock-Outs"
        f"?TYPE=KNOCK_OUT&UNDERLYING_SEARCH={ticker}"
        f"&OPTION_TYPE={richtung.upper()}"
        f"&ISSUER=Vontobel,SG,HSBC,BNP"
    )


def _calc_atm_iv(ticker: str) -> float | None:
    """
    Schätzt implizite Volatilität aus 30-Tage historischer Volatilität (yfinance).
    Gibt None zurück wenn nicht berechenbar.
    """
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
    Berechnet optimale Turbo-Parameter basierend auf:
    - Aktuellem Kurs (yfinance)
    - Historischer Volatilität (30T HV als IV-Proxy)
    - Risikobereitschaft (TURBO_RISIKO)

    Kriterien für News-Trade nach Trump-Post:
    - KO-Abstand: 13–23 % (Puffer gegen Gap-Risiko)
    - Hebel: 7–16x
    - Spread: < 0.8 % (manuell auf onvista prüfen)
    - Emittenten: Vontobel, SG, HSBC, BNP
    - Typ: Open End Turbo (keine Laufzeit)
    - Preis: 4–25 € (praktisch handelbar)
    """
    if direction == "UNKLAR":
        return "⛔ Keine Empfehlung – Trade-Richtung unklar"

    data = fetch_market_data(ticker)
    if not data:
        return "⛔ Keine Empfehlung – Marktdaten nicht verfügbar"

    price  = data["price"]
    prof   = _TURBO_PROFILES[TURBO_RISIKO]
    hv     = _calc_atm_iv(ticker)

    # Puffer dynamisch: bei hoher Volatilität größerer KO-Abstand
    puffer = prof["puffer"]
    if hv is not None and hv > 0.45:   # >45 % annualisierte Vol → konservativer
        puffer = min(puffer + 0.04, 0.25)
    elif hv is not None and hv < 0.20: # <20 % Vol → etwas aggressiver okay
        puffer = max(puffer - 0.02, 0.13)

    onvista_url = _onvista_url(ticker, direction)

    if direction == "LONG":
        ko       = round(price * (1 - puffer), 2)
        abstand  = round(puffer * 100, 1)
        hebel_lo = prof["hebel_min"]
        hebel_hi = prof["hebel_max"]
        approx_lever = round(price / (price - ko), 1)
        hv_info  = f"{hv*100:.1f}% (30T HV)" if hv else "k.A."
        return (
            f"📈 LONG Open End Turbo auf {ticker}\n"
            f"\n"
            f"   Kurs aktuell:      {price:.2f} USD\n"
            f"   Volatilität:       {hv_info}\n"
            f"   ──────────────────────────────────\n"
            f"   Empf. KO-Bereich:  ≤ {ko:.2f} USD  ({abstand}% Abstand)\n"
            f"   Hebel-Ziel:        {hebel_lo}–{hebel_hi}x  (approx. ~{approx_lever}x)\n"
            f"   ──────────────────────────────────\n"
            f"   Emittenten:        Vontobel · SG · HSBC · BNP\n"
            f"   Max. Spread:       < 0.8 %\n"
            f"   Preis-Ziel:        4–25 €\n"
            f"   Typ:               Open End (keine Laufzeit)\n"
            f"\n"
            f"   🔍 onvista Finder:\n"
            f"   {onvista_url}"
        )
    else:  # SHORT
        ko       = round(price * (1 + puffer), 2)
        abstand  = round(puffer * 100, 1)
        hebel_lo = prof["hebel_min"]
        hebel_hi = prof["hebel_max"]
        approx_lever = round(price / (ko - price), 1)
        hv_info  = f"{hv*100:.1f}% (30T HV)" if hv else "k.A."
        return (
            f"📉 SHORT Open End Turbo auf {ticker}\n"
            f"\n"
            f"   Kurs aktuell:      {price:.2f} USD\n"
            f"   Volatilität:       {hv_info}\n"
            f"   ──────────────────────────────────\n"
            f"   Empf. KO-Bereich:  ≥ {ko:.2f} USD  ({abstand}% Abstand)\n"
            f"   Hebel-Ziel:        {hebel_lo}–{hebel_hi}x  (approx. ~{approx_lever}x)\n"
            f"   ──────────────────────────────────\n"
            f"   Emittenten:        Vontobel · SG · HSBC · BNP\n"
            f"   Max. Spread:       < 0.8 %\n"
            f"   Preis-Ziel:        4–25 €\n"
            f"   Typ:               Open End (keine Laufzeit)\n"
            f"\n"
            f"   🔍 onvista Finder:\n"
            f"   {onvista_url}"
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
    Fragt Claude welche börsennotierten Unternehmen durch den Post betroffen sind.
    Gibt max. 3 (ticker, 'claude') Tupel zurück, oder [] bei keinem Treffer.
    """
    prompt = (
        "Trump hat folgenden Text auf Truth Social gepostet:\n\n"
        f"{text}\n\n"
        "Welche börsennotierten US-Unternehmen sind dadurch am wahrscheinlichsten "
        "DIREKT und KONKRET betroffen (Kursreaktion realistisch)? "
        "Antworte NUR mit kommaseparierten Ticker-Symbolen, max. 3 (z.B. NVDA,TSM,INTC). "
        "Falls kein konkreter Unternehmensbezug erkennbar: antworte nur mit NONE"
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


def _haiku_tradeable(ticker: str, text: str, finbert: str) -> bool:
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

    # Low-Priorität: Tier-3/Claude-Inferenz-Treffer sofort verwerfen
    if priority in ("low", "unknown") and confidence in ("niedrig", "claude"):
        log.info(f"  ⏭️  {ticker} [{priority}] + {confidence} Konfidenz → übersprungen")
        return

    # ── Stufe 0: FinBERT + Marktdaten (günstig, lokal) ───────────────────────
    finbert_sent = get_finbert_sentiment(raw_text)
    market_data  = fetch_market_data(ticker)
    market_block = format_market_block(ticker)
    holding_info = trump_holding_info(ticker)

    # ── Rate-Limit-Check (vor jedem Sonnet-Call) ─────────────────────────────
    if not _rate_limit_ok(ticker):
        return

    # ── Stufe 1: Haiku-Tradability-Screen (nur Medium/Low/Unknown) ───────────
    if priority != "high" and not _haiku_tradeable(ticker, raw_text, finbert_sent):
        return  # High-Priority-Ticker überspringen diesen Screen

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
FINBERT: {finbert_sent}

ANALYSIS FORMAT — respond exactly:

RELEVANCE: [YES / NO] — {ticker} is [directly named / sector-affected / tangentially mentioned]
COMPANY: [Full legal name] ({ticker})
EVENT_SUMMARY: [One sentence: what Trump said/did]
SENTIMENT: [BULLISH / BEARISH / NEUTRAL] for {ticker}
SENTIMENT_BASIS: [Quote from text — max 15 words]
FINBERT_ALIGNMENT: [AGREES / DISAGREES / PARTIAL]
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

    # ── HTML-E-Mail ──────────────────────────────────────────────────────────
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

  <!-- TURBO-EMPFEHLUNG -->
  <tr><td style="background:#ffffff;padding:0 32px 24px;">
    <p style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;
       font-size:11px;font-weight:600;letter-spacing:0.08em;color:#6e6e73;text-transform:uppercase;">
      Turbo-Zertifikat-Empfehlung
    </p>
    <div style="background:#f0fdf4;border-radius:10px;padding:14px 18px;border:1px solid #d1fae5;">
      <pre style="margin:0;font-family:'SF Mono',Menlo,monospace;font-size:12px;
           line-height:1.6;color:#1d1d1f;white-space:pre-wrap;">{turbo_block.replace(
               _onvista_url(ticker, turbo_dir),
               f'<a href="{_onvista_url(ticker, turbo_dir)}" style="color:#0071e3;">onvista Knock-Out-Finder öffnen ↗</a>'
           )}</pre>
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
      FinBERT-Sentiment: {finbert_sent} &nbsp;·&nbsp;
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
        return

    dir_emoji = {"LONG": "📈", "SHORT": "📉"}.get(direction, "❓")
    conf_tag  = {"niedrig": " ⚠️", "claude": " 🤖"}.get(confidence, "")
    subject   = f"{dir_emoji} Trump-Impact – {ticker}{conf_tag} [{direction}] – {source}"
    send_gmail(subject, html_body)
    log.info(f"  🎯 Alert gesendet: {ticker} | {direction} | {source} | FinBERT: {finbert_sent}")

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
        time.sleep(2)
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

    processed  = 0
    seen_urls: set[str] = set()

    def _cap_reached() -> bool:
        if processed >= MAX_ALERTS_PER_RUN:
            log.warning(f"  ⚠️  Alert-Cap ({MAX_ALERTS_PER_RUN}) erreicht – verbleibende Artikel übersprungen.")
            return True
        return False

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
            analyze_and_alert("Truth Social", ts, text, ticker, post_url, confidence)
            processed += 1

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
            analyze_and_alert(
                article.get("_source", "RSS"),
                article.get("publishedAt", ""),
                text,
                ticker,
                art_url,
                confidence,
            )
            processed += 1

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
            analyze_and_alert(
                "White House",
                entry.get("published", ""),
                text,
                ticker,
                entry.get("link", "https://www.whitehouse.gov"),
                confidence,
            )
            processed += 1

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
            analyze_and_alert(
                "Federal Register",
                doc.get("publishedAt", ""),
                text,
                ticker,
                doc_url,
                confidence,
            )
            processed += 1

    if SRC_EDGAR:
        check_edgar_alerts()
    if SRC_OGE:
        check_oge_alerts()
    record_outcomes()

    log.info(f"\n{'═'*62}")
    log.info(f"  ✅ Durchlauf beendet – {processed} Alert(s) verarbeitet")
    log.info(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
