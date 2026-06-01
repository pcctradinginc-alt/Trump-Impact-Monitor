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


_VALUE_SORT_KEY = {
    "Over $50,000,000": 0, "$25,000,001 - $50,000,000": 1,
    "$5,000,001 - $25,000,000": 2, "$1,000,001 - $5,000,000": 3,
    "$500,001 - $1,000,000": 4, "$250,001 - $500,000": 5,
    "$100,001 - $250,000": 6, "$50,001 - $100,000": 7,
    "$15,001 - $50,000": 8, "$1,001 - $15,000": 9,
    "$1,001 - $15,000": 9, "None (or less than $1,001)": 10,
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

    def _ptr_badge(ticker):
        r = ptr_map.get(ticker)
        if not r:
            return ""
        _, _, buys, sells, last_date, history = r
        net  = buys - sells
        txs  = [x.split("|") for x in (history or "").split(",") if x]
        last = txs[-1][0] if txs else ""
        prev_net = sum(1 if t[0]=="KAUF" else -1 if t[0]=="VERKAUF" else 0 for t in txs[:-1])
        if net <= 0:
            return ('<span style="background:#fee2e2;color:#dc2626;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;margin-left:4px;">VERKAUFT</span>')
        if prev_net <= 0:
            return ('<span style="background:#d1fae5;color:#059669;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;margin-left:4px;">NEU</span>')
        if last == "KAUF":
            return (f'<span style="background:#dbeafe;color:#2563eb;font-size:10px;'
                    f'font-weight:700;padding:2px 6px;border-radius:4px;margin-left:4px;">'
                    f'+{buys}× KAUF</span>')
        if last == "VERKAUF" and net > 0:
            return ('<span style="background:#fef3c7;color:#d97706;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;margin-left:4px;">REDUZIERT</span>')
        return ""

    def _badge_ptr_only(buys, sells, history):
        net = buys - sells
        txs = [x.split("|") for x in (history or "").split(",") if x]
        last = txs[-1][0] if txs else ""
        prev_net = sum(1 if t[0]=="KAUF" else -1 if t[0]=="VERKAUF" else 0 for t in txs[:-1])
        if net <= 0:
            return ('<span style="background:#fee2e2;color:#dc2626;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">VERKAUFT</span>')
        if prev_net <= 0 and net > 0:
            return ('<span style="background:#d1fae5;color:#059669;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">NEU</span>')
        if last == "KAUF":
            return ('<span style="background:#dbeafe;color:#2563eb;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">AUFGESTOCKT</span>')
        if last == "VERKAUF" and net > 0:
            return ('<span style="background:#fef3c7;color:#d97706;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">REDUZIERT</span>')
        return ('<span style="background:#f0f0f0;color:#6e6e73;font-size:10px;'
                'font-weight:700;padding:2px 6px;border-radius:4px;">GEHALTEN</span>')
        if prev_net <= 0 and net > 0:
            return ('<span style="background:#d1fae5;color:#059669;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">NEU</span>')
        if last_type == "KAUF" and buys > 1:
            return ('<span style="background:#dbeafe;color:#2563eb;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">AUFGESTOCKT</span>')
        if last_type == "VERKAUF" and net > 0:
            return ('<span style="background:#fef3c7;color:#d97706;font-size:10px;'
                    'font-weight:700;padding:2px 6px;border-radius:4px;">REDUZIERT</span>')
        return ('<span style="background:#f0f0f0;color:#6e6e73;font-size:10px;'
                'font-weight:700;padding:2px 6px;border-radius:4px;">GEHALTEN</span>')

    th = ('style="padding:0 10px 6px 0;font-size:10px;font-weight:600;color:#9ca3af;'
          'text-align:left;text-transform:uppercase;letter-spacing:0.06em;"')
    td = 'style="padding:5px 10px 5px 0;font-size:11px;vertical-align:top;border-bottom:1px solid #f5f5f7;"'

    # ── 1. Aktienportfolio: 278e Basis + PTR Deltas ───────────────────────────
    if snap_rows:
        # Sortiere nach Wert (größte Position zuerst)
        sorted_snap = sorted(snap_rows,
            key=lambda r: _VALUE_SORT_KEY.get(r[2], 9))

        stock_html = ""
        for asset_name, ticker, value_range, income_type, income_amount in sorted_snap:
            ptr_badge  = _ptr_badge(ticker) if ticker else ""
            val_color  = "#1d1d1f" if "None" not in (value_range or "") else "#9ca3af"
            inc_str    = f"{income_type}: {income_amount}" if income_type and income_amount else ""
            stock_html += (
                f'<tr>'
                f'<td {td} style="padding:5px 10px 5px 0;font-size:11px;vertical-align:top;'
                f'border-bottom:1px solid #f5f5f7;font-weight:600;color:#1d1d1f;">'
                f'{ticker or "–"}</td>'
                f'<td {td} style="color:#6e6e73;">{asset_name[:45]}</td>'
                f'<td {td} style="color:{val_color};white-space:nowrap;">{value_range or "–"}</td>'
                f'<td {td} style="color:#6e6e73;font-size:10px;">{inc_str}</td>'
                f'<td {td}>{ptr_badge}</td>'
                f'</tr>'
            )

        snap_label = f"Jahresend {snap_year} · OGE Form 278e"
        stocks_section = f"""
<p style="margin:12px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">
  Aktienportfolio · {snap_label}
  <span style="font-weight:400;text-transform:none;letter-spacing:0;">
    — sortiert nach Positionsgröße · PTR-Änderungen als Badge
  </span>
</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;">
  <thead><tr>
    <th {th}>Ticker</th><th {th}>Name</th>
    <th {th}>Wert (Range)</th><th {th}>Einkommen</th>
    <th {th}>PTR-Delta</th>
  </tr></thead>
  <tbody>{stock_html}</tbody>
</table>"""

    else:
        # Fallback: nur PTR-Daten (noch kein 278e geparst)
        ptr_only_rows = list(ptr_map.values())
        ptr_html = ""
        for _, asset_name, buys, sells, last_date, history in ptr_only_rows:
            ticker = _
            badge  = _badge_ptr_only(buys, sells, history)
            txinfo = f"{buys}× KAUF" + (f", {sells}× VERKAUF" if sells else "")
            ptr_html += (
                f'<tr>'
                f'<td {td} style="font-weight:600;color:#1d1d1f;">{ticker}</td>'
                f'<td {td} style="color:#6e6e73;">{asset_name[:45]}</td>'
                f'<td {td}>{badge}</td>'
                f'<td {td} style="color:#6e6e73;">{txinfo}</td>'
                f'<td {td} style="color:#6e6e73;white-space:nowrap;">{last_date or "–"}</td>'
                f'</tr>'
            )
        if not ptr_html:
            ptr_html = ('<tr><td colspan="5" style="padding:10px 0;font-size:12px;color:#9ca3af;">'
                        '278e noch nicht geparst — wird beim nächsten Run befüllt.</td></tr>')
        stocks_section = f"""
<p style="margin:12px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">
  Aktienportfolio · OGE Form 278-T PTR (278e noch ausstehend)
</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;">
  <thead><tr>
    <th {th}>Ticker</th><th {th}>Name</th><th {th}>Status</th>
    <th {th}>Transaktionen</th><th {th}>Letzter Eintrag</th>
  </tr></thead>
  <tbody>{ptr_html}</tbody>
</table>"""

    # ── 2. Strategische Beteiligungen ─────────────────────────────────────────
    strategic_section = """
<p style="margin:16px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">Strategische Beteiligungen</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;"><tbody>
  <tr>
    <td style="padding:6px 10px 6px 0;font-size:12px;font-weight:600;color:#1d1d1f;
        border-bottom:1px solid #f5f5f7;vertical-align:top;">DJT</td>
    <td style="padding:6px 10px 6px 0;font-size:11px;color:#6e6e73;
        border-bottom:1px solid #f5f5f7;">Trump Media &amp; Technology Group</td>
    <td style="padding:6px 10px 6px 0;font-size:11px;color:#1d1d1f;
        border-bottom:1px solid #f5f5f7;">~57 % / ~114 Mio. Shares</td>
    <td style="padding:6px 0 6px 0;font-size:11px;color:#6e6e73;
        border-bottom:1px solid #f5f5f7;white-space:nowrap;">2024-09-20<br>
      <a href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000947033&type=4"
         style="font-size:10px;color:#0071e3;text-decoration:none;">SEC Form 4</a>
    </td>
  </tr>
</tbody></table>"""

    # ── 3. Krypto & DeFi ──────────────────────────────────────────────────────
    crypto_rows = ""
    for h in TRUMP_KNOWN_HOLDINGS:
        if h["type"] in ("Aktie (börsennotiert)", "Aktien-Portfolio"):
            continue
        crypto_rows += (
            f'<tr>'
            f'<td style="padding:6px 10px 6px 0;font-size:11px;color:#1d1d1f;'
            f'border-bottom:1px solid #f5f5f7;vertical-align:top;">'
            f'<a href="{h["url"]}" style="color:#0071e3;text-decoration:none;">'
            f'{h["asset"]}</a></td>'
            f'<td style="padding:6px 10px 6px 0;font-size:11px;color:#6e6e73;'
            f'border-bottom:1px solid #f5f5f7;">{h["type"]}</td>'
            f'<td style="padding:6px 10px 6px 0;font-size:11px;color:#1d1d1f;'
            f'border-bottom:1px solid #f5f5f7;">{h["stake"]}</td>'
            f'<td style="padding:6px 0;font-size:11px;color:#6e6e73;'
            f'border-bottom:1px solid #f5f5f7;white-space:nowrap;">{h["disclosed"]}<br>'
            f'<span style="font-size:10px;">{h["source"]}</span></td>'
            f'</tr>'
        )
    crypto_section = f"""
<p style="margin:16px 0 4px;font-size:10px;font-weight:700;color:#6e6e73;
   text-transform:uppercase;letter-spacing:0.08em;">Krypto &amp; DeFi</p>
<table style="border-collapse:collapse;width:100%;margin-top:2px;">
  <thead><tr>
    <th {th}>Asset</th><th {th}>Typ</th>
    <th {th}>Beteiligung</th><th {th}>Offengelegt</th>
  </tr></thead>
  <tbody>{crypto_rows}</tbody>
</table>"""

    return stocks_section + strategic_section + crypto_section


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


def parse_oge_ptr_pdf(pdf_url: str, db_conn=None) -> list[dict]:
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
        _save_oge_holdings(transactions, pdf_url, db_conn=db_conn)
        return transactions
    except Exception as e:
        log.warning(f"  ⚠️  OGE PDF Parse Fehler ({pdf_url}): {e}")
        return []


# Ticker-Mapping für häufige Asset-Namen in OGE-PDFs
_OGE_ASSET_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "amazon": "AMZN", "alphabet": "GOOGL",
    "google": "GOOGL", "meta": "META", "nvidia": "NVDA", "tesla": "TSLA",
    "ibm": "IBM", "intel": "INTC", "amd": "AMD", "boeing": "BA",
    "lockheed": "LMT", "exxon": "XOM", "jpmorgan": "JPM", "goldman": "GS",
    "bank of america": "BAC", "costco": "COST", "walmart": "WMT",
    "pfizer": "PFE", "johnson": "JNJ", "berkshire": "BRK",
    "palantir": "PLTR", "trump media": "DJT",
}

def _ticker_from_asset_name(name: str) -> str | None:
    low = name.lower()
    # Direktes Ticker-Symbol (2-5 Großbuchstaben in Klammern oder am Ende)
    m = re.search(r'\b([A-Z]{2,5})\b', name)
    if m and m.group(1) not in {"THE", "AND", "FOR", "LLC", "INC", "CORP"}:
        return m.group(1)
    for keyword, ticker in _OGE_ASSET_TICKER_MAP.items():
        if keyword in low:
            return ticker
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
    for tx in stock_txs:
        ticker   = tx["_ticker"]
        label, bg, fg = _context_badge(ticker, tx.get("tx_type","?"))
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

    subject = (f"🏛️ Trump PTR: {n_new_buy} NEW BUY · {n_add} ADD · "
               f"{n_sell} SELL · {n_sold_out} SOLD OUT · "
               f"{len(transactions)} Tx gesamt")
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

_278E_EXTRACT_PROMPT = """This is a page from Trump's OGE Form 278e financial disclosure.
Extract EVERY row from the Part 6 table (Other Assets and Income).
Return ONLY a JSON array, no other text. Each element:
{"asset": "full name as written", "value": "value range as written", "income_type": "type or empty", "income_amount": "amount or empty"}
If a row has no value (blank or 'None (or less than $1,001)'), still include it with value as written.
Skip header rows and the 'INVESTMENT ACCOUNT #4' section header.
If the page has no Part 6 table data, return []."""

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
            content.append({"type": "text", "text":
                _278E_EXTRACT_PROMPT +
                "\nReturn ONE combined JSON array for ALL pages shown above."})

            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
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
    send_278e_alert(pdf_url, year)
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


def parse_ptr_via_claude_vision(pdf_url: str, db_conn=None) -> list[dict]:
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
        r = requests.get(pdf_url, headers=OGE_HEADERS, timeout=30)
        r.raise_for_status()
        doc = fitz.open(stream=r.content, filetype="pdf")
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
            content.append({"type": "text", "text":
                _PTR_EXTRACT_PROMPT +
                "\nReturn ONE combined JSON array for ALL pages shown above."})

            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
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
        log.info("Neues 278e PDF: %s…", pdf_url[:80])
        year_m = re.search(r'/(\d{4})/', pdf_url)
        year   = int(year_m.group(1)) if year_m else now_utc().year
        n = parse_278e_via_claude_vision(pdf_url, year)
        conn.execute("INSERT OR IGNORE INTO oge_ptrs VALUES (?,?,?,?)",
                     (pdf_url, source, n, now_utc().isoformat()))
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
            transactions = parse_oge_ptr_pdf(pdf_url, db_conn=thread_conn)
            if not transactions:
                transactions = parse_ptr_via_claude_vision(pdf_url, db_conn=thread_conn)

            thread_conn.execute("INSERT OR IGNORE INTO oge_ptrs VALUES (?,?,?,?)",
                         (pdf_url, source, len(transactions), now_utc().isoformat()))
            thread_conn.commit()

            if not transactions:
                log.warning("OGE PTR: 0 Tx nach Vision — übersprungen: %s", pdf_url)
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
            return {
                "price":   current,
                "chg_1d":  round((current / prev_close - 1) * 100, 2),
                "chg_1w":  round((current / week_ago   - 1) * 100, 2),
                "chg_1m":  round((current / month_ago  - 1) * 100, 2),
            }
        except Exception as e:
            log.warning(f"  ⚠️  Yahoo Finance ({ticker}) attempt {attempt+1}: {e}")
            if attempt < 2:
                import time; time.sleep(2 ** attempt)
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
EVENT_DATE: [YYYY-MM-DD of when the described event actually occurred, NOT the article publish date — if unknown write UNKNOWN]
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
