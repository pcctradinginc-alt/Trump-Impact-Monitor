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
import smtplib
import sys
import time

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
LOOKBACK_HOURS       = 24
MAX_ALERTS_PER_RUN   = 10   # Schutz vor Kosten-Explosion bei Breaking-News-Wellen
MAX_TICKERS_PER_ART  = 3    # max. Tickers pro Artikel (hoch vor niedrig)
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
    print(f"❌ Fehlende Secrets: {', '.join(missing)}")
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
            print("  🧠 FinBERT wird geladen …")
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
        print(f"  ⚠️  FinBERT Fehler: {e}")
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
    "tariff", "tariffs", "sanction", "sanctions", "trade deal", "trade war",
    "invest", "investment", "stock", "shares", "market", "deal", "contract",
    "merger", "acquisition", "ban", "subsidy", "tax", "fine", "penalty",
    "regulation", "import", "export", "manufacturer", "factory", "production",
    "revenue", "profit", "earnings", "ipo", "billions", "millions", "trillion",
    "economy", "economic", "federal reserve", "interest rate", "inflation",
    "oil", "energy", "chip", "semiconductor", "defense", "military contract",
    "crypto", "bitcoin", "deregulation", "privatize", "nationalize",
    "price", "cost", "supply chain", "jobs", "layoff", "hire",
}

def is_financially_relevant(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in FINANCIAL_KEYWORDS)

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
        print(f"  ⚠️  Zeitstempel nicht parsebar ({ts!r}): {e} → übersprungen")
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
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_EMAIL, GMAIL_PASS)
            server.sendmail(GMAIL_EMAIL, RECIPIENT, msg.as_string())
        print(f"  ✅ E-Mail gesendet: {subject}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("  ❌ Gmail: Authentifizierung fehlgeschlagen – App-Passwort prüfen")
    except smtplib.SMTPException as e:
        print(f"  ❌ Gmail SMTP-Fehler: {e}")
    except Exception as e:
        print(f"  ❌ Gmail unbekannter Fehler: {e}")
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
def fetch_truth_social() -> list[dict]:
    url = (
        f"https://api.scrapecreators.com/v1/truthsocial/user/posts"
        f"?user_id={TRUMP_TRUTH_ID}&limit=20"
    )
    headers = {"x-api-key": SCRAPE_KEY}
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            posts = r.json().get("posts", r.json().get("data", []))
            print(f"  Truth Social: {len(posts)} Posts abgerufen")
            return posts
        except requests.exceptions.Timeout:
            print(f"  ⚠️  Truth Social: Timeout (Versuch {attempt}/3)")
        except requests.exceptions.HTTPError as e:
            print(f"  ⚠️  Truth Social HTTP-Fehler: {e} (Versuch {attempt}/3)")
            if e.response is not None and e.response.status_code < 500:
                break  # 4xx nicht wiederholen
        except Exception as e:
            print(f"  ⚠️  Truth Social Fehler: {e} (Versuch {attempt}/3)")
        if attempt < 3:
            time.sleep(2 ** attempt)  # 2s, 4s
    return []

FINANCIAL_RSS_FEEDS = [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("CNBC Markets",     "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Yahoo Finance",    "https://finance.yahoo.com/rss/topstories"),
    ("AP Business",      "https://feeds.apnews.com/apnews/businessnews"),
    # Trump-spezifische Feeds
    ("Google News Trump", "https://news.google.com/rss/search?q=trump+tariff+trade&hl=en-US&gl=US&ceid=US:en"),
    ("Google News Trump Markets", "https://news.google.com/rss/search?q=trump+stock+market+executive+order&hl=en-US&gl=US&ceid=US:en"),
    ("Politico Economy",  "https://rss.politico.com/economy.xml"),
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
            print(f"  {name}: {len(arts)} Artikel")
        except Exception as ex:
            print(f"  ⚠️  {name} Fehler: {ex}")
    return results

def fetch_whitehouse() -> list:
    try:
        feed    = feedparser.parse("https://www.whitehouse.gov/feed/")
        entries = feed.entries[:30]
        print(f"  White House RSS: {len(entries)} Einträge")
        return entries
    except Exception as e:
        print(f"  ⚠️  White House RSS Fehler: {e}")
        return []

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
        print(f"  ⚠️  Yahoo Finance ({ticker}): {e}")
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
# TURBO-ZERTIFIKAT-EMPFEHLUNG
# ─────────────────────────────────────────────────────────────────────────────
def parse_trade_direction(alert_text: str) -> str:
    """Extrahiert LONG / SHORT / UNKLAR aus dem Claude-Output."""
    for line in alert_text.splitlines():
        if "trade-richtung:" in line.lower():
            upper = line.upper()
            if "LONG"  in upper: return "LONG"
            if "SHORT" in upper: return "SHORT"
    return "UNKLAR"

def turbo_recommendation(ticker: str, direction: str) -> str:
    """
    Gibt eine skalierbar handelbare Turbo-Empfehlung aus.
    Kriterien: KO-Abstand > 12%, Spread < 0.5% (muss live geprüft werden).
    """
    if direction == "UNKLAR":
        return "⛔ Keine Empfehlung – Trade-Richtung unklar"
    data = fetch_market_data(ticker)
    if not data:
        return "⛔ Keine Empfehlung – Marktdaten nicht verfügbar"

    price = data["price"]

    if direction == "LONG":
        ko      = round(price * 0.88, 2)          # 12 % unterhalb
        lever   = round(price / (price - ko), 1)
        return (
            f"📈 LONG-Turbo auf {ticker}\n"
            f"   Aktueller Kurs:   {price:.2f} USD\n"
            f"   Empf. KO-Level:  ≤ {ko:.2f} USD  (>12 % Abstand)\n"
            f"   Hebel (approx):  ~{lever}x\n"
            f"   ⚠️  Spread vor Kauf prüfen: < 0.5 % erforderlich"
        )
    else:   # SHORT
        ko      = round(price * 1.12, 2)          # 12 % oberhalb
        lever   = round(price / (ko - price), 1)
        return (
            f"📉 SHORT-Turbo auf {ticker}\n"
            f"   Aktueller Kurs:   {price:.2f} USD\n"
            f"   Empf. KO-Level:  ≥ {ko:.2f} USD  (>12 % Abstand)\n"
            f"   Hebel (approx):  ~{lever}x\n"
            f"   ⚠️  Spread vor Kauf prüfen: < 0.5 % erforderlich"
        )

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
            model=MODEL,
            max_tokens=30,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip().upper()
        if raw == "NONE" or not raw:
            return []
        tickers = [t.strip() for t in raw.split(",") if re.match(r'^[A-Z]{1,5}$', t.strip())]
        if tickers:
            print(f"  🔍 Claude Sektor-Erkennung: {tickers}")
        return [(t, "claude") for t in tickers[:3]]
    except Exception as e:
        print(f"  ⚠️  Sektor-Erkennung Fehler: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# HAUPTANALYSE  –  LLM + Alert + E-Mail
# ─────────────────────────────────────────────────────────────────────────────
def _quick_relevance_check(ticker: str, text: str) -> bool:
    """Haiku-Pre-Check (~$0.0001) — vor teuren FinBERT/yfinance/Sonnet-Calls."""
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            temperature=0,
            messages=[{"role": "user", "content":
                f"Is {ticker} directly and concretely mentioned or clearly affected "
                f"in this text? Answer only YES or NO:\n\n{text[:600]}"}],
        )
        return "YES" in resp.content[0].text.upper()
    except Exception as e:
        print(f"  ⚠️  Pre-Check Fehler ({ticker}): {e}")
        return True  # im Zweifel durchlassen


def analyze_and_alert(
    source:     str,
    published,
    raw_text:   str,
    ticker:     str,
    url:        str,
    confidence: str = "hoch",
):
    # ── Haiku-Pre-Check (nur bei Claude-Inferenz, wo Relevanz unsicher) ──────
    if confidence == "claude" and not _quick_relevance_check(ticker, raw_text):
        print(f"  ⏭️  {ticker} Haiku-Pre-Check NEIN → übersprungen")
        return

    # ── Teure Calls erst nach Pre-Check ──────────────────────────────────────
    market_data  = fetch_market_data(ticker)
    market_block = format_market_block(ticker)
    holding_info = trump_holding_info(ticker)
    finbert_sent = get_finbert_sentiment(raw_text)

    # Konfidenz-Beschreibung für Prompt
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

    price    = market_data.get("price", 0)
    chg_1d   = market_data.get("chg_1d", 0)
    stop_long  = round(price * 0.92, 2) if price else 0
    stop_short = round(price * 1.08, 2) if price else 0

    prompt = f"""You are a quantitative political-risk analyst specializing in Trump-driven market dislocations. Be precise, factual, and calibrated. Never speculate beyond what the source text directly supports.

SOURCE TEXT:
{raw_text}

SOURCE: {source} | PUBLISHED: {published}

MARKET DATA ({ticker}):
{market_block}

DETECTION CONFIDENCE: {conf_desc}
TRUMP FINANCIAL INTEREST: {holding_info}

COMPARABLE PRECEDENTS (use for magnitude calibration):
- Trump tariff tweet on steel (Mar 2018): NUE +8%, X +6% intraday
- Trump Truth Post attacking Amazon (Apr 2018): AMZN -5% within 2h
- Trump executive order on TikTok (Aug 2020): SNAP +8%, META +2%
- Trump China chip export ban (Oct 2022): NVDA -15% over 3 days

Respond ONLY in this exact format. No preamble. No markdown.

RELEVANCE: [YES / NO] — {ticker} is [directly named / sector-affected / tangentially mentioned]
COMPANY: [Full legal name] ({ticker})
EVENT_SUMMARY: [One sentence: what Trump said/did, stripped of spin]
DIRECT_MENTION: [YES / NO] — ticker or company name explicitly in text
SENTIMENT: [BULLISH / BEARISH / NEUTRAL] for {ticker}
SENTIMENT_BASIS: [Quote or paraphrase from text that drives sentiment — max 15 words]
FINBERT_ALIGNMENT: [AGREES / DISAGREES / PARTIAL] with machine reading of "{finbert_sent}"
PRICE_ALREADY_REACTED: [YES ({chg_1d:+.1f}% today) / NO / UNCLEAR]
MAGNITUDE_ESTIMATE: [SMALL <3% / MEDIUM 3-10% / LARGE >10%] intraday — [one-sentence rationale]
TIME_TO_IMPACT: [IMMEDIATE pre/intraday / SHORT 1-5 days / MEDIUM 1-4 weeks / UNCLEAR]
TRUMP_CONFLICT_OF_INTEREST: [YES / NO / UNKNOWN]
SUMMARY: [Max 2 sentences. Only facts from source text. Zero speculation.]
TRADE_DIRECTION: [LONG / SHORT / NO_TRADE]
TRADE_RATIONALE: [Text evidence + current price level ({price:.2f}) in one sentence]
STOP_LEVEL: [LONG stop: {stop_long:.2f} (−8%) / SHORT stop: {stop_short:.2f} (+8%) / N/A]
CONFIDENCE_SCORE: [HIGH / MEDIUM / LOW] — [limiting factor in max 5 words]"""

    # ── Claude-Aufruf (Sonnet) ────────────────────────────────────────────────
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=900,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        alert_text = response.content[0].text.strip()
    except Exception as e:
        print(f"  ❌ Claude-API Fehler ({ticker}): {e}")
        return

    # ── Relevanz-Gate ────────────────────────────────────────────────────────
    first_line = alert_text.splitlines()[0].upper()
    if "RELEVANCE:" in first_line and "NO" in first_line:
        print(f"  ⏭️  {ticker} übersprungen – kein konkreter Unternehmensbezug")
        return

    # ── Trade-Richtung aus neuem Format ──────────────────────────────────────
    direction = "UNKLAR"
    for line in alert_text.splitlines():
        if line.upper().startswith("TRADE_DIRECTION:"):
            if "LONG"     in line.upper(): direction = "LONG"
            elif "SHORT"  in line.upper(): direction = "SHORT"
            elif "NO_TRADE" in line.upper(): direction = "NO_TRADE"
            break

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
        print(f"  ⏭️  {ticker} bereits in DB – kein doppelter Alert")
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
           line-height:1.6;color:#1d1d1f;white-space:pre-wrap;">{turbo_block}</pre>
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
    dir_emoji = {"LONG": "📈", "SHORT": "📉", "NO_TRADE": "⛔"}.get(direction, "❓")
    conf_tag  = {"niedrig": " ⚠️", "claude": " 🤖"}.get(confidence, "")
    subject   = f"{dir_emoji} Trump-Impact – {ticker}{conf_tag} [{direction}] – {source}"
    send_gmail(subject, html_body)
    print(f"  🎯 Alert gesendet: {ticker} | {direction} | {source} | FinBERT: {finbert_sent}")

# ─────────────────────────────────────────────────────────────────────────────
# BACKTESTING  –  Alert-Outcome nach 24h / 7d nachfüllen
# ─────────────────────────────────────────────────────────────────────────────
def record_outcomes():
    """
    Lädt alle Events ohne vollständige Outcome-Daten und füllt
    price_24h / price_7d nach, sobald genug Zeit vergangen ist.
    Ermöglicht spätere Trefferquoten-Analyse per SQL.
    """
    rows = conn.execute("""
        SELECT e.event_id, e.ticker, e.processed_at,
               o.price_alert, o.price_24h, o.price_7d
        FROM events e
        LEFT JOIN outcomes o ON e.event_id = o.event_id
        WHERE o.event_id IS NULL
           OR (o.price_24h IS NULL AND e.processed_at < datetime('now', '-25 hours'))
           OR (o.price_7d  IS NULL AND e.processed_at < datetime('now', '-8 days'))
        LIMIT 20
    """).fetchall()

    if not rows:
        return

    print(f"\n📊 Backtesting: {len(rows)} Outcomes zu aktualisieren …")
    for event_id, ticker, processed_at, price_alert, price_24h, price_7d in rows:
        data = fetch_market_data.__wrapped__(ticker)  # Cache umgehen für aktuelle Daten
        if not data:
            continue
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
    print("  ✅ Outcomes aktualisiert")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global CUTOFF
    CUTOFF = now_utc() - timedelta(hours=LOOKBACK_HOURS)

    print(f"\n{'═'*62}")
    print(f"  Trump-Impact Monitor  –  {now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Zeitfenster: ab {CUTOFF.strftime('%Y-%m-%d %H:%M UTC')}  (letzte {LOOKBACK_HOURS}h)")
    print(f"  Modell: {MODEL}")
    print(f"{'═'*62}\n")

    processed  = 0
    seen_urls: set[str] = set()

    def _cap_reached() -> bool:
        if processed >= MAX_ALERTS_PER_RUN:
            print(f"  ⚠️  Alert-Cap ({MAX_ALERTS_PER_RUN}) erreicht – verbleibende Artikel übersprungen.")
            return True
        return False

    def _sorted_tickers(tickers: list) -> list:
        """Sortiert hoch vor niedrig/claude, begrenzt auf MAX_TICKERS_PER_ART."""
        high = [(t, c) for t, c in tickers if c == "hoch"]
        rest = [(t, c) for t, c in tickers if c != "hoch"]
        return (high + rest)[:MAX_TICKERS_PER_ART]

    # ── Truth Social ──────────────────────────────────────────────────────────
    print("📡 Truth Social …")
    for post in fetch_truth_social():
        if _cap_reached():
            break
        text = clean_text(post.get("text", post.get("content", "")))
        if not text:
            continue
        ts = post.get("created_at", post.get("published"))
        if not is_recent(ts):
            continue
        if not is_financially_relevant(text):
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
    print("\n📰 Nachrichten-RSS …")
    for article in fetch_financial_rss():
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
    print("\n🏛️  White House RSS …")
    for entry in fetch_whitehouse():
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

    record_outcomes()

    print(f"\n{'═'*62}")
    print(f"  ✅ Durchlauf beendet – {processed} Alert(s) verarbeitet")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
