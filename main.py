import os
import re
import json
import sqlite3
import hashlib
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

# ─────────────────────────────────────────────
# CONFIG (all from GitHub Secrets / env vars)
# ─────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY")
SCRAPE_KEY          = os.getenv("SCRAPE_CREATORS_API_KEY")
GMAIL_EMAIL         = os.getenv("GMAIL_EMAIL")
GMAIL_PASS          = os.getenv("GMAIL_APP_PASSWORD")
RECIPIENT           = os.getenv("RECIPIENT_EMAIL")

TRUMP_TRUTH_ID      = "107780257626128497"
DB_PATH             = "alerts.db"
LOOKBACK_HOURS      = 24  # 24h-Fenster – SQLite-Dedup verhindert Doppel-Alerts
MAX_ALERTS_PER_RUN  = 15  # Schutz vor Kosten-Explosion bei Breaking-News-Wellen
MAX_TICKERS_PER_ARTICLE = 3  # max. Tickers pro Artikel, Priorität: hoch > niedrig

# ─────────────────────────────────────────────
# VALIDATE REQUIRED SECRETS
# ─────────────────────────────────────────────
REQUIRED = {
    "ANTHROPIC_API_KEY":        ANTHROPIC_API_KEY,
    "SCRAPE_CREATORS_API_KEY":  SCRAPE_KEY,
    "GMAIL_EMAIL":              GMAIL_EMAIL,
    "GMAIL_APP_PASSWORD":       GMAIL_PASS,
    "RECIPIENT_EMAIL":          RECIPIENT,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    print(f"❌ Fehlende Secrets: {', '.join(missing)}")
    sys.exit(1)

# ─────────────────────────────────────────────
# SQLite SETUP
# ─────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
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
conn.commit()

# Global Anthropic client (FIX: nicht pro Alert neu instanziieren)
client = Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────────
# FINANCIAL RELEVANCE KEYWORDS (Pre-Filter)
# ─────────────────────────────────────────────
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
    text_lower = text.lower()
    return any(kw in text_lower for kw in FINANCIAL_KEYWORDS)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

CUTOFF: datetime = now_utc() - timedelta(hours=LOOKBACK_HOURS)

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

def already_seen(h: str) -> bool:
    return conn.execute("SELECT 1 FROM events WHERE hash=?", (h,)).fetchone() is not None

def send_gmail(subject: str, html_body: str):
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
    except Exception as e:
        print(f"  ❌ Gmail-Fehler: {e}")

# ─────────────────────────────────────────────
# ENTITY RESOLUTION – alle Tickers pro Post
# ─────────────────────────────────────────────
ENTITIES_FILE = os.path.join(os.path.dirname(__file__), "entities.json")
with open(ENTITIES_FILE, encoding="utf-8") as f:
    ENTITIES: dict = json.load(f)

def find_all_tickers(text: str) -> list[tuple[str, str]]:
    """
    Return ALL matching (ticker, confidence) tuples found in text.
    3-Tier-Matching gegen strukturiertes entities.json:
      - symbol  : case-sensitiv  (Ticker-Kürzel),            immer → "hoch"
      - company : case-insensitiv (Firmenname/CEO/Tochter),   immer → "hoch"
      - weak    : case-insensitiv (Produkt/Brand/Begriff),
                  nur wenn is_financially_relevant(text)      → "niedrig"
    """
    results: list[tuple[str, str]] = []
    text_has_finance = is_financially_relevant(text)

    for ticker, tiers in ENTITIES.items():
        matched = False

        # Tier 1: Symbol – case-sensitiv, immer auslösen
        for alias in tiers.get("symbol", []):
            if re.search(r'\b' + re.escape(alias) + r'\b', text):
                results.append((ticker.upper(), "hoch"))
                matched = True
                break
        if matched:
            continue

        # Tier 2: Firmenname / CEO – case-insensitiv, immer auslösen
        for alias in tiers.get("company", []):
            if re.search(r'\b' + re.escape(alias) + r'\b', text, re.IGNORECASE):
                results.append((ticker.upper(), "hoch"))
                matched = True
                break
        if matched:
            continue

        # Tier 3: Schwache Signale (Produkte/Brands) – nur mit Finanz-Kontext
        if text_has_finance:
            for alias in tiers.get("weak", []):
                if re.search(r'\b' + re.escape(alias) + r'\b', text, re.IGNORECASE):
                    results.append((ticker.upper(), "niedrig"))
                    break

    return results

# ─────────────────────────────────────────────
# DATA SOURCES
# ─────────────────────────────────────────────
def fetch_truth_social() -> list[dict]:
    url = (
        f"https://api.scrapecreators.com/v1/truthsocial/user/posts"
        f"?user_id={TRUMP_TRUTH_ID}&limit=20"
    )
    headers = {"x-api-key": SCRAPE_KEY}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data  = r.json()
        posts = data.get("posts", data.get("data", []))
        print(f"  Truth Social: {len(posts)} Posts abgerufen")
        return posts
    except Exception as e:
        print(f"  ⚠️  Truth Social Fehler: {e}")
        return []

# ─────────────────────────────────────────────
# NACHRICHTEN-RSS (Google News + Finanz-Feeds)
# Kein API-Key, kein Rate-Limit, feedparser läuft bereits.
# ─────────────────────────────────────────────
GNEWS_QUERIES = [
    "Donald Trump stock market",
    "Trump tariff trade company",
    "Trump executive order sanctions",
    "Trump economy policy",
]

FINANCIAL_RSS_FEEDS = [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("CNBC Markets",     "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Yahoo Finance",    "https://finance.yahoo.com/rss/topstories"),
    ("AP Business",      "https://feeds.apnews.com/apnews/businessnews"),
]

def _rss_entry_to_dict(entry, source: str) -> dict:
    """Normalisiert einen feedparser-Eintrag auf das Standard-Artikel-Format."""
    return {
        "title":       entry.get("title", ""),
        "description": entry.get("summary", entry.get("description", "")),
        "publishedAt": entry.get("published", ""),
        "url":         entry.get("link", ""),
        "_source":     source,
    }

def fetch_gnews_rss() -> list[dict]:
    """Google News RSS – kostenlos, kein API-Key, kein Rate-Limit."""
    results = []
    for q in GNEWS_QUERIES:
        url = (
            "https://news.google.com/rss/search"
            f"?q={requests.utils.quote(q)}&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            feed     = feedparser.parse(url)
            articles = [_rss_entry_to_dict(e, "Google News") for e in feed.entries[:15]]
            results.extend(articles)
            print(f"  GNews [{q[:35]}]: {len(articles)} Artikel")
        except Exception as ex:
            print(f"  ⚠️  GNews RSS Fehler ({q[:30]}): {ex}")
    return results

def fetch_financial_rss() -> list[dict]:
    """Finanz-RSS-Feeds – Reuters, CNBC, MarketWatch, Yahoo Finance, AP."""
    results = []
    for name, url in FINANCIAL_RSS_FEEDS:
        try:
            feed     = feedparser.parse(url)
            articles = [_rss_entry_to_dict(e, name) for e in feed.entries[:20]]
            results.extend(articles)
            print(f"  {name}: {len(articles)} Artikel")
        except Exception as ex:
            print(f"  ⚠️  {name} RSS Fehler: {ex}")
    return results

def fetch_whitehouse() -> list:
    try:
        feed    = feedparser.parse("https://www.whitehouse.gov/feed/")
        entries = feed.entries[:30]
        print(f"  White House RSS: {len(entries)} Einträge abgerufen")
        return entries
    except Exception as e:
        print(f"  ⚠️  White House RSS Fehler: {e}")
        return []

# ─────────────────────────────────────────────
# TRUMP HOLDINGS (OGE Form 278 – öffentlich)
# ─────────────────────────────────────────────
TRUMP_HOLDINGS = {
    "DJT":  "JA – Trump hält ~57% an Trump Media & Technology Group (DJT), Quelle: SEC Form 4 / OGE 2024",
}
def trump_holding_info(ticker: str) -> str:
    return TRUMP_HOLDINGS.get(ticker.upper(),
        "Nicht aus öffentlichen OGE-Filings (Form 278) bekannt – keine Annahmen.")

# ─────────────────────────────────────────────
# YAHOO FINANCE – MARKTDATEN
# ─────────────────────────────────────────────
# Sonderfälle: Crypto-Ticker brauchen Yahoo-Finance-Suffix
YF_TICKER_MAP = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "BRK": "BRK-B",
    "GOOGL": "GOOGL", "GOOG": "GOOG",
}

@lru_cache(maxsize=512)
def fetch_market_data(ticker: str) -> dict:
    """Holt Kursdaten von Yahoo Finance. Gibt leeres Dict zurück bei Fehler."""
    yf_sym = YF_TICKER_MAP.get(ticker.upper(), ticker.upper())
    try:
        hist = yf.Ticker(yf_sym).history(period="1mo", auto_adjust=True)
        if hist.empty or len(hist) < 2:
            return {}
        close      = hist["Close"]
        current    = round(float(close.iloc[-1]), 2)
        prev_close = round(float(close.iloc[-2]), 2)
        week_ago   = round(float(close.iloc[-6]) if len(close) >= 6 else float(close.iloc[0]), 2)
        month_ago  = round(float(close.iloc[0]), 2)
        chg_1d = round((current / prev_close - 1) * 100, 2)
        chg_1w = round((current / week_ago   - 1) * 100, 2)
        chg_1m = round((current / month_ago  - 1) * 100, 2)
        return {
            "price":     current,
            "chg_1d":   chg_1d,
            "chg_1w":   chg_1w,
            "chg_1m":   chg_1m,
        }
    except Exception as e:
        print(f"  ⚠️  Yahoo Finance ({ticker}): {e}")
        return {}

def format_market_block(ticker: str) -> str:
    """Formatiert Marktdaten als lesbaren Block für den Prompt."""
    d = fetch_market_data(ticker)
    if not d:
        return "Marktdaten: nicht verfügbar"
    def arrow(v): return "▲" if v >= 0 else "▼"
    return (
        f"Letzter Schlusskurs:           {d['price']} USD\n"
        f"Ggü. vorigem Handelstag:       {arrow(d['chg_1d'])} {d['chg_1d']:+.2f}%\n"
        f"5 Handelstage:                 {arrow(d['chg_1w'])} {d['chg_1w']:+.2f}%\n"
        f"1 Monat:                       {arrow(d['chg_1m'])} {d['chg_1m']:+.2f}%"
    )

# ─────────────────────────────────────────────
# LLM ANALYSIS + ALERT
# ─────────────────────────────────────────────
def analyze_and_alert(source: str, published, raw_text: str, ticker: str, url: str,
                      confidence: str = "hoch"):
    market_block  = format_market_block(ticker)
    holding_info  = trump_holding_info(ticker)

    if confidence == "niedrig":
        confidence_block = (
            f"NIEDRIG – {ticker} wurde nur über ein indirektes Stichwort (Produkt/Brand) gefunden, "
            f"nicht über Ticker-Symbol oder Firmenname. Prüfe im Text, ob {ticker} wirklich gemeint ist."
        )
    else:
        confidence_block = "HOCH – Ticker-Symbol oder Firmenname direkt im Text gefunden."

    prompt = f"""Du bist ein präziser Finanz-Analyst. Analysiere folgenden Trump-bezogenen Text.
Dir werden echte Marktdaten und verifizierte OGE-Informationen geliefert – nutze NUR diese, keine Annahmen.

── TEXT ──────────────────────────────────────
{raw_text}

── MARKTDATEN ({ticker}, Yahoo Finance, aktuell) ──
{market_block}

── TRUMP-BETEILIGUNG (OGE Form 278, öffentlich) ──
{holding_info}

── ERKENNUNGS-KONFIDENZ ──
{confidence_block}

Antworte NUR mit folgendem Format (keine Einleitung, keine Ergänzungen):

Unternehmen: [Firmenname] ({ticker})
Quelle: {source} – [max. 1 Zeile Zusammenfassung des Textes]
Sentiment: [positiv / negativ / neutral – begründet aus dem Text]
Marktreaktion: [Hat der Kurs schon reagiert? Einschätzung basierend auf den Tagesdaten]
Trump-Beteiligung: {holding_info}
Zusammenfassung: [max. 2 Sätze – nur aus Textinhalt ableitbar, keine Spekulation]
Trade-Richtung: [LONG / SHORT / UNKLAR – Begründung aus Text UND Kurslage]
Konfidenz: [hoch / mittel / niedrig – kombiniert aus Erkennungs-Konfidenz ({confidence}) UND Textqualität]"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        alert_text = response.content[0].text
    except Exception as e:
        print(f"  ❌ Claude-API Fehler: {e}")
        return

    event_id = hashlib.sha256((ticker + raw_text).encode("utf-8")).hexdigest()
    h        = get_hash(raw_text + ticker)
    try:
        conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
            (event_id, source, str(published), raw_text, h, ticker, now_utc().isoformat()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass

    conf_badge = (
        '<span style="background:#e67e22;color:white;padding:2px 6px;border-radius:4px;font-size:11px;">'
        '⚠️ Erkennungs-Konfidenz: niedrig</span><br><br>'
        if confidence == "niedrig" else ""
    )
    html = f"""
<html><body style="font-family:monospace;font-size:14px;">
<h2 style="color:#c0392b;">🚨 Trump-Impact Alert – {ticker}</h2>
{conf_badge}<pre style="background:#f4f4f4;padding:12px;border-radius:6px;">{alert_text}</pre>
<hr>
<h3 style="color:#2c3e50;">📊 Marktdaten ({ticker})</h3>
<pre style="background:#eaf4fb;padding:10px;border-radius:6px;">{market_block}</pre>
<hr>
<p><b>Quelle:</b> {source}<br>
<b>Veröffentlicht:</b> {published}<br>
<b>Original:</b> <a href="{url}">{url}</a></p>
<p style="color:#888;font-size:11px;">
Generiert {now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')} · Zeitfenster: letzte {LOOKBACK_HOURS}h
</p>
</body></html>
"""
    conf_tag = " ⚠️ [niedrig]" if confidence == "niedrig" else ""
    subject = f"🚨 Trump-Impact Alert – {ticker}{conf_tag} – Trade Candidate"
    send_gmail(subject, html)
    print(f"  🎯 Alert: {ticker} via {source}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"  Trump-Impact Monitor – {now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Zeitfenster: alles ab {CUTOFF.strftime('%Y-%m-%d %H:%M:%S UTC')} (letzte {LOOKBACK_HOURS}h)")
    print(f"{'='*60}\n")

    processed = 0

    def _cap_reached() -> bool:
        if processed >= MAX_ALERTS_PER_RUN:
            print(f"  ⚠️  Alert-Cap ({MAX_ALERTS_PER_RUN}) erreicht – Rest übersprungen.")
            return True
        return False

    def _sorted_tickers(tickers):
        high = [(t, c) for t, c in tickers if c == "hoch"]
        low  = [(t, c) for t, c in tickers if c != "hoch"]
        return (high + low)[:MAX_TICKERS_PER_ARTICLE]

    # ── Truth Social ──────────────────────────────────────────────────
    print("📡 Truth Social …")
    for post in fetch_truth_social():
        if _cap_reached():
            break
        text = post.get("text", post.get("content", ""))
        if not text:
            continue
        ts = post.get("created_at", post.get("published"))
        if not is_recent(ts):
            continue
        if not is_financially_relevant(text):
            continue
        tickers = find_all_tickers(text)
        if not tickers:
            continue
        url = post.get("url", post.get("uri", "https://truthsocial.com/@realDonaldTrump"))
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            h = get_hash(text + ticker)
            if already_seen(h):
                continue
            analyze_and_alert("Truth Social", ts, text, ticker, url, confidence)
            processed += 1

    # ── Nachrichten-RSS (Google News + Finanz-Feeds) ──────────────────
    print("\n📰 Nachrichten-RSS …")
    for article in fetch_gnews_rss() + fetch_financial_rss():
        if _cap_reached():
            break
        text = ((article.get("title") or "") + " " + (article.get("description") or "")).strip()
        if not text:
            continue
        if not is_recent(article.get("publishedAt")):
            continue
        if not is_financially_relevant(text):
            continue
        tickers = find_all_tickers(text)
        if not tickers:
            continue
        for ticker, confidence in _sorted_tickers(tickers):
            if _cap_reached():
                break
            h = get_hash(text + ticker)
            if already_seen(h):
                continue
            analyze_and_alert(article.get("_source", "RSS"), article.get("publishedAt", ""), text, ticker, article.get("url", ""), confidence)
            processed += 1

    # ── White House RSS ────────────────────────────────────────────────
    print("\n🏛️  White House RSS …")
    for entry in fetch_whitehouse():
        if _cap_reached():
            break
        text = (entry.get("title", "") + " " + entry.get("summary", "")).strip()
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
            h = get_hash(text + ticker)
            if already_seen(h):
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

    print(f"\n{'='*60}")
    print(f"  ✅ Durchlauf beendet – {processed} Alert(s) verarbeitet")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
