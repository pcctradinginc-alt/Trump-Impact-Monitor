"""
turbo_selector.py — "Trump Post → Turbo Selector DE"

Analysis-only pipeline: Trump post → market signal → market confirmation
(is it already priced in / contradicted?) → concrete, currently tradable
German turbo certificates (onvista finder, all issuers + Vontobel) → risk
(spread, leverage, KO distance, P(KO) via bootstrap Monte-Carlo, financing
cost) → one recommended product or NO TRADE.

NEVER places or suggests order execution and has no broker integration.
Prefers NO TRADE over forcing a bad product — see select_turbo().

Kept as its own module (main.py is ~4600 lines already) and imported by
main.py's analyze_and_alert(). Deliberately does NOT import main.py (would
create a circular import); shared lookups (entities.json, onvista helpers,
yfinance/curl_cffi fallback pattern) are duplicated in a small, self
contained form instead.

Data sources (all free, no API key):
  - onvista derivatives finder (api.onvista.de/api/v1/derivatives/finder/
    configuration_query) — aggregates SG, Goldman, JPM, BNP, HSBC, Morgan
    Stanley, UBS, UniCredit, Vontobel, ... behind ONE public endpoint. This
    is the primary and, in practice, only source with a real bid/ask/spread
    (verified live 2026-09-22).
  - Vontobel's own product-search API as an *additional* issuer source (used
    to widen the candidate pool). Its free endpoint does not return an ask
    price (verified: /products/{isin} detail route 404s, /products/search
    only carries a bid). Candidates from this source will therefore usually
    fail the ask>0 hard filter — kept anyway per spec, documented limitation
    below and in the README.
  - Boerse Stuttgart / Boerse Frankfurt: time-boxed check (~15 min) found no
    workable free keyless JSON — Boerse Frankfurt's REST endpoints reject
    plain GETs (405/308, need session/referrer plumbing not worth building
    for a redundant data source), Boerse Stuttgart's product-finder page
    returns 403 to a non-browser client. Not integrated; onvista alone
    already aggregates enough issuers to have real comparables.
  - yfinance, with the same curl_cffi Yahoo-chart-JSON fallback pattern
    main.py already uses for fetch_market_data(), for the underlying's daily
    close history (~1y) needed for market confirmation and the Monte-Carlo.
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import math
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone

import numpy as np
import requests
import yfinance as yf

try:
    from curl_cffi import requests as cffi_requests
except ImportError:  # pragma: no cover — always available in prod (main.py needs it too)
    cffi_requests = None

from config import TURBO_SELECTOR_CFG as CFG

log = logging.getLogger("turbo_selector")

FEED_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_ENTITIES_FILE = os.path.join(os.path.dirname(__file__), "entities.json")
try:
    with open(_ENTITIES_FILE, encoding="utf-8") as _f:
        ENTITIES: dict = json.load(_f)
except Exception:  # pragma: no cover
    ENTITIES = {}

# yfinance-Symbol-Mapping für Sonderfälle (analog main.py YF_TICKER_MAP)
YF_TICKER_MAP = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "BRK": "BRK-B",
}

YAHOO_CHART_HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")

# ─────────────────────────────────────────────────────────────────────────────
# Bevorzugte Makro-Basiswerte (indices/FX/commodities) — onvista Entity IDs
# live verifiziert am 2026-09-22 über api.onvista.de/api/v1/instruments/query
# ─────────────────────────────────────────────────────────────────────────────
MACRO_UNDERLYINGS = {
    "DAX":    {"yf": "^GDAXI",   "onvista": ("INDEX", "20735"),          "label": "DAX"},
    "SPX":    {"yf": "^GSPC",    "onvista": ("INDEX", "4359526"),        "label": "S&P 500"},
    "NDX":    {"yf": "^NDX",     "onvista": ("INDEX", "325104"),         "label": "Nasdaq 100"},
    "EURUSD": {"yf": "EURUSD=X", "onvista": ("CURRENCY", "EURUSD"),      "label": "EUR/USD"},
    "GOLD":   {"yf": "GC=F",     "onvista": ("PRECIOUS_METAL", "1326189"), "label": "Gold"},
    "BRENT":  {"yf": "BZ=F",     "onvista": ("COMMODITY", "7743988"),    "label": "Brent"},
    "WTI":    {"yf": "CL=F",     "onvista": ("COMMODITY", "15534884"),   "label": "WTI"},
}


# ─────────────────────────────────────────────────────────────────────────────
# DATACLASSES
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class MarketSignal:
    underlying: str            # z.B. "NVDA" oder "NDX" (Ticker/Makro-Kürzel)
    yf_symbol: str              # yfinance-Symbol
    direction: str               # LONG / SHORT / NEUTRAL
    confidence: float             # 0..1  (aus SIGNAL_CONFIDENCE_PCT / 100)
    expected_return: float          # signiert, z.B. -0.03 für -3%
    horizon_days: int
    rationale: str
    uncertainty: str                  # LOW / MEDIUM / HIGH
    volatility: float | None = None    # annualisiert, wird bei Bedarf aus Historie nachgefüllt
    is_macro: bool = False
    macro_label: str | None = None


@dataclass
class Product:
    wkn: str
    isin: str
    issuer: str
    underlying: str
    long_short: str              # LONG / SHORT
    bid: float | None
    ask: float | None
    spread_pct: float | None       # None → wird aus bid/ask berechnet wo möglich
    knockout_barrier: float | None
    financing_level: float | None    # Strike; bei Turbos oft == KO oder mit Puffer
    ratio: float | None                # Bezugsverhältnis (coverRatio)
    leverage: float | None
    quote_timestamp: datetime | None
    currency: str
    underlying_currency: str
    url: str = ""

    def effective_spread_pct(self) -> float | None:
        if self.spread_pct is not None:
            return self.spread_pct
        if self.bid and self.ask:
            return (self.ask - self.bid) / self.ask * 100.0
        return None


@dataclass
class ProductMetrics:
    product: Product
    p_ko: float
    expected_net_return: float
    conservative_expected_return: float
    p5_return: float           # 5%-Quantil (config: conservative_ci_percentile) der
                                # Basissimulation — reines Risiko-Reporting, siehe
                                # conservative_expected_return()-Docstring für den Grund,
                                # warum es NICHT in die conservative_expected_return-
                                # Gate-Entscheidung einfließt
    ko_distance_pct: float
    ko_distance_in_vol: float
    financing_cost_frac: float
    score: float


@dataclass
class SelectionResult:
    decision: str                # ACTIONABLE / WATCH / NO_TRADE
    reason: str
    signal: MarketSignal | None
    selected: ProductMetrics | None = None
    median_comparable_score: float | None = None
    best_alternative: ProductMetrics | None = None
    n_fetched: int = 0
    n_filtered: int = 0
    text: str = ""
    html: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# MARKTDATEN  –  Tageshistorie (~1J) via yfinance + curl_cffi-Fallback,
# pro Prozesslauf gecacht (analog main.py fetch_market_data)
# ─────────────────────────────────────────────────────────────────────────────
_CLOSES_CACHE: dict[str, list[tuple[datetime, float]]] = {}


def _yf_symbol(underlying: str) -> str:
    u = underlying.upper()
    if u in MACRO_UNDERLYINGS:
        return MACRO_UNDERLYINGS[u]["yf"]
    return YF_TICKER_MAP.get(u, u)


def _fetch_yahoo_chart_1y(yf_sym: str) -> list[tuple[datetime, float]]:
    if cffi_requests is None:
        return []
    for host in YAHOO_CHART_HOSTS:
        try:
            r = cffi_requests.get(
                f"https://{host}/v8/finance/chart/{yf_sym}",
                params={"interval": "1d", "range": "1y"},
                impersonate="chrome", timeout=15,
            )
            if r.status_code == 429:
                continue
            r.raise_for_status()
            result_list = r.json().get("chart", {}).get("result") or []
            if not result_list:
                continue
            result = result_list[0]
            ts = result.get("timestamp") or []
            closes = (result.get("indicators", {}).get("quote", [{}])[0]).get("close") or []
            out = [
                (datetime.fromtimestamp(t, tz=timezone.utc), float(c))
                for t, c in zip(ts, closes) if c is not None
            ]
            if out:
                return out
        except Exception as e:
            log.warning(f"  ⚠️  Yahoo Chart-API ({host}) 1y-Fallback fehlgeschlagen: {e}")
    return []


def fetch_daily_closes(yf_sym: str) -> list[tuple[datetime, float]]:
    """~1 Jahr Tagesschlusskurse (Datum, Close), aufsteigend sortiert. Gecacht
    pro Prozesslauf (main.py läuft alle 10 Min als frischer Prozess neu)."""
    if yf_sym in _CLOSES_CACHE:
        return _CLOSES_CACHE[yf_sym]
    closes: list[tuple[datetime, float]] = []
    try:
        hist = yf.Ticker(yf_sym).history(period="1y", auto_adjust=True, timeout=15)
        if not hist.empty:
            closes = [
                (idx.to_pydatetime().replace(tzinfo=timezone.utc) if idx.tzinfo is None
                 else idx.to_pydatetime(), float(c))
                for idx, c in zip(hist.index, hist["Close"]) if c == c  # NaN check
            ]
    except Exception as e:
        log.warning(f"  ⚠️  yfinance 1y-History ({yf_sym}) fehlgeschlagen: {e}")
    if len(closes) < 30:
        fallback = _fetch_yahoo_chart_1y(yf_sym)
        if len(fallback) > len(closes):
            closes = fallback
    _CLOSES_CACHE[yf_sym] = closes
    return closes


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 2 — MARKTBESTÄTIGUNG
# NO TRADE wenn (a) bereits eingepreist: Reaktion seit Post-Zeitpunkt in
# Signal-Richtung >= priced_in_fraction * erwartete Bewegung, oder
# (b) widersprüchlich: Reaktion seit Post >= against_reaction_fraction der
# erwarteten Bewegung GEGEN das Signal, oder starker 20-Tage-Gegentrend
# (> contradiction_threshold), der auch über 5 Tage noch intakt ist.
# ─────────────────────────────────────────────────────────────────────────────
def confirm_market_signal(signal: MarketSignal, post_time: datetime,
                          cfg: dict | None = None) -> tuple[MarketSignal | None, str]:
    cfg = cfg or CFG
    closes = fetch_daily_closes(signal.yf_symbol)
    if len(closes) < 25:
        return signal, "Marktdaten unzureichend (<25 Tage Historie) — Signal ungeprüft übernommen"

    dates = [d for d, _ in closes]
    prices = [p for _, p in closes]
    current = prices[-1]
    ret_5d = current / prices[-6] - 1 if len(prices) >= 6 else 0.0
    ret_20d = current / prices[-21] - 1 if len(prices) >= 21 else 0.0

    tail = np.array(prices[-31:] if len(prices) >= 31 else prices, dtype=float)
    log_rets_recent = np.diff(np.log(tail))
    vol30 = float(np.std(log_rets_recent) * math.sqrt(252)) if len(log_rets_recent) else None

    if post_time.tzinfo is None:
        post_time = post_time.replace(tzinfo=timezone.utc)
    pre_price = None
    for d, p in zip(dates, prices):
        if d < post_time:
            pre_price = p
        else:
            break
    reaction = (current / pre_price - 1) if pre_price else 0.0

    dirn = 1 if signal.direction == "LONG" else (-1 if signal.direction == "SHORT" else 0)
    expected = signal.expected_return or 0.0

    if dirn != 0 and expected != 0 and (reaction * dirn) > 0 and \
       abs(reaction) >= cfg["priced_in_fraction"] * abs(expected):
        return None, (f"Signal bereits eingepreist — Reaktion seit Post "
                      f"{reaction*100:+.1f}% vs. erwartet {expected*100:+.1f}%")

    # Widerspruch 1: Markt hat seit dem Post klar GEGEN das Signal reagiert.
    if dirn != 0 and expected != 0 and (reaction * dirn) < 0 and \
       abs(reaction) >= cfg.get("against_reaction_fraction", 0.5) * abs(expected):
        return None, (f"Markt widerspricht Signal — Reaktion seit Post "
                      f"{reaction*100:+.1f}% gegen {signal.direction}")

    # Widerspruch 2: starker Gegentrend, der noch intakt ist (auch 5T gegen
    # das Signal). Ein normaler Monatstrend allein blockiert keinen Katalysator —
    # Trump-Posts sind oft gerade der Trendbruch.
    if dirn != 0 and (ret_20d * dirn) < -cfg["contradiction_threshold"] and (ret_5d * dirn) < 0:
        return None, (f"Trend widerspricht Signal — 20T {ret_20d*100:+.1f}%, "
                      f"5T {ret_5d*100:+.1f}% gegen {signal.direction}")

    updated_vol = signal.volatility if signal.volatility is not None else vol30
    note = (f"bestätigt — Reaktion seit Post {reaction*100:+.1f}%, "
           f"5T {ret_5d*100:+.1f}%, 20T {ret_20d*100:+.1f}%, HV30 "
           f"{(vol30*100 if vol30 is not None else float('nan')):.1f}%")
    return replace(signal, volatility=updated_vol), note


def spot_price(yf_sym: str) -> float | None:
    closes = fetch_daily_closes(yf_sym)
    return closes[-1][1] if closes else None


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 3 — ONVISTA DERIVATE-FINDER (primäre, alle Emittenten aggregierende
# Quelle) + VONTOBEL (zusätzliche Emittenten-Quelle, siehe Modul-Docstring
# für die bekannte Ask-Preis-Lücke)
# ─────────────────────────────────────────────────────────────────────────────
ONVISTA_FINDER_API = "https://api.onvista.de/api/v1/derivatives/finder/configuration_query"
ONVISTA_INSTRUMENTS_API = "https://api.onvista.de/api/v1/instruments/query"

_ENTITY_CACHE: dict[str, tuple | None] = {}


def resolve_onvista_entity(underlying: str) -> tuple[str, str] | None:
    """(entityType, entityValue) für die onvista-Finder-API. Makro-Basiswerte
    sind fest verdrahtet (live verifiziert), Aktien werden per Ticker/Firmenname
    über die öffentliche Such-API aufgelöst (analog main.py
    _onvista_underlying_entity)."""
    u = underlying.upper()
    if u in MACRO_UNDERLYINGS:
        return MACRO_UNDERLYINGS[u]["onvista"]
    if u in _ENTITY_CACHE:
        return _ENTITY_CACHE[u]

    terms = [u]
    company = (ENTITIES.get(u, {}).get("company") or [None])[0]
    if company:
        terms.append(company)

    result = None
    for term in terms:
        try:
            r = requests.get(ONVISTA_INSTRUMENTS_API, params={"searchValue": term},
                              headers={"User-Agent": FEED_AGENT}, timeout=10)
            r.raise_for_status()
            for item in r.json().get("list", []):
                if item.get("entityType") != "STOCK":
                    continue
                sym = (item.get("homeSymbol") or item.get("symbol") or "").upper()
                if sym == u or term != u:
                    ev = item.get("entityValue")
                    if ev:
                        result = ("STOCK", ev)
                    break
            if result:
                break
        except Exception as e:
            log.warning(f"  ⚠️  onvista Entity-Suche ({term}): {e}")
    _ENTITY_CACHE[u] = result
    return result


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_onvista_item(it: dict, underlying: str, direction: str) -> Product | None:
    try:
        if not it.get("openEnded"):
            return None
        instr = it.get("instrument", {})
        isin = instr.get("isin")
        wkn = instr.get("wkn") or (isin[5:11] if isin and len(isin) >= 11 else None)
        if not isin or not wkn:
            return None
        q = it.get("quote", {})
        spread_pct = it.get("spreadAskPct")
        ts = it.get("dateCalculation") or q.get("datetimeBid") or q.get("datetimeLast")
        return Product(
            wkn=wkn, isin=isin,
            issuer=(it.get("issuer") or {}).get("name") or "?",
            underlying=underlying, long_short=direction,
            bid=q.get("bid"), ask=q.get("ask"),
            spread_pct=round(spread_pct, 3) if spread_pct is not None else None,
            knockout_barrier=it.get("knockOutAbs"),
            financing_level=it.get("strikeAbs"),
            ratio=it.get("coverRatio"),
            leverage=it.get("gearingAsk"),
            quote_timestamp=_parse_iso(ts),
            currency=it.get("isoCurrency") or "EUR",
            underlying_currency=it.get("isoCurrencyUnderlying") or "USD",
            url=instr.get("urls", {}).get("WEBSITE", ""),
        )
    except Exception:
        return None


_ONVISTA_LEVERAGE_BANDS = [(2, 4), (4, 6), (6, 8), (8, 10), (10, 13), (13, 16), (16, 20)]


def fetch_products_onvista(underlying: str, direction: str,
                           per_page: int = 100, max_leverage: float | None = None) -> list[Product]:
    """Open-End-Knock-Outs für einen Basiswert/Richtung über alle Emittenten
    (die onvista-API aggregiert SG/Goldman/JPM/BNP/HSBC/Morgan Stanley/UBS/
    UniCredit/Vontobel/... hinter einem Call).

    onvista sortiert absteigend nach Hebel; bei Indizes/Gold gibt es >10.000
    Papiere, die ersten Seiten sind dann nur KO-nahe 200-800x-Turbos. Deshalb
    serverseitig openEnded=1 + gearingAskRange je Hebel-Band, eine Seite pro
    Band, danach Dedup per ISIN → ganzes sinnvolles Hebel-Spektrum abgedeckt."""
    entity = resolve_onvista_entity(underlying)
    if not entity:
        log.info(f"  ℹ️  onvista: kein Underlying für {underlying}")
        return []
    entity_type, entity_value = entity
    want_right = 2 if direction == "LONG" else 1
    max_lev = max_leverage or CFG.get("max_leverage", 20.0)

    products: list[Product] = []
    seen: set[str] = set()
    for lo, hi in _ONVISTA_LEVERAGE_BANDS:
        if lo >= max_lev:
            break
        hi = min(hi, max_lev)
        try:
            r = requests.get(ONVISTA_FINDER_API, params={
                "application": "WEBSITE", "device": "DESKTOP",
                "entityTypeUnderlying": entity_type, "entityValueUnderlying": entity_value,
                "page": 0, "perPage": per_page,
                "queryParameters": (f"entitySubType=KNOCKOUT_CERTIFICATE&idExerciseRight={want_right}"
                                    f"&openEnded=1&gearingAskRange={lo};{hi:g}&"),
            }, headers={"User-Agent": FEED_AGENT, "Accept": "application/json"}, timeout=12)
            r.raise_for_status()
            items = r.json().get("list", [])
        except Exception as e:
            log.warning(f"  ⚠️  onvista Finder ({underlying}, Hebel {lo}-{hi:g}): {e}")
            continue
        for it in items:
            if it.get("idExerciseRight") != want_right:
                continue
            p = _normalize_onvista_item(it, underlying, direction)
            if p and p.isin not in seen:
                seen.add(p.isin)
                products.append(p)
    return products


VONTOBEL_API = "https://markets.vontobel.com/api/v1"
VONTOBEL_PURL = ("https://markets.vontobel.com/de-de/produkte/hebel/"
                 "turbo-optionsscheine-open-end/")
_VT_HEADERS = {"User-Agent": FEED_AGENT, "Accept": "application/json"}
_VT_KEY_CACHE: dict[str, int | None] = {}


def _vontobel_underlying_key(underlying: str):
    u = underlying.upper()
    if u in _VT_KEY_CACHE:
        return _VT_KEY_CACHE[u]
    company = (ENTITIES.get(u, {}).get("company") or [u])[0]
    key = None
    for query in (company, u):
        try:
            r = requests.get(f"{VONTOBEL_API}/underlyings/search",
                             params={"Query": query, "Page": 0, "PageSize": 5,
                                     "ProductType": 5, "c": "de-de"},
                             headers=_VT_HEADERS, timeout=12)
            r.raise_for_status()
            for it in r.json().get("payload", {}).get("items", []):
                if query.lower().split()[0] in it.get("text", "").lower():
                    key = it.get("key")
                    break
            if key:
                break
        except Exception as e:
            log.warning(f"  ⚠️  Vontobel Underlying-Suche ({query}): {e}")
    _VT_KEY_CACHE[u] = key
    return key


def fetch_products_vontobel(underlying: str, direction: str) -> list[Product]:
    """Zusätzliche Emittenten-Quelle. BEKANNTE LÜCKE: die kostenlose Vontobel-
    API liefert über /products/search keinen Ask-Preis (nur Bid) — Kandidaten
    von hier fallen daher praktisch immer beim ask>0-Hartfilter raus, bis
    Vontobel selbst einen Ask veröffentlicht. Trotzdem eingebunden (Spec-
    Anforderung + zukunftssicher falls sich die API ändert)."""
    key = _vontobel_underlying_key(underlying)
    if not key:
        return []
    want_dir = 1 if direction == "LONG" else 2
    try:
        body = {"productType": 5, "page": 0, "pageSize": 50, "filters": [
            {"property": 1, "selectedItems": [{"key": key}]},
            {"property": 4, "selectedItem": {"key": want_dir}},
        ]}
        r = requests.post(f"{VONTOBEL_API}/products/search", json=body,
                          params={"c": "de-de"}, headers=_VT_HEADERS, timeout=15)
        r.raise_for_status()
        out = []
        for it in r.json().get("payload", {}).get("items", []):
            if it.get("direction") != want_dir or not it.get("leverage"):
                continue
            isin = next((f["isin"] for f in it.get("primaryFeatures", []) if f.get("isin")), "")
            if not isin:
                continue
            out.append(Product(
                wkn=isin[5:11], isin=isin, issuer="Vontobel", underlying=underlying,
                long_short=direction, bid=it.get("price", {}).get("bid"), ask=None,
                spread_pct=None, knockout_barrier=it.get("knockOut"),
                financing_level=it.get("knockOut"), ratio=None, leverage=it.get("leverage"),
                quote_timestamp=None, currency="EUR", underlying_currency="USD",
                url=VONTOBEL_PURL + isin,
            ))
        return out
    except Exception as e:
        log.warning(f"  ⚠️  Vontobel Produktsuche ({underlying}): {e}")
        return []


def fetch_all_products(underlying: str, direction: str) -> list[Product]:
    products = fetch_products_onvista(underlying, direction)
    seen = {p.isin for p in products}
    # Vontobel nur ergänzend (liefert keinen Ask) — Duplikate aus onvista verwerfen
    products += [p for p in fetch_products_vontobel(underlying, direction) if p.isin not in seen]
    return products


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 4 — HARTE FILTER
# ─────────────────────────────────────────────────────────────────────────────
def _is_trading_hours(now: datetime) -> bool:
    """Grobe Xetra/EU-Handelsfenster-Heuristik: Mo–Fr 07–21 UTC. Außerhalb
    dieses Fensters wird die Frische-Anforderung um stale_relax_factor
    gelockert (Kurse aktualisieren sich außerbörslich seltener/gar nicht)."""
    if now.weekday() >= 5:
        return False
    return 7 <= now.hour < 21


def apply_hard_filters(products: list[Product], spot: float, direction: str,
                       vol: float | None, horizon_days: int,
                       cfg: dict | None = None, now: datetime | None = None) -> list[Product]:
    cfg = cfg or CFG
    now = now or datetime.now(timezone.utc)
    vol = vol if vol is not None else 0.30  # konservativer Default falls unbekannt

    out = []
    for p in products:
        if p.bid is None or p.ask is None:
            continue
        # Bid == Ask ist keine handelbare Zweiwege-Quote (indikative Taxe,
        # z.B. außerhalb der Emittenten-Handelszeit) → verwerfen
        if p.bid <= 0 or p.ask <= 0 or p.bid >= p.ask:
            continue
        if p.knockout_barrier is None or p.leverage is None or p.leverage <= 0:
            continue
        if direction == "LONG" and p.knockout_barrier >= spot:
            continue
        if direction == "SHORT" and p.knockout_barrier <= spot:
            continue
        spread_pct = p.effective_spread_pct()
        if spread_pct is None or spread_pct > cfg["max_spread_pct"]:
            continue
        if p.leverage > cfg["max_leverage"]:
            continue

        ko_dist_pct = abs(p.knockout_barrier - spot) / spot * 100.0
        vol_based_min_pct = cfg["min_ko_distance_vol_mult"] * vol * math.sqrt(horizon_days / 252.0) * 100.0
        if ko_dist_pct < max(cfg["min_ko_distance_pct"], vol_based_min_pct):
            continue

        if p.quote_timestamp is not None:
            age_min = (now - p.quote_timestamp).total_seconds() / 60.0
            limit = cfg["freshness_minutes"]
            if not _is_trading_hours(now):
                limit *= cfg.get("stale_relax_factor", 4)
            if age_min > limit:
                continue

        out.append(p)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 5-9 — RISIKO PRO PRODUKT: P(KO) via bootstrap Monte-Carlo mit
# Pfadabhängigkeit + Brownian-Bridge-Korrektur für Intraday-Berührungen,
# erwartete Netto-Rendite, konservative Variante
# ─────────────────────────────────────────────────────────────────────────────
def _bridge_touch_prob(s0: np.ndarray, s1: np.ndarray, barrier: float,
                       direction: str, sigma_daily: float) -> np.ndarray:
    """P(Pfad berührt barrier zwischen zwei aufeinanderfolgenden Tagesschluss-
    kursen s0→s1), gegeben beide Punkte liegen (noch) auf der sicheren Seite.
    Standard-Brownsche-Brücken-Formel: P = exp(-2*a*b / sigma^2) mit a,b =
    log-Abstand der Endpunkte zur Barriere (>=0 auf der sicheren Seite)."""
    eps = 1e-9
    if direction == "LONG":  # KO wird von oben nach unten durchbrochen
        a = np.log(np.maximum(s0, eps) / barrier)
        b = np.log(np.maximum(s1, eps) / barrier)
        already = (s0 <= barrier) | (s1 <= barrier)
    else:  # SHORT: KO wird von unten nach oben durchbrochen
        a = np.log(barrier / np.maximum(s0, eps))
        b = np.log(barrier / np.maximum(s1, eps))
        already = (s0 >= barrier) | (s1 >= barrier)
    a = np.maximum(a, 0.0)
    b = np.maximum(b, 0.0)
    denom = max(sigma_daily ** 2, 1e-8)
    p = np.exp(-2.0 * a * b / denom)
    return np.where(already, 1.0, p)


def _fx_overlay(underlying_currency: str, horizon_days: int, n_paths: int,
                rng: np.random.Generator) -> np.ndarray:
    """Grobe, unabhängige FX-Überlagerung: wenn der Basiswert nicht in EUR
    notiert, wird der EUR-Wert der Auszahlung zusätzlich durch die EURUSD-
    Bewegung über den Horizont verwässert/verstärkt. Vereinfachung
    (dokumentiert): kein gemeinsames Modell mit dem Basiswert-Pfad, nur ein
    additiver Bootstrap-Overlay auf Basis der historischen EURUSD-Renditen."""
    if underlying_currency == "EUR":
        return np.zeros(n_paths)
    fx_closes = fetch_daily_closes("EURUSD=X")
    if len(fx_closes) < 30:
        return np.zeros(n_paths)
    fx_prices = np.array([c for _, c in fx_closes], dtype=float)
    fx_rets = np.diff(np.log(fx_prices))
    idx = rng.integers(0, len(fx_rets), size=(n_paths, horizon_days))
    fx_path_return = np.exp(np.sum(fx_rets[idx], axis=1)) - 1.0
    # EURUSD steigt = EUR wird stärker ggü. USD => EUR-Wert eines USD-Payoffs sinkt
    return -fx_path_return


def financing_cost_fraction(direction: str, horizon_days: int, cfg: dict | None = None) -> float:
    """Vereinfachtes Modell: annual_rate = Referenzzins + Emittenten-Aufschlag,
    linear auf den Horizont skaliert und direkt als Renditeabzug auf die
    Basiswert-Exposition angewendet (nicht separat mit Hebel multipliziert,
    da der Finanzierungslevel/KO selbst mit der Zeit driftet und dieser Drift
    bereits die Hebel-Exposition preisgesteuert widerspiegelt). Für Short-
    Turbos per Konfiguration wahlweise als Gutschrift (negative Kosten)
    behandelbar — Default: auch Short kostet (konservativ, da die kostenlose
    API keine belastbare Aufschlüsselung Long-Finanzierung/Short-Gutschrift
    liefert)."""
    cfg = cfg or CFG
    annual_rate = cfg["financing_reference_rate"] + cfg["financing_issuer_spread"]
    if direction == "SHORT" and cfg.get("short_financing_is_credit", False):
        annual_rate = -cfg["financing_issuer_spread"]
    return annual_rate * (horizon_days / 365.0)


def conservative_expected_return(closes, spot, product, direction, horizon_days,
                                 expected_return, confidence, uncertainty,
                                 n_paths, seed, cfg: dict | None = None) -> float:
    """Konservative EV-Variante: zweite Simulation mit dem Signal-Drift
    geschrumpft um Konfidenz und Unsicherheit —
    shrunk_drift = expected_return * confidence * uncertainty_shrink[uncertainty]
    (z.B. HIGH-Unsicherheit + 50% Konfidenz ⇒ nur 20% des rohen Signal-Drifts
    bleiben übrig für die EV-Berechnung).

    Bewusst NICHT zusätzlich mit dem unteren Perzentil (P5) der Rendite-
    verteilung kombiniert (kein min(shrunk_ev, p5)): bei den hier üblichen
    Hebeln von 5-15x ist das P5-Quantil einer bootstrap-simulierten
    Rendite strukturell fast immer nahe -100% (schon ein durchschnittlicher
    Gegenbewegungstag reicht bei 10x Hebel), unabhängig davon wie gut das
    Signal ist — als Gate hätte das jede Empfehlung verhindert. Das P5-
    Quantil wird trotzdem berechnet und in ProductMetrics.p5_return für die
    Risikohinweise ausgewiesen, fließt aber nicht in die NO-TRADE-Schwelle
    ein."""
    cfg = cfg or CFG
    shrink = cfg["uncertainty_shrink"].get(uncertainty, 0.5)
    shrunk_return = expected_return * confidence * shrink
    result = simulate_product_full(closes, spot, product, direction, horizon_days,
                                   shrunk_return, n_paths, seed + 1, cfg)
    return result[1] if result else expected_return * confidence * shrink


def simulate_product_full(closes, spot, product, direction, horizon_days,
                          expected_return, n_paths, seed, cfg=None):
    """Wie simulate_product(), gibt aber zusätzlich das untere Perzentil der
    Netto-Rendite-Verteilung zurück (für die konservative Schranke)."""
    cfg = cfg or CFG
    prices = np.array([c for _, c in closes], dtype=float)
    if len(prices) < 30:
        return None
    log_rets = np.diff(np.log(prices))
    hist_mean = log_rets.mean()
    sigma_daily = float(log_rets.std())
    if sigma_daily <= 0:
        return None
    rng = np.random.default_rng(seed)
    n = len(log_rets)
    daily_drift = expected_return / horizon_days
    idx = rng.integers(0, n, size=(n_paths, horizon_days))
    sampled = log_rets[idx] - hist_mean + daily_drift
    log_path = np.cumsum(sampled, axis=1)
    price_paths = spot * np.exp(log_path)
    full = np.concatenate([np.full((n_paths, 1), spot), price_paths], axis=1)

    ko = product.knockout_barrier
    p_no_touch = np.ones(n_paths)
    for t in range(horizon_days):
        s0, s1 = full[:, t], full[:, t + 1]
        p_touch = _bridge_touch_prob(s0, s1, ko, direction, sigma_daily)
        p_no_touch *= (1.0 - p_touch)
    p_ko = float(np.clip(1.0 - p_no_touch.mean(), 0.0, 1.0))

    underlying_return = price_paths[:, -1] / spot - 1.0
    directional_return = underlying_return if direction == "LONG" else -underlying_return
    fin_cost = financing_cost_fraction(direction, horizon_days, cfg)
    fx_overlay = _fx_overlay(product.underlying_currency, horizon_days, n_paths, rng)
    net_underlying_return = directional_return - fin_cost + fx_overlay
    gross_value = np.clip(product.ask * (1.0 + product.leverage * net_underlying_return), 0.0, None)
    if direction == "LONG":
        ko_hit_path = np.any(full <= ko, axis=1)
    else:
        ko_hit_path = np.any(full >= ko, axis=1)
    exit_value = np.where(ko_hit_path, 0.0, gross_value)
    net_return_paths = (exit_value - product.ask) / product.ask
    p5 = float(np.percentile(net_return_paths, cfg.get("conservative_ci_percentile", 5)))
    return p_ko, float(net_return_paths.mean()), p5


def compute_score(expected_net_return: float, p_ko: float, uncertainty: str,
                  spread_pct: float, financing_cost: float, cfg: dict | None = None,
                  leverage: float | None = None) -> float:
    """score = expected_net_return − KO_PENALTY*p_ko − UNCERTAINTY_PENALTY*uncertainty
              − spread_cost − financing_cost   (Gewichte aus config.yml)

    Mit score_per_exposure=true (Default) und bekanntem Hebel werden Rendite und
    Kosten je Einheit Basiswert-Exposure bewertet:
        score = (EV − spread_cost − financing_cost) / leverage
                − KO_PENALTY*p_ko − UNCERTAINTY_PENALTY*uncertainty
    Sonst skaliert der EV linear mit dem Hebel und das Ranking wählt stets das
    Produkt mit dem höchsten zulässigen Hebel. Pro Exposure gewinnt das Papier,
    das dasselbe Signal am günstigsten und mit dem geringsten KO-Risiko umsetzt."""
    cfg = cfg or CFG
    unc_num = {"LOW": 0.0, "MEDIUM": 0.5, "HIGH": 1.0}.get(uncertainty, 0.5)
    net = (expected_net_return
           - cfg["spread_cost_weight"] * (spread_pct / 100.0)
           - cfg["financing_cost_weight"] * financing_cost)
    if cfg.get("score_per_exposure", True) and leverage and leverage > 0:
        net /= leverage
    return (net
            - cfg["ko_penalty"] * p_ko
            - cfg["uncertainty_penalty"] * unc_num)


def evaluate_candidates(products: list[Product], signal: MarketSignal,
                        cfg: dict | None = None) -> list[ProductMetrics]:
    cfg = cfg or CFG
    closes = fetch_daily_closes(signal.yf_symbol)
    spot = closes[-1][1] if closes else None
    if spot is None:
        return []
    out = []
    for p in products:
        full = simulate_product_full(closes, spot, p, signal.direction, signal.horizon_days,
                                     signal.expected_return, cfg["n_paths"], cfg["mc_seed"], cfg)
        if not full:
            continue
        p_ko, exp_net, p5 = full
        cons = conservative_expected_return(
            closes, spot, p, signal.direction, signal.horizon_days,
            signal.expected_return, signal.confidence, signal.uncertainty,
            cfg["n_paths"], cfg["mc_seed"], cfg,
        )
        fin_cost = financing_cost_fraction(signal.direction, signal.horizon_days, cfg)
        spread_pct = p.effective_spread_pct() or 0.0
        ko_dist_pct = abs(p.knockout_barrier - spot) / spot * 100.0
        ko_dist_vol = ko_dist_pct / 100.0 / ((signal.volatility or 0.30) * math.sqrt(signal.horizon_days / 252.0))
        score = compute_score(exp_net, p_ko, signal.uncertainty, spread_pct, fin_cost, cfg,
                              leverage=p.leverage)
        out.append(ProductMetrics(
            product=p, p_ko=p_ko, expected_net_return=exp_net,
            conservative_expected_return=cons, p5_return=p5, ko_distance_pct=ko_dist_pct,
            ko_distance_in_vol=ko_dist_vol, financing_cost_frac=fin_cost, score=score,
        ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 10-12 — RANKING, ENTSCHEIDUNG, RENDERING
# ─────────────────────────────────────────────────────────────────────────────
def rank_and_decide(candidates: list[ProductMetrics], signal: MarketSignal,
                    cfg: dict | None = None) -> tuple[str, str, ProductMetrics | None,
                                                      float | None, ProductMetrics | None]:
    cfg = cfg or CFG
    if not candidates:
        return "NO_TRADE", "kein Produkt erfüllt die Mindestkriterien (Spread/KO-Abstand/Frische)", None, None, None

    candidates.sort(key=lambda m: m.score, reverse=True)
    median_score = float(np.median([c.score for c in candidates]))
    # Nur Produkte mit positivem konservativem EV sind empfehlbar — erst
    # filtern, dann ranken (sonst verdeckt ein Top-Score mit negativem
    # konservativem EV ein empfehlbares Produkt dahinter).
    eligible = [c for c in candidates if c.conservative_expected_return > 0]
    if not eligible:
        return ("NO_TRADE", "kein Produkt mit positivem konservativem Expected Return",
               candidates[0], median_score, None)
    best = eligible[0]
    best_alt = next((c for c in eligible[1:] if c.product.issuer != best.product.issuer), None)

    if signal.confidence >= cfg["actionable_confidence_min"] and best.expected_net_return > 0:
        return "ACTIONABLE", "Signal bestätigt, Produkt erfüllt alle Filter, EV positiv", best, median_score, best_alt
    return "WATCH", "Positiver konservativer EV, aber Konfidenz/EV unter ACTIONABLE-Schwelle", best, median_score, best_alt


def _fmt_pct(x: float) -> str:
    return f"{x*100:+.1f}%"


def render_result(signal: MarketSignal, decision: str, reason: str,
                  best: ProductMetrics | None, median_score: float | None,
                  best_alt: ProductMetrics | None, note: str = "") -> str:
    label = signal.macro_label or signal.underlying
    lines = [
        f"UNDERLYING:        {label}",
        f"SIGNAL:            {signal.direction} · erwartet {_fmt_pct(signal.expected_return)} "
        f"· Unsicherheit {signal.uncertainty}",
        f"HORIZON:           {signal.horizon_days} Handelstage",
        f"SIGNAL CONFIDENCE: {signal.confidence*100:.0f}%",
    ]
    if note:
        lines.append(f"MARKTBESTÄTIGUNG:  {note}")
    lines.append(f"RATIONALE:         {signal.rationale}")
    lines.append("─" * 50)

    if decision == "NO_TRADE" or best is None:
        lines.append("❌ NO TRADE")
        lines.append(f"Grund: {reason}")
        if best is not None:
            lines.append("")
            lines.append("(bestes verworfenes Kandidat zur Referenz:)")
            lines.append(_product_block(best, prefix="   ", horizon_days=signal.horizon_days))
        return "\n".join(lines)

    tag = "✅ ACTIONABLE" if decision == "ACTIONABLE" else "🟡 WATCH"
    lines.append(f"{tag} — EMPFOHLENER TURBO:")
    lines.append(_product_block(best, prefix="   ", horizon_days=signal.horizon_days))
    lines.append("")
    lines.append("Warum dieses Produkt:")
    for b in _why_bullets(best, median_score, signal):
        lines.append(f"  + {b}")
    lines.append("")
    lines.append("Risiken:")
    for b in _risk_bullets(best, signal):
        lines.append(f"  - {b}")
    if median_score is not None or best_alt is not None:
        lines.append("")
        cmp_parts = []
        if median_score is not None:
            cmp_parts.append(f"Median vergleichbarer Score: {median_score:.4f}")
        if best_alt is not None:
            cmp_parts.append(
                f"Beste Alternative ({best_alt.product.issuer}): {best_alt.product.isin} "
                f"Score {best_alt.score:.4f}")
        lines.append(" · ".join(cmp_parts))
    return "\n".join(lines)


def _product_block(m: ProductMetrics, prefix: str = "", horizon_days: int | None = None) -> str:
    p = m.product
    n_txt = f"{horizon_days}" if horizon_days is not None else "N"
    return "\n".join(f"{prefix}{l}" for l in [
        f"WKN:              {p.wkn}",
        f"ISIN:             {p.isin}",
        f"Emittent:         {p.issuer}",
        f"Bid / Ask:        {p.bid:.3f} / {p.ask:.3f} EUR",
        f"Spread:           {p.effective_spread_pct():.2f}%",
        f"Hebel:            {p.leverage:.2f}x",
        f"KO-Abstand:       {m.ko_distance_pct:.1f}% ({m.ko_distance_in_vol:.2f}x Vol-Horizont)",
        f"P(KO innerhalb {n_txt} Tagen): {m.p_ko*100:.1f}%",
        f"Expected Net Return:      {_fmt_pct(m.expected_net_return)}",
        f"Conservative Exp. Return: {_fmt_pct(m.conservative_expected_return)}",
        f"Score:            {m.score:.4f}",
    ])


def _why_bullets(m: ProductMetrics, median_score: float | None, signal: MarketSignal) -> list[str]:
    out = []
    out.append(f"KO-Abstand {m.ko_distance_pct:.1f}% entspricht {m.ko_distance_in_vol:.2f}x der "
              f"erwarteten Vol-Bewegung über den Horizont")
    if median_score is not None and m.score > median_score:
        out.append(f"Score {m.score:.4f} über dem Median vergleichbarer Produkte ({median_score:.4f})")
    out.append(f"Spread {m.product.effective_spread_pct():.2f}% im zulässigen Rahmen")
    if signal.direction in ("LONG", "SHORT"):
        out.append(f"Signal- und Marktbestätigungsrichtung stimmen überein ({signal.direction})")
    return out


def _risk_bullets(m: ProductMetrics, signal: MarketSignal) -> list[str]:
    out = [
        f"P(KO innerhalb {signal.horizon_days} Tagen) = {m.p_ko*100:.1f}%",
        f"Hebel {m.product.leverage:.1f}x verstärkt Verluste symmetrisch zu Gewinnen",
        f"Ungünstiges Szenario (5%-Quantil der Simulation): {_fmt_pct(m.p5_return)}",
        f"Finanzierungskosten über Horizont ≈ {m.financing_cost_frac*100:.2f}%",
    ]
    if signal.uncertainty != "LOW":
        out.append(f"Signal-Unsicherheit: {signal.uncertainty}")
    if m.product.underlying_currency != m.product.currency:
        out.append(f"Fremdwährungsrisiko ({m.product.underlying_currency} vs. {m.product.currency}), "
                   f"nur näherungsweise modelliert")
    return out


def _text_to_html(text: str) -> str:
    escaped = html.escape(text)
    linked = re.sub(r'(https?://[^\s<]+)', r'<a href="\1" style="color:#0071e3;">\1</a>', escaped)
    return linked


# ─────────────────────────────────────────────────────────────────────────────
# SCHRITT 13 — PERSISTENZ
# ─────────────────────────────────────────────────────────────────────────────
def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS turbo_selections (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp                 TEXT,
            trump_post_id             TEXT,
            post_text_hash            TEXT,
            underlying                TEXT,
            direction                 TEXT,
            signal_confidence         REAL,
            horizon                   INTEGER,
            selected_wkn              TEXT,
            selected_isin             TEXT,
            issuer                    TEXT,
            bid                       REAL,
            ask                       REAL,
            spread                    REAL,
            leverage                  REAL,
            ko_distance               REAL,
            p_ko                      REAL,
            expected_net_return       REAL,
            conservative_expected_return REAL,
            score                     REAL,
            decision                  TEXT,
            median_comparable_score   REAL,
            best_alternative_isin     TEXT,
            underlying_price_at_signal    REAL,
            product_bid_after_horizon     REAL,
            underlying_return_after_horizon REAL,
            knocked_out               INTEGER
        )
    """)
    conn.commit()


def persist_selection(conn: sqlite3.Connection | None, result: SelectionResult,
                      trump_post_id: str, post_text_hash: str) -> None:
    if conn is None:
        return
    try:
        ensure_schema(conn)
        sig = result.signal
        best = result.selected
        spot = spot_price(sig.yf_symbol) if sig else None
        conn.execute(
            "INSERT INTO turbo_selections (timestamp, trump_post_id, post_text_hash, underlying, "
            "direction, signal_confidence, horizon, selected_wkn, selected_isin, issuer, bid, ask, "
            "spread, leverage, ko_distance, p_ko, expected_net_return, conservative_expected_return, "
            "score, decision, median_comparable_score, best_alternative_isin, underlying_price_at_signal) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                datetime.now(timezone.utc).isoformat(), trump_post_id, post_text_hash,
                sig.underlying if sig else None, sig.direction if sig else None,
                sig.confidence if sig else None, sig.horizon_days if sig else None,
                best.product.wkn if best else None, best.product.isin if best else None,
                best.product.issuer if best else None, best.product.bid if best else None,
                best.product.ask if best else None,
                best.product.effective_spread_pct() if best else None,
                best.product.leverage if best else None,
                best.ko_distance_pct if best else None, best.p_ko if best else None,
                best.expected_net_return if best else None,
                best.conservative_expected_return if best else None,
                best.score if best else None, result.decision, result.median_comparable_score,
                result.best_alternative.product.isin if result.best_alternative else None,
                spot,
            ),
        )
        conn.commit()
    except Exception as e:
        log.warning(f"  ⚠️  turbo_selections INSERT fehlgeschlagen: {e}")


def record_outcomes(conn: sqlite3.Connection, min_age_days: int = 1) -> int:
    """Füllt underlying_price_at_signal/underlying_return_after_horizon für
    Selektionen nach, deren Horizont abgelaufen ist. Produkt-Bid nach Horizont
    wird best-effort über onvista nachgeschlagen (sonst NULL — die kostenlose
    API hat keine Historie je ISIN)."""
    ensure_schema(conn)
    filled = 0
    cur = conn.execute(
        "SELECT id, timestamp, underlying, horizon, underlying_price_at_signal, selected_isin, direction "
        "FROM turbo_selections WHERE underlying_return_after_horizon IS NULL"
    )
    rows = cur.fetchall()
    now = datetime.now(timezone.utc)
    for row in rows:
        sel_id, ts, underlying, horizon, price_at_signal, isin, direction = row
        if not ts or not horizon:
            continue
        try:
            signal_dt = datetime.fromisoformat(ts)
        except Exception:
            continue
        if (now - signal_dt).days < horizon:
            continue
        yf_sym = _yf_symbol(underlying)
        closes = fetch_daily_closes(yf_sym)
        if not closes:
            continue
        current_price = closes[-1][1]
        base_price = price_at_signal
        if base_price is None:
            for d, p in closes:
                if d <= signal_dt:
                    base_price = p
        if not base_price:
            continue
        ret = current_price / base_price - 1.0
        knocked_out = None  # ohne Produkt-Kurshistorie nicht sicher bestimmbar → NULL bleibt
        conn.execute(
            "UPDATE turbo_selections SET underlying_price_at_signal=?, "
            "underlying_return_after_horizon=? WHERE id=?",
            (base_price, ret, sel_id),
        )
        filled += 1
    conn.commit()
    return filled


# ─────────────────────────────────────────────────────────────────────────────
# HAUPTEINSTIEGSPUNKT
# ─────────────────────────────────────────────────────────────────────────────
def select_turbo(signal: MarketSignal, post_time: datetime,
                 conn: sqlite3.Connection | None = None,
                 trump_post_id: str = "", post_text_hash: str = "",
                 cfg: dict | None = None) -> SelectionResult:
    """Kompletter Schritt 2-12 der Pipeline für EIN Signal (Aktie ODER Makro-
    Basiswert). Gibt max. ein empfohlenes Produkt zurück, sonst NO_TRADE."""
    cfg = cfg or CFG
    if signal.direction not in ("LONG", "SHORT"):
        result = SelectionResult(decision="NO_TRADE", reason="Kein klares LONG/SHORT-Signal",
                                 signal=signal)
        result.text = render_result(signal, "NO_TRADE", result.reason, None, None, None)
        result.html = _text_to_html(result.text)
        persist_selection(conn, result, trump_post_id, post_text_hash)
        return result

    confirmed, note = confirm_market_signal(signal, post_time, cfg)
    if confirmed is None:
        result = SelectionResult(decision="NO_TRADE", reason=note, signal=signal)
        result.text = render_result(signal, "NO_TRADE", note, None, None, None)
        result.html = _text_to_html(result.text)
        persist_selection(conn, result, trump_post_id, post_text_hash)
        return result
    signal = confirmed

    spot = spot_price(signal.yf_symbol)
    if spot is None:
        result = SelectionResult(decision="NO_TRADE", reason="Keine Marktdaten für Basiswert verfügbar",
                                 signal=signal)
        result.text = render_result(signal, "NO_TRADE", result.reason, None, None, None)
        result.html = _text_to_html(result.text)
        persist_selection(conn, result, trump_post_id, post_text_hash)
        return result

    raw_products = fetch_all_products(signal.underlying, signal.direction)
    filtered = apply_hard_filters(raw_products, spot, signal.direction, signal.volatility,
                                  signal.horizon_days, cfg)
    candidates = evaluate_candidates(filtered, signal, cfg)
    decision, reason, best, median_score, best_alt = rank_and_decide(candidates, signal, cfg)

    result = SelectionResult(
        decision=decision, reason=reason, signal=signal, selected=best if decision != "NO_TRADE" else best,
        median_comparable_score=median_score, best_alternative=best_alt,
        n_fetched=len(raw_products), n_filtered=len(filtered),
    )
    # Bei NO_TRADE trotzdem "best" (falls vorhanden) NICHT als 'selected' im Sinne
    # einer Empfehlung ausweisen, aber zur Referenz im Text mitgeben:
    if decision == "NO_TRADE":
        result.selected = None
        result.text = render_result(signal, decision, reason, best, median_score, best_alt, note)
    else:
        result.text = render_result(signal, decision, reason, best, median_score, best_alt, note)
    result.html = _text_to_html(result.text)
    persist_selection(conn, result, trump_post_id, post_text_hash)
    return result
