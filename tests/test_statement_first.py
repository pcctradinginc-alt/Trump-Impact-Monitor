"""
Offline tests for the statement-first Truth Social flow (main.py):
triage JSON parsing, triage cache, terminal-decision markers, macro/foreign
underlyings as primary subject, onvista name-hint logic and the daily digest.

No network and no real Anthropic key: main.client is replaced by a fake that
counts calls, Yahoo/onvista/selector calls are monkeypatched.

Run with:  python3 -m pytest -q tests
"""
import json
import os
import shutil
import sys
import tempfile
import types
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FILES_NEEDED = ["main.py", "config.py", "config.yml", "entities.json", "turbo_selector.py"]


def _import_main():
    if "main" in sys.modules and hasattr(sys.modules["main"], "triage_post"):
        return sys.modules["main"]
    scratch = tempfile.mkdtemp(prefix="trump_monitor_sf_test_")
    for fname in _FILES_NEEDED:
        shutil.copy(os.path.join(REPO_ROOT, fname), os.path.join(scratch, fname))
    for k in ("ANTHROPIC_API_KEY", "GMAIL_EMAIL", "GMAIL_APP_PASSWORD", "RECIPIENT_EMAIL"):
        os.environ.setdefault(k, "x")
    old = os.getcwd()
    os.chdir(scratch)
    sys.path.insert(0, scratch)
    try:
        import main as m  # noqa: PLC0415
    finally:
        os.chdir(old)
    return m


m = _import_main()
ts = m.turbo_selector


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────
class FakeClient:
    """Zählt Calls; liefert je Modell eine vorbereitete Antwort."""

    def __init__(self, haiku_text="", sonnet_text=""):
        self.haiku_text, self.sonnet_text = haiku_text, sonnet_text
        self.calls = []
        self.messages = types.SimpleNamespace(create=self._create)

    def _create(self, **kw):
        self.calls.append(kw["model"])
        text = self.haiku_text if "haiku" in kw["model"] else self.sonnet_text
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(type="text", text=text)], stop_reason="end_turn")


def _sonnet_text(direction="LONG", relevance="YES", conf="LOW", macro="NONE"):
    return f"""RELEVANCE: {relevance} — x is directly named
COMPANY: X (X)
EVENT_DATE: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}
EVENT_SUMMARY: Trump lobt das Unternehmen.
SENTIMENT: BULLISH for X
MAGNITUDE_ESTIMATE: SMALL <3% — x
TRADE_DIRECTION: {direction}
CONFIDENCE_SCORE: {conf} — x
HORIZON_DAYS: 5
EXPECTED_MOVE_PCT: {3 if direction == 'LONG' else -3}
SIGNAL_CONFIDENCE_PCT: 60
UNCERTAINTY: MEDIUM
MACRO_UNDERLYING: {macro}
MACRO_DIRECTION: {'SHORT' if macro != 'NONE' else 'NEUTRAL'}
MACRO_EXPECTED_MOVE_PCT: -2
RATIONALE: Direkte Aussage."""


def _patch_env(monkeypatch, client, mails=None, selector=None):
    monkeypatch.setattr(m, "client", client)
    monkeypatch.setattr(m, "fetch_market_data", lambda t: {"price": 10.0, "chg_1d": 0.0, "chg_1w": 0.0, "chg_1m": 0.0})
    monkeypatch.setattr(m, "format_market_block", lambda t, is_macro=None: "Schlusskurs 10.00")
    monkeypatch.setattr(m, "trump_position_performance", lambda t: "")
    monkeypatch.setattr(m, "send_gmail", lambda s, b: (mails.append((s, b)) if mails is not None else None) or True)
    monkeypatch.setitem(m.TURBO_SELECTOR_CFG, "send_no_trade_alerts", True)
    if selector is not None:
        monkeypatch.setattr(ts, "select_turbo", selector)


def _fake_selector(captured):
    def sel(signal, post_time, conn=None, trump_post_id="", post_text_hash="", cfg=None):
        captured.append(signal)
        return ts.SelectionResult(decision="NO_TRADE", reason="Testgrund", signal=signal,
                                  text="UNDERLYING: test\n❌ NO TRADE", html="")
    return sel


# ─────────────────────────────────────────────────────────────────────────────
# Triage JSON parsing
# ─────────────────────────────────────────────────────────────────────────────
GOOD = {"relevant": True, "reason": "Bayer baut Werk", "underlyings": [
    {"symbol": "bayn.de", "name": "Bayer", "kind": "NAMED", "direction": "long"}]}


def test_parse_plain_json():
    r = m.parse_triage_json(json.dumps(GOOD))
    assert r["ok"] and r["relevant"]
    assert r["underlyings"] == [{"symbol": "BAYN.DE", "name": "Bayer", "kind": "NAMED", "direction": "LONG"}]


def test_parse_code_fence_and_prose():
    raw = "Here you go:\n```json\n" + json.dumps(GOOD) + "\n```\nHope it helps"
    assert m.parse_triage_json(raw)["underlyings"][0]["symbol"] == "BAYN.DE"
    assert m.parse_triage_json("```" + json.dumps(GOOD) + "```")["relevant"] is True


def test_parse_garbage_is_not_relevant():
    for raw in ("", "   ", "no json at all", "{not json}", "[1,2,3]", None, 42, '{"foo": 1}'):
        r = m.parse_triage_json(raw)
        assert r["ok"] is False and r["relevant"] is False and r["underlyings"] == [], raw


def test_parse_macro_code_forces_macro_kind_and_validates():
    raw = json.dumps({"relevant": "true", "reason": "Öl", "underlyings": [
        {"symbol": "$brent", "name": "Brent", "kind": "SECTOR", "direction": "SHORT"},
        {"symbol": "XOM", "name": "Exxon", "kind": "WEIRD", "direction": "SHORT"},
        {"symbol": "BAD SYMBOL!", "kind": "NAMED", "direction": "LONG"},
        {"symbol": "CVX", "kind": "NAMED", "direction": "SIDEWAYS"},
        {"symbol": "BRENT", "kind": "MACRO", "direction": "LONG"},   # Dublette
        {"symbol": "WTI", "name": "WTI", "kind": "MACRO", "direction": "SHORT"},
        {"symbol": "GOLD", "kind": "MACRO", "direction": "LONG"},    # über Limit 3
    ]})
    r = m.parse_triage_json(raw)
    assert r["relevant"] is True
    assert [(u["symbol"], u["kind"]) for u in r["underlyings"]] == [
        ("BRENT", "MACRO"), ("XOM", "SECTOR"), ("WTI", "MACRO")]


def test_parse_not_relevant_drops_underlyings():
    r = m.parse_triage_json(json.dumps({"relevant": False, "reason": "Politik", "underlyings": GOOD["underlyings"]}))
    assert r["ok"] and not r["relevant"] and r["underlyings"] == []


def test_merge_explicit_and_pick_underlyings():
    tri = {"underlyings": [{"symbol": "BA", "name": "Boeing", "kind": "NAMED", "direction": "LONG"},
                           {"symbol": "LMT", "name": "Lockheed", "kind": "SECTOR", "direction": "SHORT"}]}
    merged = m.merge_explicit_tickers(tri, [("BA", "hoch"), ("NVDA", "hoch"), ("XYZ", "niedrig"), ("GOLD", "hoch")])
    syms = [u["symbol"] for u in merged]
    assert syms.count("BA") == 1 and "NVDA" in syms and "XYZ" not in syms and "GOLD" not in syms
    assert next(u for u in merged if u["symbol"] == "NVDA")["kind"] == "NAMED"
    picked = m.pick_underlyings(merged)
    assert all(u["kind"] != "SECTOR" for u in picked)   # SECTOR nur ohne NAMED/MACRO
    only_sector = m.pick_underlyings([tri["underlyings"][1]])
    assert [u["symbol"] for u in only_sector] == ["LMT"]


# ─────────────────────────────────────────────────────────────────────────────
# Triage cache + terminal decisions (no repeated API calls)
# ─────────────────────────────────────────────────────────────────────────────
def test_triage_is_cached_per_post(monkeypatch):
    fc = FakeClient(haiku_text="```json\n" + json.dumps(GOOD) + "\n```")
    monkeypatch.setattr(m, "client", fc)
    text = "Bayer just announced a 2.2 Billion Dollar campus near Columbus [cache-test]"
    r1 = m.triage_post("Truth Social", "https://x/1", text)
    r2 = m.triage_post("Truth Social", "https://x/1", text)
    assert len(fc.calls) == 1
    assert r1["relevant"] and r2["relevant"] and r2["underlyings"][0]["symbol"] == "BAYN.DE"
    row = m.conn.execute("SELECT relevant, source, outcome FROM post_triage WHERE hash=?",
                         (m.get_hash(text),)).fetchone()
    assert row[0] == 1 and row[1] == "Truth Social"


def test_triage_garbage_not_cached_until_max_fails(monkeypatch):
    fc = FakeClient(haiku_text="sorry I cannot")
    monkeypatch.setattr(m, "client", fc)
    text = "Some long enough statement about nothing in particular [garbage-test]"
    assert m.triage_post("Truth Social", "u", text) is None
    assert m.triage_post("Truth Social", "u", text) is None
    r = m.triage_post("Truth Social", "u", text)    # 3. Fehler → als irrelevant gecacht
    assert r is not None and r["relevant"] is False
    n = len(fc.calls)
    assert m.triage_post("Truth Social", "u", text)["relevant"] is False
    assert len(fc.calls) == n


def test_triage_short_text_makes_no_call(monkeypatch):
    fc = FakeClient(haiku_text=json.dumps(GOOD))
    monkeypatch.setattr(m, "client", fc)
    assert m.triage_post("Truth Social", "u", "Thank you!")["relevant"] is False
    assert fc.calls == []


def test_haiku_no_trade_is_remembered_second_round_makes_no_call(monkeypatch):
    fc = FakeClient(haiku_text="NO_TRADE")
    _patch_env(monkeypatch, fc)
    text, ticker = "Unclear news about a company [marker-test-1]", "ZZMARK1"
    h = m.event_hash(ticker, text)
    assert not m.already_handled(h)
    m.analyze_and_alert("RSS", "now", text, ticker, "u", "hoch")        # klassischer Pfad
    assert fc.calls == ["claude-haiku-4-5-20251001"]
    assert m.is_decided(h) and m.already_handled(h)
    # Zweite Runde: die main()-Schleifen fragen already_handled() VOR dem Call
    calls_before = len(fc.calls)
    if not m.already_handled(h):
        m.analyze_and_alert("RSS", "now", text, ticker, "u", "hoch")
    assert len(fc.calls) == calls_before


def test_unknown_priority_claude_skip_is_marked_without_api_call(monkeypatch):
    fc = FakeClient()
    _patch_env(monkeypatch, fc)
    text, ticker = "Sector news [marker-test-2]", "ZZMARK2"
    m.analyze_and_alert("RSS", "now", text, ticker, "u", "claude")
    assert fc.calls == [] and m.is_decided(m.event_hash(ticker, text))


def test_cooldown_skip_is_not_marked(monkeypatch):
    fc = FakeClient(haiku_text="ACTIONABLE")
    _patch_env(monkeypatch, fc)
    text, ticker = "Cooldown news [marker-test-3]", "ZZMARK3"
    m.conn.execute("INSERT OR REPLACE INTO rate_limit (key,count,window_start) VALUES (?,1,?)",
                   (f"ticker_{ticker}", m.now_utc().isoformat()))
    m.conn.commit()
    m.analyze_and_alert("RSS", "now", text, ticker, "u", "hoch")
    assert fc.calls == []                                   # nichts aufgerufen
    assert not m.is_decided(m.event_hash(ticker, text))     # später erneut versuchen


def test_sonnet_relevance_no_is_terminal_and_sets_outcome(monkeypatch):
    fc = FakeClient(sonnet_text=_sonnet_text(relevance="NO"))
    _patch_env(monkeypatch, fc)
    text, ticker = "Boeing something unclear [marker-test-4]", "ZZMARK4"
    m.conn.execute("INSERT OR REPLACE INTO post_triage (hash,created_at,source,url,text,relevant,reason,underlyings,outcome) "
                   "VALUES (?,?,?,?,?,1,'r','[]','')", (m.get_hash(text), m.now_utc().isoformat(), "Truth Social", "u", text))
    m.conn.commit()
    m.analyze_and_alert("Truth Social", "now", text, ticker, "u", "hoch", kind="NAMED", name="Zz")
    assert m.is_decided(m.event_hash(ticker, text))
    out = m.conn.execute("SELECT outcome FROM post_triage WHERE hash=?", (m.get_hash(text),)).fetchone()[0]
    assert out.startswith("no_trade:") and ticker in out


# ─────────────────────────────────────────────────────────────────────────────
# Macro / foreign underlying as primary subject
# ─────────────────────────────────────────────────────────────────────────────
def test_macro_primary_builds_macro_signal_without_appendix(monkeypatch):
    captured, mails = [], []
    fc = FakeClient(sonnet_text=_sonnet_text("SHORT", macro="BRENT"))   # Sonnet will "suggest" BRENT again
    _patch_env(monkeypatch, fc, mails, _fake_selector(captured))
    text = "Europe releases diesel [macro-test]"
    m.analyze_and_alert("Truth Social", "now", text, "BRENT", "u", "hoch", kind="MACRO", name="Brent crude")
    assert len(captured) == 1                              # kein zusätzlicher Makro-Anhang
    sig = captured[0]
    assert sig.is_macro and sig.underlying == "BRENT" and sig.yf_symbol == "BZ=F"
    assert sig.macro_label == "Brent" and sig.direction == "SHORT"
    subject, body = mails[0]
    assert "Trump-Impact – Brent [SHORT · NO TRADE]" in subject and subject.endswith("Truth Social")
    assert "Kurzfazit" in body and "Trumps bekannte Positionen" not in body
    assert body.index("Kurzfazit") < body.index("Quelltext")


def test_named_foreign_symbol_passes_name_and_bypasses_gates(monkeypatch):
    captured, mails = [], []
    fc = FakeClient(sonnet_text=_sonnet_text("LONG", conf="LOW"))
    _patch_env(monkeypatch, fc, mails, _fake_selector(captured))
    text = "Bayer announces campus [foreign-test]"
    # confidence="claude" + unbekanntes Symbol würde im klassischen Pfad verworfen,
    # NAMED aus der Triage umgeht das Gate; LOW-Konfidenz reicht bei Truth+NAMED.
    m.analyze_and_alert("Truth Social", "now", text, "BAYN.DE", "u", "claude", kind="NAMED", name="Bayer")
    assert len(captured) == 1 and captured[0].underlying == "BAYN.DE"
    assert captured[0].name == "Bayer" and captured[0].yf_symbol == "BAYN.DE" and not captured[0].is_macro
    assert "haiku" not in "".join(fc.calls)                # Pre-Screen übersprungen
    assert "Bayer (BAYN.DE) [LONG · NO TRADE] – Truth Social" in mails[0][0]


def test_truth_threshold_low_only_for_truth_named_or_macro(monkeypatch):
    import config
    assert config.confidence_ok("LOW", truth_primary=True)
    assert not config.confidence_ok("LOW")
    captured, mails = [], []
    fc = FakeClient(sonnet_text=_sonnet_text("LONG", conf="LOW"))
    _patch_env(monkeypatch, fc, mails, _fake_selector(captured))
    text = "Low conf news [thr-test]"
    m.analyze_and_alert("RSS", "now", text, "NVDA", "u", "hoch")          # News: MEDIUM nötig
    assert captured == [] and m.is_decided(m.event_hash("NVDA", text))


# ─────────────────────────────────────────────────────────────────────────────
# onvista name hint (pure logic)
# ─────────────────────────────────────────────────────────────────────────────
ONVISTA_LIST = [
    {"entityType": "STOCK", "name": "BMW", "homeSymbol": "BMW", "entityValue": "81490"},
    {"entityType": "BOND", "name": "Bayer AG MTN", "entityValue": "248147207"},
    {"entityType": "STOCK", "name": "Bayer", "homeSymbol": "BAYN", "entityValue": "25272187"},
]


def test_onvista_search_terms_foreign_uses_name_first():
    assert ts.onvista_search_terms("BAYN.DE", "Bayer") == ["Bayer", "BAYN"]
    assert ts.onvista_search_terms("SAP.DE", None) == ["SAP"]
    us = ts.onvista_search_terms("AAPL", "Apple Inc")
    assert us[0] == "AAPL" and "Apple Inc" in us


def test_pick_onvista_stock_prefers_symbol_then_name():
    assert ts.pick_onvista_stock(ONVISTA_LIST, "BAYN", "BAYN.DE", "Bayer") == "25272187"
    # Name-Suche ohne Symbol-Treffer: nur Aktie, deren Name mit dem Hinweis beginnt (nicht BMW)
    no_sym = [ONVISTA_LIST[0], ONVISTA_LIST[1], {"entityType": "STOCK", "name": "Bayer", "homeSymbol": "XX", "entityValue": "7"}]
    assert ts.pick_onvista_stock(no_sym, "Bayer", "BAYN.DE", "Bayer") == "7"
    assert ts.pick_onvista_stock([ONVISTA_LIST[0]], "Bayer", "BAYN.DE", "Bayer") is None
    # Ticker-Suche ohne Treffer darf nie die erste beliebige Aktie nehmen
    assert ts.pick_onvista_stock([ONVISTA_LIST[0]], "AAPL", "AAPL", None) is None


def test_resolve_onvista_entity_uses_name_hint(monkeypatch):
    seen = []

    class R:
        def __init__(self, term): self.term = term
        def raise_for_status(self): pass
        def json(self): return {"list": ONVISTA_LIST if self.term == "Bayer" else []}

    def fake_get(url, params=None, **kw):
        seen.append(params["searchValue"])
        return R(params["searchValue"])

    monkeypatch.setattr(ts.requests, "get", fake_get)
    ts._ENTITY_CACHE.clear()
    assert ts.resolve_onvista_entity("BAYN.DE", "Bayer") == ("STOCK", "25272187")
    assert seen == ["Bayer"]
    assert ts.resolve_onvista_entity("BRENT") == ts.MACRO_UNDERLYINGS["BRENT"]["onvista"]


# ─────────────────────────────────────────────────────────────────────────────
# Daily digest
# ─────────────────────────────────────────────────────────────────────────────
def _evening():
    return datetime.now(timezone.utc).replace(hour=20, minute=30, second=0, microsecond=0)


def _digest_setup(monkeypatch):
    mails = []
    monkeypatch.setattr(m, "send_gmail", lambda s, b: mails.append((s, b)) or True)
    monkeypatch.setattr(m, "now_utc", _evening)
    m.conn.execute("DELETE FROM post_triage")
    m.conn.execute("DELETE FROM events WHERE source='DAILY_SUMMARY'")
    m.conn.commit()
    return mails


def test_build_digest_none_without_rows():
    assert m.build_digest([], "2026-10-04") is None


def test_digest_not_sent_without_relevant_statement(monkeypatch):
    mails = _digest_setup(monkeypatch)
    m.conn.execute("INSERT INTO post_triage (hash,created_at,source,url,text,relevant,reason,underlyings,outcome) "
                   "VALUES ('h0',?,?,?,?,0,'Politik','[]','')", (_evening().isoformat(), "Truth Social", "u", "Endorsement"))
    m.conn.commit()
    m._maybe_send_daily_summary([], 0)
    assert mails == []


def test_digest_sent_once_with_relevant_statements(monkeypatch):
    mails = _digest_setup(monkeypatch)
    unders = json.dumps([{"symbol": "BAYN.DE", "name": "Bayer", "kind": "NAMED", "direction": "LONG"}])
    m.conn.execute("INSERT INTO post_triage (hash,created_at,source,url,text,relevant,reason,underlyings,outcome) "
                   "VALUES ('h1',?,?,?,?,1,'Werk',?,?)",
                   (_evening().isoformat(), "Truth Social", "u", "Bayer builds a campus", unders,
                    "alert_sent:Bayer (BAYN.DE) LONG ACTIONABLE"))
    m.conn.execute("INSERT INTO post_triage (hash,created_at,source,url,text,relevant,reason,underlyings,outcome) "
                   "VALUES ('h2',?,?,?,?,1,'Öl',?,?)",
                   (_evening().isoformat(), "Truth Social", "u", "Diesel release",
                    json.dumps([{"symbol": "BRENT", "name": "Brent", "kind": "MACRO", "direction": "SHORT"}]),
                    "no_trade: Brent SHORT – Signal bereits eingepreist"))
    m.conn.commit()
    m._maybe_send_daily_summary([], 0)
    assert len(mails) == 1
    subject, body = mails[0]
    assert "Tages-Digest" in subject and "2 relevante" in subject and "1 Alert" in subject
    for needle in ("Bayer builds a campus", "Bayer (BAYN.DE) LONG", "Alert gesendet", "NO TRADE", "eingepreist"):
        assert needle in body, needle
    m._maybe_send_daily_summary([], 0)        # gleicher Tag → Sentinel verhindert zweite Mail
    assert len(mails) == 1


def test_digest_can_be_switched_off(monkeypatch):
    mails = _digest_setup(monkeypatch)
    m.conn.execute("INSERT INTO post_triage (hash,created_at,source,url,text,relevant,reason,underlyings,outcome) "
                   "VALUES ('h3',?,?,?,?,1,'x','[]','')", (_evening().isoformat(), "Truth Social", "u", "Something relevant"))
    m.conn.commit()
    monkeypatch.setattr(m, "DAILY_DIGEST", False)
    m._maybe_send_daily_summary([], 0)
    assert mails == []
