"""
Offline tests for turbo_selector.py ("Trump Post → Turbo Selector DE").

No network calls: daily-close histories are injected directly into
turbo_selector._CLOSES_CACHE (module-level per-process cache) instead of
going through fetch_daily_closes()/yfinance, and all Product objects are
constructed by hand rather than fetched from onvista/Vontobel.

Covers:
  - hard filters reject bad quotes / wrong-direction KO / too-close KO / stale quotes
  - P(KO) is monotone: far KO ~ 0, near KO high, longer horizon => higher P(KO)
  - ranking prefers lower spread / lower P(KO) at roughly equal EV
  - NO_TRADE when conservative EV <= 0, and when the signal is already priced in
  - the score formula actually uses the config weights (not hardcoded)

Run with:  python3 -m pytest -q tests
"""
import os
import shutil
import sys
import tempfile
import math
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FILES_NEEDED = ["turbo_selector.py", "config.py", "config.yml", "entities.json"]


def _import_turbo_selector_in_scratch_copy():
    scratch = tempfile.mkdtemp(prefix="turbo_selector_test_")
    for fname in _FILES_NEEDED:
        shutil.copy(os.path.join(REPO_ROOT, fname), os.path.join(scratch, fname))
    old_cwd = os.getcwd()
    os.chdir(scratch)
    sys.path.insert(0, scratch)
    try:
        import turbo_selector as ts  # noqa: PLC0415
    finally:
        os.chdir(old_cwd)
    return ts


ts = _import_turbo_selector_in_scratch_copy()


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic-data helpers
# ─────────────────────────────────────────────────────────────────────────────
def synthetic_closes(n=260, start=100.0, daily_vol=0.02, seed=7, drift=0.0):
    """Deterministic pseudo-random daily closes (no numpy RandomState surprises
    across versions: use numpy via the already-imported module's np)."""
    rng = ts.np.random.default_rng(seed)
    rets = rng.normal(drift, daily_vol, size=n)
    prices = start * ts.np.exp(ts.np.cumsum(rets))
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [(base + timedelta(days=i), float(p)) for i, p in enumerate(prices)]


def make_product(isin="DE000TEST001", issuer="TestBank", bid=1.0, ask=1.01,
                 ko=90.0, leverage=10.0, spread_pct=None, ts_offset_min=5,
                 currency="EUR", underlying_currency="USD", now=None):
    now = now or datetime.now(timezone.utc)
    return ts.Product(
        wkn=isin[5:11], isin=isin, issuer=issuer, underlying="TEST",
        long_short="LONG", bid=bid, ask=ask, spread_pct=spread_pct,
        knockout_barrier=ko, financing_level=ko, ratio=0.1, leverage=leverage,
        quote_timestamp=now - timedelta(minutes=ts_offset_min),
        currency=currency, underlying_currency=underlying_currency,
        url="https://example.test/product",
    )


def make_signal(direction="LONG", expected_return=0.05, horizon_days=5,
                confidence=0.7, uncertainty="MEDIUM", volatility=0.30):
    return ts.MarketSignal(
        underlying="TEST", yf_symbol="TEST", direction=direction,
        confidence=confidence, expected_return=expected_return,
        horizon_days=horizon_days, rationale="synthetic test signal",
        uncertainty=uncertainty, volatility=volatility,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Hard filters
# ─────────────────────────────────────────────────────────────────────────────
def test_filter_rejects_bad_quotes():
    spot = 100.0
    bad_bid_zero   = make_product(bid=0.0, ask=1.0, ko=90.0)
    bad_ask_zero   = make_product(bid=1.0, ask=0.0, ko=90.0)
    bad_bid_gt_ask = make_product(bid=2.0, ask=1.0, ko=90.0)
    bad_bid_eq_ask = make_product(bid=1.0, ask=1.0, ko=90.0)   # indikative Taxe
    good           = make_product(bid=1.0, ask=1.01, ko=90.0)
    out = ts.apply_hard_filters(
        [bad_bid_zero, bad_ask_zero, bad_bid_gt_ask, bad_bid_eq_ask, good],
        spot, "LONG", 0.30, 5,
    )
    assert [p.isin for p in out] == [good.isin]


def test_filter_rejects_wrong_direction_ko():
    spot = 100.0
    # LONG turbo braucht KO < spot; hier KO > spot -> muss rausfallen
    wrong = make_product(bid=1.0, ask=1.01, ko=110.0)
    right = make_product(bid=1.0, ask=1.01, ko=90.0)
    out = ts.apply_hard_filters([wrong, right], spot, "LONG", 0.30, 5)
    assert [p.isin for p in out] == [right.isin]

    # SHORT: KO muss > spot liegen
    wrong_short = make_product(bid=1.0, ask=1.01, ko=90.0)
    right_short = make_product(bid=1.0, ask=1.01, ko=110.0)
    out2 = ts.apply_hard_filters([wrong_short, right_short], spot, "SHORT", 0.30, 5)
    assert [p.isin for p in out2] == [right_short.isin]


def test_filter_rejects_ko_too_close():
    spot = 100.0
    too_close = make_product(bid=1.0, ask=1.01, ko=99.0)   # 1% Abstand
    far_enough = make_product(bid=1.0, ask=1.01, ko=85.0)  # 15% Abstand
    out = ts.apply_hard_filters([too_close, far_enough], spot, "LONG", 0.30, 5)
    assert [p.isin for p in out] == [far_enough.isin]


def test_filter_rejects_stale_quote_during_trading_hours():
    spot = 100.0
    now = datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)  # Mittwoch, 10:00 UTC -> Handelszeit
    stale = make_product(isin="DE000STALE01", bid=1.0, ask=1.01, ko=85.0, ts_offset_min=999, now=now)
    fresh = make_product(isin="DE000FRESH01", bid=1.0, ask=1.01, ko=85.0, ts_offset_min=2, now=now)
    out = ts.apply_hard_filters([stale, fresh], spot, "LONG", 0.30, 5, now=now)
    assert [p.isin for p in out] == [fresh.isin]


def test_filter_relaxes_freshness_outside_trading_hours():
    spot = 100.0
    now = datetime(2026, 9, 26, 23, 0, tzinfo=timezone.utc)  # Samstag -> außerhalb Handelszeit
    cfg = dict(ts.CFG)
    age = cfg["freshness_minutes"] * 2  # innerhalb relaxter, außerhalb normaler Grenze
    relaxed_ok = make_product(bid=1.0, ask=1.01, ko=85.0, ts_offset_min=age, now=now)
    out = ts.apply_hard_filters([relaxed_ok], spot, "LONG", 0.30, 5, now=now)
    assert [p.isin for p in out] == [relaxed_ok.isin]


def test_filter_rejects_excess_spread_and_leverage():
    spot = 100.0
    wide_spread = make_product(bid=1.0, ask=1.5, ko=85.0)   # ~33% Spread
    tight = make_product(bid=1.0, ask=1.01, ko=85.0)
    too_leveraged = make_product(bid=1.0, ask=1.01, ko=85.0, leverage=999.0)
    out = ts.apply_hard_filters([wide_spread, tight, too_leveraged], spot, "LONG", 0.30, 5)
    assert [p.isin for p in out] == [tight.isin]


# ─────────────────────────────────────────────────────────────────────────────
# P(KO) monotonicity
# ─────────────────────────────────────────────────────────────────────────────
def test_p_ko_monotone_in_ko_distance():
    closes = synthetic_closes(seed=1, daily_vol=0.02)
    spot = closes[-1][1]
    far_ko  = make_product(ko=spot * 0.70, ask=1.0, bid=0.99)
    near_ko = make_product(ko=spot * 0.97, ask=1.0, bid=0.99)
    far  = ts.simulate_product_full(closes, spot, far_ko, "LONG", 5, 0.0, 3000, 42)
    near = ts.simulate_product_full(closes, spot, near_ko, "LONG", 5, 0.0, 3000, 42)
    assert far[0] < 0.05      # weit weg -> praktisch kein KO
    assert near[0] > far[0]   # nah dran -> deutlich höher
    assert near[0] > 0.15


def test_p_ko_monotone_in_horizon():
    closes = synthetic_closes(seed=2, daily_vol=0.02)
    spot = closes[-1][1]
    product = make_product(ko=spot * 0.90, ask=1.0, bid=0.99)
    short_h = ts.simulate_product_full(closes, spot, product, "LONG", 3, 0.0, 3000, 42)
    long_h  = ts.simulate_product_full(closes, spot, product, "LONG", 10, 0.0, 3000, 42)
    assert long_h[0] >= short_h[0]


# ─────────────────────────────────────────────────────────────────────────────
# Ranking / scoring
# ─────────────────────────────────────────────────────────────────────────────
def test_rank_prefers_lower_spread_at_equal_ev():
    good_spread = ts.ProductMetrics(
        product=make_product(isin="DE000AAA0001", spread_pct=0.5),
        p_ko=0.1, expected_net_return=0.2, conservative_expected_return=0.05,
        p5_return=-0.5, ko_distance_pct=8.0, ko_distance_in_vol=1.0,
        financing_cost_frac=0.01,
        score=ts.compute_score(0.2, 0.1, "MEDIUM", 0.5, 0.01),
    )
    bad_spread = ts.ProductMetrics(
        product=make_product(isin="DE000BBB0002", spread_pct=3.0),
        p_ko=0.1, expected_net_return=0.2, conservative_expected_return=0.05,
        p5_return=-0.5, ko_distance_pct=8.0, ko_distance_in_vol=1.0,
        financing_cost_frac=0.01,
        score=ts.compute_score(0.2, 0.1, "MEDIUM", 3.0, 0.01),
    )
    sig = make_signal()
    decision, reason, best, median, alt = ts.rank_and_decide([bad_spread, good_spread], sig)
    assert best.product.isin == "DE000AAA0001"


def test_rank_prefers_lower_p_ko_at_equal_ev():
    low_pko = ts.ProductMetrics(
        product=make_product(isin="DE000CCC0003"),
        p_ko=0.02, expected_net_return=0.2, conservative_expected_return=0.05,
        p5_return=-0.5, ko_distance_pct=10.0, ko_distance_in_vol=1.5,
        financing_cost_frac=0.01,
        score=ts.compute_score(0.2, 0.02, "MEDIUM", 0.5, 0.01),
    )
    high_pko = ts.ProductMetrics(
        product=make_product(isin="DE000DDD0004"),
        p_ko=0.40, expected_net_return=0.2, conservative_expected_return=0.05,
        p5_return=-0.9, ko_distance_pct=3.0, ko_distance_in_vol=0.3,
        financing_cost_frac=0.01,
        score=ts.compute_score(0.2, 0.40, "MEDIUM", 0.5, 0.01),
    )
    sig = make_signal()
    decision, reason, best, median, alt = ts.rank_and_decide([high_pko, low_pko], sig)
    assert best.product.isin == "DE000CCC0003"


def test_score_formula_uses_config_weights():
    base = ts.compute_score(0.10, 0.10, "MEDIUM", 1.0, 0.02, cfg=ts.CFG)
    custom_cfg = dict(ts.CFG)
    custom_cfg["ko_penalty"] = ts.CFG["ko_penalty"] * 5
    boosted_penalty = ts.compute_score(0.10, 0.10, "MEDIUM", 1.0, 0.02, cfg=custom_cfg)
    assert boosted_penalty < base  # höherer KO_PENALTY -> niedrigerer Score bei gleichem p_ko>0

    custom_cfg2 = dict(ts.CFG)
    custom_cfg2["spread_cost_weight"] = 0.0
    no_spread_cost = ts.compute_score(0.10, 0.10, "MEDIUM", 5.0, 0.02, cfg=custom_cfg2)
    with_spread_cost = ts.compute_score(0.10, 0.10, "MEDIUM", 5.0, 0.02, cfg=ts.CFG)
    assert no_spread_cost > with_spread_cost


# ─────────────────────────────────────────────────────────────────────────────
# NO_TRADE gating
# ─────────────────────────────────────────────────────────────────────────────
def test_no_trade_when_conservative_ev_non_positive():
    negative = ts.ProductMetrics(
        product=make_product(isin="DE000EEE0005"),
        p_ko=0.5, expected_net_return=0.3, conservative_expected_return=-0.1,
        p5_return=-0.9, ko_distance_pct=3.0, ko_distance_in_vol=0.3,
        financing_cost_frac=0.01, score=0.5,
    )
    sig = make_signal()
    decision, reason, best, median, alt = ts.rank_and_decide([negative], sig)
    assert decision == "NO_TRADE"
    assert "konservativ" in reason.lower()


def test_no_trade_when_no_candidates():
    decision, reason, best, median, alt = ts.rank_and_decide([], make_signal())
    assert decision == "NO_TRADE"
    assert best is None


def test_market_confirmation_priced_in_triggers_no_trade():
    # Baue eine Historie, die in den letzten Tagen bereits stark im Long-Sinn
    # gestiegen ist -> Reaktion seit Post >= priced_in_fraction * erwartete Bewegung
    closes = synthetic_closes(seed=3, daily_vol=0.005, drift=0.0)
    # letzten 3 Tage künstlich stark nach oben ziehen (Reaktion auf den "Post")
    base_price = closes[-4][1]
    boosted = closes[:-3] + [
        (closes[-3][0], base_price * 1.05),
        (closes[-2][0], base_price * 1.06),
        (closes[-1][0], base_price * 1.07),
    ]
    ts._CLOSES_CACHE["PRICED_IN_TEST"] = boosted
    sig = ts.MarketSignal(underlying="PRICED_IN_TEST", yf_symbol="PRICED_IN_TEST",
                          direction="LONG", confidence=0.7, expected_return=0.05,
                          horizon_days=5, rationale="test", uncertainty="MEDIUM")
    post_time = boosted[-4][0] + timedelta(hours=12)
    confirmed, note = ts.confirm_market_signal(sig, post_time)
    assert confirmed is None
    assert "eingepreist" in note.lower()


def test_market_confirmation_contradiction_triggers_no_trade():
    closes = synthetic_closes(seed=4, daily_vol=0.005, drift=0.004)  # klarer Aufwärtstrend
    ts._CLOSES_CACHE["CONTRA_TEST"] = closes
    sig = ts.MarketSignal(underlying="CONTRA_TEST", yf_symbol="CONTRA_TEST",
                          direction="SHORT", confidence=0.7, expected_return=-0.05,
                          horizon_days=5, rationale="test", uncertainty="MEDIUM")
    post_time = closes[-1][0] + timedelta(hours=1)
    confirmed, note = ts.confirm_market_signal(sig, post_time)
    assert confirmed is None
    assert "widerspricht" in note.lower()


def test_full_select_turbo_no_trade_on_tiny_expected_move(monkeypatch):
    """End-to-end (aber offline): winzige erwartete Bewegung + neutrale
    Historie soll nicht zwingend am Markt-Gate scheitern, aber am
    evaluate_candidates()-Ergebnis (kein Produkt gefunden, da fetch_all_products
    ohne Netzwerk nichts liefert) -> NO_TRADE."""
    closes = synthetic_closes(seed=5, daily_vol=0.01, drift=0.0)
    ts._CLOSES_CACHE["TINY_TEST"] = closes
    monkeypatch.setattr(ts, "fetch_all_products", lambda underlying, direction: [])
    sig = ts.MarketSignal(underlying="TINY_TEST", yf_symbol="TINY_TEST",
                          direction="LONG", confidence=0.5, expected_return=0.001,
                          horizon_days=3, rationale="tiny", uncertainty="HIGH")
    post_time = closes[-1][0] + timedelta(hours=1)
    result = ts.select_turbo(sig, post_time, conn=None)
    assert result.decision == "NO_TRADE"


def test_moderate_countertrend_does_not_block_signal():
    """+6 % über 20 Tage, danach seitwärts: normaler Monatstrend darf einen
    SHORT-Katalysator nicht blockieren (Trump-Posts sind oft der Trendbruch)."""
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    prices = [100.0] * 230 + [100.0 * (1 + 0.06 * i / 20) for i in range(1, 21)] + [106.0, 106.2, 105.9, 106.1, 105.8, 105.7, 105.9, 105.6, 105.5, 105.4]
    ts._CLOSES_CACHE["COUNTER_OK"] = [(base + timedelta(days=i), p) for i, p in enumerate(prices)]
    sig = ts.MarketSignal(underlying="COUNTER_OK", yf_symbol="COUNTER_OK",
                          direction="SHORT", confidence=0.7, expected_return=-0.04,
                          horizon_days=5, rationale="test", uncertainty="MEDIUM")
    post_time = base + timedelta(days=len(prices) - 1, hours=1)
    confirmed, note = ts.confirm_market_signal(sig, post_time)
    assert confirmed is not None, note


def test_per_exposure_score_does_not_reward_leverage_alone():
    """Gleiches Signal (µ=3 %), gleiche Kosten in % des Produktpreises:
    20x mit höherem KO-Risiko darf 5x mit geringem KO-Risiko nicht schlagen,
    nur weil sein EV linear mit dem Hebel wächst."""
    low = ts.compute_score(5 * 0.03 - 0.01, 0.01, "MEDIUM", 0.5, 0.002, leverage=5.0)
    high = ts.compute_score(20 * 0.03 - 0.08, 0.12, "MEDIUM", 1.0, 0.008, leverage=20.0)
    assert low > high
    # ohne Hebel-Normierung (alte Formel) gewänne der 20x-Turbo
    cfg_old = dict(ts.CFG, score_per_exposure=False)
    assert ts.compute_score(20 * 0.03 - 0.08, 0.12, "MEDIUM", 1.0, 0.008, cfg=cfg_old, leverage=20.0) > \
           ts.compute_score(5 * 0.03 - 0.01, 0.01, "MEDIUM", 0.5, 0.002, cfg=cfg_old, leverage=5.0)


def test_rank_skips_top_score_with_negative_conservative_ev():
    top_but_negative = ts.ProductMetrics(
        product=make_product(isin="DE000EEE0005", issuer="BankA"),
        p_ko=0.05, expected_net_return=0.3, conservative_expected_return=-0.01,
        p5_return=-0.8, ko_distance_pct=6.0, ko_distance_in_vol=1.0,
        financing_cost_frac=0.01, score=0.9,
    )
    eligible = ts.ProductMetrics(
        product=make_product(isin="DE000FFF0006", issuer="BankB"),
        p_ko=0.02, expected_net_return=0.15, conservative_expected_return=0.04,
        p5_return=-0.4, ko_distance_pct=10.0, ko_distance_in_vol=1.8,
        financing_cost_frac=0.01, score=0.5,
    )
    decision, reason, best, median, alt = ts.rank_and_decide([top_but_negative, eligible], make_signal())
    assert decision != "NO_TRADE"
    assert best.product.isin == "DE000FFF0006"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
