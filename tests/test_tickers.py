"""
Regression tests for find_all_tickers() in main.py.

Guards against the phantom-ticker regression found in the 2026-09-22 live
smoke test, where the news-RSS pipeline sent WTO, UK, AI, MS, ACA, BBT, CMS,
HOMB, CHCO, JD to the (paid) Sonnet analysis step as if they were real stock
mentions, alongside genuine hits like DJT, MSFT, AMZN.

Root causes fixed in main.py:
  - Tier 1 (bare uppercase symbol match, no '$'): any of the ~7000
    entities.json keys could bare-match in normal mixed-case text as long as
    it wasn't in the small hand-picked English-word stoplist. Government/
    organization acronyms that also happen to be ticker symbols (WTO, UK,
    AI, ACA, CMS, ...) slipped through. Fix: bare matches now require the
    symbol be in config.yml's priority_high OR at least 3 characters long
    AND not in the new _ACRONYM_STOPLIST; 2-letter bare symbols (AI, MS, JD,
    TV, SF, ...) always need a '$' prefix or explicit "(NASDAQ: XYZ)" /
    "(NYSE: XYZ)" exchange context.
  - Tier 2 (company-name alias): several small-cap tickers are aliased to a
    single generic English word in entities.json (HOMB -> "Home",
    CHCO -> "City", BBT -> "Beacon", AFGB/C/D/E -> "American",
    IBOC -> "International", NTIC -> "Northern", JYNT -> "Joint",
    STRC/D/F/K -> "Strategy", TWI -> "Titan"), which matched almost any
    news text. Fix: _GENERIC_ALIAS_STOPWORDS blocks these generic aliases
    in both Tier 2 and Tier 3.

Run with:  python3 -m pytest -q tests
       or: python3 tests/test_tickers.py
"""
import os
import shutil
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Files main.py needs at import time (config.py reads config.yml directly;
# entities.json is loaded relative to main.py's own directory).
_FILES_NEEDED = ["main.py", "config.py", "config.yml", "entities.json", "turbo_selector.py"]


def _import_main_in_scratch_copy():
    """
    Copies the repo's main.py + friends into a fresh temp directory and
    imports main from there, so import-time side effects (sqlite3.connect
    on DB_PATH = "alerts.db", which CREATEs tables if missing) never touch
    the real repo's alerts.db. Dummy secrets satisfy main.py's own
    REQUIRED-env-var check at import time.
    """
    scratch = tempfile.mkdtemp(prefix="trump_monitor_test_")
    for fname in _FILES_NEEDED:
        shutil.copy(os.path.join(REPO_ROOT, fname), os.path.join(scratch, fname))

    os.environ.setdefault("ANTHROPIC_API_KEY", "x")
    os.environ.setdefault("GMAIL_EMAIL", "x")
    os.environ.setdefault("GMAIL_APP_PASSWORD", "x")
    os.environ.setdefault("RECIPIENT_EMAIL", "x")

    old_cwd = os.getcwd()
    os.chdir(scratch)
    sys.path.insert(0, scratch)
    try:
        import main as m  # noqa: PLC0415 (import must happen inside scratch cwd)
    finally:
        os.chdir(old_cwd)
    return m


main = _import_main_in_scratch_copy()
find_all_tickers = main.find_all_tickers


def _tickers(text):
    return {t for t, _confidence in find_all_tickers(text)}


# ─────────────────────────────────────────────────────────────────────────────
# Positive cases — real ticker mentions that must still be detected
# ─────────────────────────────────────────────────────────────────────────────

def test_apple_iphones_usa():
    hits = _tickers("Trump says Apple must build iPhones in USA")
    assert "AAPL" in hits, hits


def test_dollar_prefixed_symbol_always_wins():
    hits = _tickers("Buy $TSLA before the announcement")
    assert "TSLA" in hits, hits


def test_nvidia_chips_to_china():
    hits = _tickers("Nvidia chips to China restricted by new export rule")
    assert "NVDA" in hits, hits


def test_boeing_deal_with_qatar():
    hits = _tickers("Boeing deal with Qatar signed at the White House")
    assert "BA" in hits, hits


def test_intel_stake():
    hits = _tickers("Trump administration takes equity stake in Intel")
    assert "INTC" in hits, hits


def test_priority_high_bare_symbol_still_matches():
    # BA is only 2 characters but is curated in config.yml's priority_high,
    # so it must still bare-match without a '$' or exchange context.
    hits = _tickers("Boeing wins new defense contract")
    assert "BA" in hits, hits


def test_exchange_context_allows_short_symbol():
    hits = _tickers("Hub Group (NASDAQ: HUBG) rallied on the news")
    assert "HUBG" in hits, hits


# ─────────────────────────────────────────────────────────────────────────────
# Negative cases — acronyms / generic words that must NOT trigger analysis
# ─────────────────────────────────────────────────────────────────────────────

def test_uk_trade_deal_is_not_a_ticker():
    assert _tickers("Trump announces new UK trade deal") == set()


def test_wto_ruling_is_not_a_ticker():
    assert _tickers("WTO ruling goes against US tariffs") == set()


def test_ai_executive_order_is_not_a_ticker():
    assert _tickers("Trump signs AI executive order on data centers") == set()


def test_aca_subsidies_is_not_a_ticker():
    assert "ACA" not in _tickers(
        "Vance-led task force removes enrollees from Affordable Care Act (ACA) coverage"
    )


def test_cms_medicare_rule_is_not_a_ticker():
    assert "CMS" not in _tickers(
        "Administrator for the Centers for Medicare and Medicaid Services (CMS) "
        "announces new rule"
    )


def test_eu_tariffs_is_not_a_ticker():
    assert _tickers("Trump threatens new tariffs on the EU") == set()


def test_ms_now_media_outlet_is_not_a_ticker():
    # "MS NOW" is a cable-news brand name in current coverage of the White
    # House press-pool dispute, not a mention of Morgan Stanley (MS).
    assert "MS" not in _tickers("CNN, MS NOW and Politico sue Trump over White House ban")


def test_jd_vance_is_not_jd_com():
    assert "JD" not in _tickers("Vice President JD Vance took a jab at reporters")


def test_generic_home_alias_is_not_homb():
    assert "HOMB" not in _tickers("Trump speaks about the American Home and family values")


def test_generic_city_alias_is_not_chco():
    assert "CHCO" not in _tickers("Trump to meet with NYC Mayor at City Hall")


def test_generic_american_alias_is_not_afg_series():
    text = "Trump addressed the United Nations on American strength and values"
    hits = _tickers(text)
    assert not hits & {"AFGB", "AFGC", "AFGD", "AFGE"}, hits


def test_all_caps_truth_social_post_needs_dollar_prefix():
    # Typical Trump Truth Social ALL-CAPS style: bare tickers must not fire.
    hits = _tickers("THE FAKE NEWS MEDIA IS THE ENEMY OF THE PEOPLE, SAD!")
    assert hits == set(), hits


def test_all_caps_with_dollar_prefix_still_matches():
    hits = _tickers("BUY $DJT NOW, TREMENDOUS COMPANY, THE BEST!")
    assert "DJT" in hits, hits


if __name__ == "__main__":
    failures = 0
    tests = [(name, fn) for name, fn in list(globals().items())
             if name.startswith("test_") and callable(fn)]
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {name}: {e}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    sys.exit(1 if failures else 0)
