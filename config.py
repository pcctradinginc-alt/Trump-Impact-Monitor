"""Lädt config.yml und stellt typisierte Konstanten bereit."""
import os
import yaml

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yml")

with open(_CONFIG_PATH, encoding="utf-8") as _f:
    _CFG = yaml.safe_load(_f)

# Watch-List
WATCHLIST_HIGH   = set(_CFG["watchlist"]["priority_high"])
WATCHLIST_MEDIUM = set(_CFG["watchlist"]["priority_medium"])
WATCHLIST_LOW    = set(_CFG["watchlist"]["priority_low"])
WATCHLIST_ALL    = WATCHLIST_HIGH | WATCHLIST_MEDIUM | WATCHLIST_LOW

# Schwellenwerte
MIN_CONFIDENCE   = _CFG["thresholds"]["min_confidence_score"]   # HIGH/MEDIUM/LOW
MIN_MAGNITUDE    = _CFG["thresholds"]["min_magnitude"]           # SMALL/MEDIUM/LARGE
MAX_ALERTS       = _CFG["thresholds"]["max_alerts_per_run"]
LOOKBACK_HOURS   = _CFG["thresholds"]["lookback_hours"]

# Quellen an/aus
SRC_TRUTH        = _CFG["sources"]["truth_social"]
SRC_RSS          = _CFG["sources"]["financial_rss"]
SRC_WHITEHOUSE   = _CFG["sources"]["whitehouse_rss"]
SRC_FEDREGISTER  = _CFG["sources"]["federal_register"]
SRC_EDGAR        = _CFG["sources"]["edgar_monitor"]
SRC_OGE          = _CFG["sources"]["oge_monitor"]

# Alert-Verhalten
SEND_NO_TRADE    = _CFG["alerts"]["send_no_trade"]
INCLUDE_RETWEETS = _CFG["alerts"]["include_retweets"]

# Konfidenz-Ranking (für Schwellenwert-Vergleich)
_CONF_RANK  = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
_MAG_RANK   = {"SMALL": 0, "MEDIUM": 1, "LARGE": 2}

def confidence_ok(score: str) -> bool:
    """True wenn CONFIDENCE_SCORE >= MIN_CONFIDENCE."""
    return _CONF_RANK.get(score.upper(), 0) >= _CONF_RANK.get(MIN_CONFIDENCE, 0)

def magnitude_ok(mag: str) -> bool:
    """True wenn MAGNITUDE_ESTIMATE >= MIN_MAGNITUDE."""
    return _MAG_RANK.get(mag.upper(), 0) >= _MAG_RANK.get(MIN_MAGNITUDE, 0)
