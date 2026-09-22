"""
Selbsttest der kompletten Analyse-Pipeline mit echtem Anthropic-Key:
Beispiel-Post → Haiku-Screen → Sonnet-Analyse → Turbo Selector DE.

Keine E-Mail (send_gmail wird ersetzt und gibt die Mail nur im Log aus),
kein Cooldown. Der Workflow selftest.yml committet alerts.db NICHT, alle
DB-Schreibzugriffe bleiben also im Runner. Kosten: 1 Haiku- + 1 Sonnet-Call.

Exit-Code 1, wenn Sonnet die Turbo-Selector-Felder nicht liefert oder der
Selector nicht gelaufen ist.
"""
import html
import re
import sys
import time

import main as m

SAMPLE_POST = (
    "I have just approved the sale of NVIDIA's most advanced AI chips to our "
    "great allies in the Middle East. Tremendous deal for American jobs and "
    "American technology. NVIDIA will be BIGGER than ever!"
)
TICKER = "NVDA"
REQUIRED_FIELDS = ("HORIZON_DAYS:", "EXPECTED_MOVE_PCT:", "SIGNAL_CONFIDENCE_PCT:",
                   "UNCERTAINTY:", "MACRO_UNDERLYING:")

captured: dict = {}
_orig_create = m.client.messages.create


def _capture_create(**kwargs):
    resp = _orig_create(**kwargs)
    if kwargs.get("model") == m.MODEL:
        captured["sonnet_text"] = next((b.text for b in resp.content if b.type == "text"), "")
        captured["stop_reason"] = resp.stop_reason
        captured["usage"] = resp.usage
    return resp


def _fake_gmail(subject: str, html_body: str) -> bool:
    captured["subject"] = subject
    captured["body"] = html_body
    return True


m.client.messages.create = _capture_create
m.send_gmail = _fake_gmail
m._rate_limit_ok = lambda ticker: True
m._rate_limit_record = lambda ticker: None

# Eindeutiger Text, damit der Dedup-Check den Selbsttest nie blockiert
text = f"{SAMPLE_POST} [selftest {int(time.time())}]"
m.analyze_and_alert("Truth Social", m.now_utc().isoformat(), text, TICKER,
                    "https://truthsocial.com/@realDonaldTrump", "hoch")

sonnet = captured.get("sonnet_text", "")
print("\n══ SONNET-ANTWORT ══════════════════════════════════════")
print(sonnet or "(keine — Haiku-Screen oder API-Fehler, siehe Log oben)")
print(f"stop_reason={captured.get('stop_reason')}  usage={captured.get('usage')}")

missing = [f for f in REQUIRED_FIELDS if f not in sonnet.upper()]
print("\n══ MAIL ═══════════════════════════════════════════════")
print("Betreff:", captured.get("subject"))
body = re.sub(r"<[^>]+>", " ", captured.get("body", ""))
body = re.sub(r"[ \t]+", " ", html.unescape(body))
i = body.find("UNDERLYING")
print(body[i:i + 3000] if i >= 0 else "(kein Turbo-Selector-Block in der Mail)")

rows = m.conn.execute(
    "SELECT underlying, direction, horizon, selected_wkn, issuer, p_ko, "
    "expected_net_return, conservative_expected_return, decision "
    "FROM turbo_selections ORDER BY rowid DESC LIMIT 2"
).fetchall()
print("\n══ turbo_selections ═══════════════════════════════════")
for r in rows:
    print(r)

errors = []
if not sonnet:
    errors.append("keine Sonnet-Antwort")
if captured.get("stop_reason") == "max_tokens":
    errors.append("Sonnet-Antwort bei max_tokens abgeschnitten")
if missing:
    errors.append(f"Sonnet-Felder fehlen: {missing}")
if not rows:
    errors.append("Turbo Selector hat nichts in turbo_selections geschrieben")
if "subject" not in captured:
    errors.append("keine Mail erzeugt (Gate?) — siehe Log")

if errors:
    print("\n❌ SELBSTTEST FEHLGESCHLAGEN:", "; ".join(errors))
    sys.exit(1)
print("\n✅ SELBSTTEST OK")
