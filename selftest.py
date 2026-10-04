"""
Selbsttest der Statement-first-Pipeline mit echtem Anthropic-Key:
Beispiel-Post → Haiku-Triage → Sonnet-Analyse → Turbo Selector DE → Mail.

Zwei Posts laufen durch den NEUEN Truth-Social-Pfad (wie in main()):
  1. Bayer-Werk (Auslandswert, Bayer ist nicht in entities.json) → Triage muss
     ein Bayer-Underlying (BAYN.DE) liefern.
  2. Diesel-Freigabe der EU (Rohstoff-Statement) → Triage muss ein Öl-Makro-
     Underlying (BRENT/WTI) liefern.

Keine echte E-Mail (send_gmail wird ersetzt), kein Cooldown. NO-TRADE-Mails
werden für den Test eingeschaltet, damit immer eine Mail entsteht. Der Workflow
selftest.yml committet alerts.db NICHT, alle DB-Schreibzugriffe bleiben im
Runner. Kosten: 2 Haiku- + max. 4 Sonnet-Calls (je Post höchstens 2 Underlyings).

Exit-Code 1, wenn die Triage nichts Relevantes liefert, eine Sonnet-Antwort bei
max_tokens abgeschnitten ist, die Turbo-Selector-Felder fehlen oder kein
Selector-Eintrag in turbo_selections geschrieben wurde.
"""
import html
import json
import re
import sys
import time

import main as m

SAMPLES = [
    {
        "name": "Bayer (Auslandswert, NAMED)",
        "text": ("Bayer just announced a brand new, 2.2 Billion Dollar Pharmaceutical "
                 "Manufacturing Campus near Columbus, Ohio. This is a tremendous vote of "
                 "confidence in the USA and in American workers!"),
        "ok": lambda u: ("BAY" in u["symbol"].upper() or "bayer" in (u.get("name") or "").lower()),
        "expect": "Bayer-Underlying (z.B. BAYN.DE)",
    },
    {
        "name": "Diesel-Freigabe (Makro Öl)",
        "text": ("Europe has just agreed to release a massive amount of their heavily stocked "
                 "Diesel Oil. The process will begin immediately."),
        "ok": lambda u: u["symbol"].upper() in ("BRENT", "WTI"),
        "expect": "Öl-Makro-Underlying BRENT oder WTI",
    },
]
MAX_UNDERLYINGS_PER_POST = 2
REQUIRED_FIELDS = ("HORIZON_DAYS:", "EXPECTED_MOVE_PCT:", "SIGNAL_CONFIDENCE_PCT:",
                   "UNCERTAINTY:", "MACRO_UNDERLYING:")

sonnet_calls: list[dict] = []     # {"text","stop_reason","usage"}
haiku_calls: list[dict] = []
mails: list[dict] = []
_orig_create = m.client.messages.create


def _capture_create(**kwargs):
    resp = _orig_create(**kwargs)
    text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
    rec = {"text": text, "stop_reason": resp.stop_reason, "usage": resp.usage}
    if kwargs.get("model") == m.MODEL:
        sonnet_calls.append(rec)
    else:
        haiku_calls.append(rec)
    return resp


def _fake_gmail(subject: str, html_body: str) -> bool:
    mails.append({"subject": subject, "body": html_body})
    return True


m.client.messages.create = _capture_create
m.send_gmail = _fake_gmail
m._rate_limit_ok = lambda ticker: True
m._rate_limit_record = lambda ticker: None
# Für den Test immer eine Mail erzeugen (auch bei NO TRADE), sonst wäre ein
# legitimes NO TRADE von der Prüfung "Mail erzeugt" nicht unterscheidbar.
m.TURBO_SELECTOR_CFG["send_no_trade_alerts"] = True
m.SEND_NO_TRADE = True

errors: list[str] = []
total_selector_rows = 0
stamp = int(time.time())

for idx, sample in enumerate(SAMPLES):
    print(f"\n{'═' * 70}\nPOST {idx + 1}: {sample['name']}\n{'═' * 70}")
    # Eindeutiger Text, damit Triage-Cache/Dedup den Selbsttest nie blockieren
    text = f"{sample['text']} [selftest {stamp}-{idx}]"
    url = f"https://truthsocial.com/@realDonaldTrump/selftest-{stamp}-{idx}"

    n_sonnet_before = len(sonnet_calls)
    n_mails_before = len(mails)
    tri = m.triage_post("Truth Social", url, text)

    print("── TRIAGE ──────────────────────────────────────────────")
    print(json.dumps(tri, ensure_ascii=False, indent=2) if tri else "(None — API-/Parse-Fehler)")
    if not tri or not tri.get("relevant") or not tri.get("underlyings"):
        errors.append(f"Post {idx + 1}: Triage lieferte nichts Relevantes")
        continue
    if not any(sample["ok"](u) for u in tri["underlyings"]):
        # Kein Abbruch (Haiku darf eine plausible Alternative wählen), aber sichtbar
        print(f"⚠️  Erwartet war: {sample['expect']} — Triage lieferte "
              f"{[u['symbol'] for u in tri['underlyings']]}")

    unders = m.pick_underlyings(tri["underlyings"], MAX_UNDERLYINGS_PER_POST)
    macros_here = tuple(u["symbol"] for u in unders if u["kind"] == "MACRO")
    for u in unders:
        print(f"\n── ANALYSE {u['symbol']} ({u['kind']}, {u.get('direction')}) ───────────────")
        m.analyze_and_alert(
            "Truth Social", m.now_utc().isoformat(), text, u["symbol"], url,
            "claude" if u["kind"] == "SECTOR" else "hoch",
            kind=u["kind"], name=u.get("name") or None,
            hint_direction=u.get("direction"), skip_macros=macros_here,
        )

    print("\n── SONNET-ANTWORT(EN) ──────────────────────────────────")
    new_sonnet = sonnet_calls[n_sonnet_before:]
    if not new_sonnet:
        errors.append(f"Post {idx + 1}: kein Sonnet-Call (Gate?) — siehe Log")
    for rec in new_sonnet:
        print(rec["text"] or "(leer)")
        print(f"stop_reason={rec['stop_reason']}  usage={rec['usage']}")
        if rec["stop_reason"] == "max_tokens":
            errors.append(f"Post {idx + 1}: Sonnet-Antwort bei max_tokens abgeschnitten")
        missing = [f for f in REQUIRED_FIELDS if f not in rec["text"].upper()]
        if missing:
            errors.append(f"Post {idx + 1}: Sonnet-Felder fehlen: {missing}")

    new_mails = mails[n_mails_before:]
    print("\n── MAIL ────────────────────────────────────────────────")
    if not new_mails:
        errors.append(f"Post {idx + 1}: keine Mail erzeugt (Gate?) — siehe Log")
    for mail in new_mails:
        print("Betreff:", mail["subject"])
        body = mail["body"]
        mm = re.search(r"Kurzfazit.*?</div>", body, re.S)
        kurz = re.sub(r"<[^>]+>", " ", mm.group(0)) if mm else "(kein Kurzfazit in der Mail!)"
        kurz = re.sub(r"\s+", " ", html.unescape(kurz)).strip()
        print("Kurzfazit:", kurz)
        if not mm:
            errors.append(f"Post {idx + 1}: Kurzfazit fehlt in der Mail")
        if not re.search(r"\[(LONG|SHORT|NO TRADE)[^\]]*\]", mail["subject"]):
            errors.append(f"Post {idx + 1}: Betreff ohne Richtung/Entscheidung")

    rows = m.conn.execute(
        "SELECT underlying, direction, horizon, selected_wkn, issuer, p_ko, "
        "expected_net_return, conservative_expected_return, decision "
        "FROM turbo_selections WHERE trump_post_id=? ORDER BY id",
        (url,),
    ).fetchall()
    print("\n── turbo_selections ────────────────────────────────────")
    for r in rows:
        print(r)
    total_selector_rows += len(rows)
    # Sonnet darf NO_TRADE urteilen (dann läuft der Selector zu Recht nicht) —
    # aber bei LONG/SHORT im Betreff muss ein Selector-Eintrag existieren.
    if not rows and any(re.search(r"\[(LONG|SHORT)", mm_["subject"]) for mm_ in new_mails):
        errors.append(f"Post {idx + 1}: LONG/SHORT-Signal, aber kein Eintrag in turbo_selections")

print(f"\nHaiku-Calls: {len(haiku_calls)}  Sonnet-Calls: {len(sonnet_calls)}")
if total_selector_rows == 0:
    errors.append("Turbo Selector lief bei keinem der Posts (kein Eintrag in turbo_selections)")
if errors:
    print("\n❌ SELBSTTEST FEHLGESCHLAGEN:", "; ".join(errors))
    sys.exit(1)
print("\n✅ SELBSTTEST OK")
