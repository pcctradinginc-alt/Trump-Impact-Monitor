"""
Rückblick-Lauf: schickt die echten Trump-Posts der letzten N Stunden durch die
aktuelle Statement-first-Pipeline (Haiku-Triage → Sonnet → Turbo Selector DE).

Zweck: messen, wie viele ACTIONABLE/WATCH-Empfehlungen die Logik auf echten
Posts liefert (Schwellen-Kalibrierung), und verpasste Posts nachholen.

Einzel-Mails werden NICHT verschickt. Mit REPLAY_SEND_MAIL=1 geht am Ende EINE
Sammel-Mail mit allen ACTIONABLE/WATCH-Empfehlungen raus (klar als Rückblick
markiert). Kein Cooldown, kein Tagesbudget. Der Workflow replay.yml committet
alerts.db nicht — der Dedup-Stand der Dauerschleife bleibt unberührt.

Umgebungsvariablen: REPLAY_HOURS (Default 72), REPLAY_SEND_MAIL (0/1).
"""
import html
import os
import re
import sys
from datetime import timedelta

import main as m

HOURS = int(os.getenv("REPLAY_HOURS", "72"))
SEND_MAIL = os.getenv("REPLAY_SEND_MAIL", "0") == "1"
MAX_UNDERLYINGS_PER_POST = 2

mails: list[dict] = []
_real_send_gmail = m.send_gmail
calls = {"haiku": 0, "sonnet": 0}
_orig_create = m.client.messages.create


def _count_create(**kwargs):
    calls["sonnet" if kwargs.get("model") == m.MODEL else "haiku"] += 1
    return _orig_create(**kwargs)


def _collect_mail(subject: str, html_body: str) -> bool:
    mails.append({"subject": subject, "body": html_body})
    return True


m.client.messages.create = _count_create
m.send_gmail = _collect_mail
m._rate_limit_ok = lambda ticker: True
m._rate_limit_record = lambda ticker: None
m.CUTOFF = m.now_utc() - timedelta(hours=HOURS)

rows: list[dict] = []
posts = m.fetch_truth_social()
n_window = 0
for post in posts:
    text = m.clean_text(post.get("text", post.get("content", "")))
    if not text or (text.startswith("RT @") and not m.INCLUDE_RETWEETS):
        continue
    ts = post.get("created_at", post.get("published"))
    if not m.is_recent(ts):
        continue
    n_window += 1
    url = post.get("url", post.get("uri", "https://truthsocial.com/@realDonaldTrump"))
    tri = m.triage_post("Truth Social", url, text)
    if tri is None:
        rows.append({"ts": ts, "text": text, "status": "TRIAGE-FEHLER", "detail": ""})
        continue
    if not tri["relevant"]:
        continue
    unders = m.pick_underlyings(tri["underlyings"], MAX_UNDERLYINGS_PER_POST)
    macros_here = tuple(u["symbol"] for u in unders if u["kind"] == "MACRO")
    for u in unders:
        n_before = len(mails)
        m.analyze_and_alert(
            "Truth Social", ts, text, u["symbol"], url,
            "claude" if u["kind"] == "SECTOR" else "hoch",
            kind=u["kind"], name=u.get("name") or None,
            hint_direction=u.get("direction"), skip_macros=macros_here,
        )
        new = mails[n_before:]
        if new:
            subj = new[0]["subject"]
            dec = re.search(r"\[([^\]]+)\]", subj)
            rows.append({"ts": ts, "text": text, "status": dec.group(1) if dec else "MAIL",
                         "detail": subj, "mail": new[0]})
        else:
            sel = m.conn.execute(
                "SELECT decision FROM turbo_selections WHERE trump_post_id=? AND underlying=? "
                "ORDER BY id DESC LIMIT 1", (url, u["symbol"].upper())).fetchone()
            rows.append({"ts": ts, "text": text,
                         "status": "kein Alert" + (f" (Selector: {sel[0]})" if sel else " (Sonnet-Gate)"),
                         "detail": f"{u['symbol']} {u.get('direction')} [{u['kind']}]"})

print(f"\n{'═' * 78}\nRÜCKBLICK {HOURS} h: {n_window} Posts im Fenster, "
      f"{len({r['text'] for r in rows})} marktrelevant, "
      f"{len(mails)} Empfehlung(en) (ACTIONABLE/WATCH)\n{'═' * 78}")
for r in rows:
    print(f"\n{str(r['ts'])[:25]}  →  {r['status']}")
    print(f"   Post:  {r['text'][:170]}")
    print(f"   {r['detail']}")
    if "mail" in r:
        mm = re.search(r"Kurzfazit.*?</div>", r["mail"]["body"], re.S)
        if mm:
            kurz = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", mm.group(0)))).strip()
            print(f"   {kurz[:600]}")
print(f"\nHaiku-Calls: {calls['haiku']}  Sonnet-Calls: {calls['sonnet']}")

if SEND_MAIL and mails:
    parts = [
        '<div style="font-family:-apple-system,Segoe UI,sans-serif;max-width:640px;margin:0 auto;">',
        f'<h2 style="margin:0 0 4px;">Trump-Monitor – Rückblick {HOURS} h</h2>',
        f'<p style="color:#6e6e73;font-size:13px;margin:0 0 16px;">{len(mails)} Empfehlung(en) aus '
        f'{n_window} Posts. Rückblick-Lauf: ältere Signale können bereits gelaufen sein – '
        'aktuellen Kurs und KO-Abstand vor dem Kauf prüfen.</p>',
    ]
    for r in rows:
        if "mail" not in r:
            continue
        mm = re.search(r"<div[^>]*>\s*(?:<[^>]+>\s*)*Kurzfazit.*?</div>", r["mail"]["body"], re.S)
        parts.append(f'<h3 style="font-size:14px;margin:18px 0 6px;">{html.escape(r["detail"])}</h3>')
        parts.append(f'<p style="font-size:12px;color:#6e6e73;margin:0 0 6px;">{html.escape(str(r["ts"])[:25])} · '
                     f'{html.escape(r["text"][:260])}</p>')
        parts.append(mm.group(0) if mm else "")
    parts.append("</div>")
    ok = _real_send_gmail(f"📋 Trump-Monitor – Rückblick {HOURS} h: {len(mails)} Empfehlung(en)", "".join(parts))
    print("Sammel-Mail gesendet" if ok else "Sammel-Mail FEHLGESCHLAGEN")
    if not ok:
        sys.exit(1)
