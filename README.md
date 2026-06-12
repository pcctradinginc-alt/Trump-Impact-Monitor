# 🚨 Trump-Impact Monitor v2.5

Stündliches automatisches Monitoring aller öffentlichen Trump-Verlautbarungen mit KI-gestützter Finanz-Impact-Analyse und Trade-Empfehlungen per E-Mail.
**Nur kostenlose Quellen – kein lokales Setup nötig – 100 % deploybar über die GitHub-Browser-UI.**

---

## ⚡ Quick Deploy (Browser only – 5 Schritte)

### Schritt 1 – GitHub-Repo erstellen

1. [github.com/new](https://github.com/new) öffnen
2. Name: `trump-impact-monitor`
3. Sichtbarkeit: **Private** ✅ (Hinweis: bei privaten Repos sind 2 000 Actions-Minuten/Monat frei – der Monitor ist darauf optimiert; bei **Public** sind Actions-Minuten unbegrenzt kostenlos)
4. **Create repository**

---

### Schritt 2 – Alle Dateien hochladen

Im neuen Repo **"uploading an existing file"** (oder **Add file → Upload files**):

```
trump-impact-monitor/
├── .github/
│   └── workflows/
│       └── trump-monitor.yml     ← muss exakt in diesem Pfad liegen!
├── alerts.db                     ← die leere mitgelieferte Datei
├── main.py
├── config.py
├── config.yml
├── requirements.txt
├── entities.json
└── init_db.py
```

> **Tipp für die Workflow-Datei:** Die Upload-UI legt keine Unterordner an.
> Stattdessen: **"Create new file"** → als Dateiname `.github/workflows/trump-monitor.yml` eingeben → Inhalt einfügen → **Commit**.

---

### Schritt 3 – GitHub Secrets anlegen

**Settings → Secrets and variables → Actions → New repository secret**

| Secret | Wert | Pflicht? |
|---|---|---|
| `ANTHROPIC_API_KEY` | `sk-ant-…` (Claude-Key) | ✅ |
| `GMAIL_EMAIL` | `deinname@gmail.com` | ✅ |
| `GMAIL_APP_PASSWORD` | 16-stelliges Gmail-App-Passwort (s.u.) | ✅ |
| `RECIPIENT_EMAIL` | Empfängeradresse für Alerts | ✅ |
| `SCRAPE_CREATORS_API_KEY` | Optionaler Fallback für Truth Social (kostenpflichtig) | ❌ optional |

**Gmail-App-Passwort erstellen:**
1. Google-Konto → Sicherheit → 2-Faktor-Authentifizierung (muss AN sein)
2. „App-Passwörter" suchen → „Mail" wählen → Name „GitHub Actions"
3. 16-Zeichen-Code kopieren (ohne Leerzeichen)

---

### Schritt 4 – Manuellen Testlauf starten

**Actions → Trump Impact Monitor → Run workflow → Run workflow**

In den Logs sollten die Quellen erscheinen; bei einem Treffer kommt eine E-Mail.

---

### Schritt 5 – Fertig ✅

Der Workflow läuft ab jetzt **automatisch jede volle Stunde** (plus 5 min vor NYSE-Öffnung/-Schluss).
Bei neuen Alerts erscheint ein Commit `chore: update alerts.db`.

---

## 📁 Datei-Übersicht

| Datei | Zweck |
|---|---|
| `main.py` | Kernlogik: fetch → Entity-Resolution → LLM → E-Mail |
| `config.yml` | Watchlist, Schwellenwerte, Quellen an/aus – ohne Code-Änderung anpassbar |
| `config.py` | Lädt config.yml als typisierte Konstanten |
| `entities.json` | Ticker → Keyword-Mappings (~7 000 Symbole, frei erweiterbar) |
| `.github/workflows/trump-monitor.yml` | GitHub-Actions-Cron-Job |
| `alerts.db` | SQLite-Deduplizierung (wird automatisch zurückcommittet) |
| `init_db.py` | Optional: DB lokal vorab erzeugen |

---

## 🔍 Funktionsweise

```
Jede Stunde (nur kostenlose Quellen):
  ┌─ Truth Social     trumpstruth.org RSS → CNN-Archiv → (optional ScrapeCreators)
  ├─ Finanz-News      CNBC, MarketWatch, Yahoo, Seeking Alpha, WSJ, Google News, Politico
  ├─ White House      news/, presidential-actions/, briefings-statements/ Feeds
  ├─ Federal Register Executive Orders & Proklamationen (offizielle API)
  ├─ SEC EDGAR        Trump Form 4 / 13D Insider-Filings (offizielle API)
  └─ OGE 278-T/278e   Periodic Transaction Reports (1× täglich Vollscan)
         │
         ▼
  Entity Resolution (entities.json, 3-Tier + ALL-CAPS-Schutz)
         │  Ticker gefunden? (sonst: Claude-Sektor-Inferenz)
         ▼
  Haiku-Pre-Screen (~$0.0002) → nur bei ACTIONABLE:
  Claude Sonnet: Sentiment · Magnitude · Trade-Richtung · Stop-Level
  + Trump-Interessenkonflikt inkl. Performance seit Trump-Kauf
         │
         ▼
  Turbo-Zertifikat: konkretes, in DE handelbares Papier (WKN/ISIN)
  via Vontobel-Produkt-API — Hebel ≈ 1/KO-Abstand nach Risikoprofil,
  Spread-Check ≤ 0.8 %, Preis 4–25 €, KO-Plausibilität geprüft;
  Fallback: Finder-Links mit manuellen Filterwerten
         │
         ▼
  Gmail-Alert (HTML) · SQLite-Dedup · 4h-Ticker-Cooldown · Tages-Cap
```

**Dedup-Schutz gegen Doppel-E-Mails:**
- SQLite-Hash pro (Ticker, Text) – über Runs hinweg via Repo-Commit
- 4-h-Cooldown pro Ticker (gleiche Story aus mehreren Medien = 1 Alert)
- Workflow-`concurrency`-Lock – nie zwei Läufe parallel
- `git pull --rebase` + Retry beim DB-Commit – kein Verlust des Dedup-Stands
- White-House-Feeds werden untereinander per Link dedupliziert

---

## 💰 Kosten

| Dienst | Kosten |
|---|---|
| Alle Datenquellen | **0 €** (trumpstruth.org, CNN-Archiv, RSS, Federal Register, EDGAR, OGE) |
| Anthropic Claude | Pay-per-use: Haiku-Screen ~$0.0002, Sonnet-Analyse ~$0.005/Alert, Tages-Cap 40 Calls |
| Gmail | 0 € (App-Passwort) |
| GitHub Actions | 0 € bei Public; bei Private ~2–3 min/Lauf → passt in die freien 2 000 min/Monat |
| ScrapeCreators | Optional, nur als letzter Truth-Social-Fallback |

---

## ✏️ entities.json erweitern

```json
"NVDA": {
  "symbol": ["NVDA"],
  "company": ["Nvidia", "Jensen Huang"],
  "weak": ["H100", "Blackwell"]
}
```

`symbol` = exaktes Tickersymbol, `company` = Firmenname/CEO (case-insensitiv),
`weak` = Produktnamen (matchen nur in Finanzkontext).
