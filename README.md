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

Der Workflow läuft ab jetzt **automatisch alle 10 Minuten** (Minute 7, 17, 27, … 57; nicht :00 weil GitHub die :00-Schedules unter Last verzögert).
Plus zwei NYSE-Läufe (5 min vor Öffnung/Schluss, Mo-Fr).
Das Repo ist Public → Actions-Minuten kostenlos (unbegrenzt).
Das Zeitfenster erweitert sich automatisch zurück zum letzten erfolgreichen Lauf (max. 24h) – kein Post wird übersehen.
Bei neuen Alerts erscheint ein Commit `chore: update alerts.db`.

---

## 📁 Datei-Übersicht

| Datei | Zweck |
|---|---|
| `main.py` | Kernlogik: fetch → Entity-Resolution → LLM → E-Mail |
| `turbo_selector.py` | "Trump Post → Turbo Selector DE": Signal → Marktbestätigung → Produktsuche → Risiko/Score → ACTIONABLE/WATCH/NO_TRADE |
| `config.yml` | Watchlist, Schwellenwerte, Quellen an/aus – ohne Code-Änderung anpassbar |
| `config.py` | Lädt config.yml als typisierte Konstanten |
| `entities.json` | Ticker → Keyword-Mappings (~7 000 Symbole, frei erweiterbar) |
| `.github/workflows/trump-monitor.yml` | GitHub-Actions-Cron-Job |
| `alerts.db` | SQLite-Deduplizierung (wird automatisch zurückcommittet) |
| `init_db.py` | Optional: DB lokal vorab erzeugen |

---

## 🔍 Funktionsweise

```
Alle 10 Minuten (nur kostenlose Quellen):
  ┌─ Truth Social     trumpstruth.org RSS → CNN-Archiv → (optional ScrapeCreators)
  ├─ Finanz-News      CNBC, MarketWatch, Yahoo, Seeking Alpha, WSJ, Google News, CNBC Politics, The Hill, Investing.com
  ├─ White House      news/, presidential-actions/, briefings-statements/ Feeds; USTR press releases; YouTube channel
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
  Turbo Selector DE (turbo_selector.py) — siehe eigener Abschnitt unten;
  Fallback bei Fehlern: alte Parameter-Heuristik (turbo_recommendation)
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

## 🎯 Turbo Selector DE ("Trump Post → Turbo Selector DE")

`turbo_selector.py` ersetzt die alte Parameter-Heuristik durch eine
analysebasierte Pipeline, die für jedes Sonnet-Signal höchstens EIN
Turbo-Zertifikat empfiehlt oder explizit **NO TRADE** sagt. Reine
Analyse — keine Order-Ausführung, keine Broker-Anbindung.

```
Trump-Post
   │  (Sonnet liefert zusätzlich zum bisherigen Format:
   │   HORIZON_DAYS, EXPECTED_MOVE_PCT, SIGNAL_CONFIDENCE_PCT,
   │   UNCERTAINTY, MACRO_UNDERLYING/-DIRECTION, RATIONALE)
   ▼
MarketSignal (Aktie, optional zusätzlich ein Makro-Signal:
   DAX/S&P 500/Nasdaq 100/EUR-USD/Gold/Brent/WTI)
   ▼
Marktbestätigung — NO TRADE wenn:
   • bereits eingepreist (Reaktion seit Post ≥ 70 % der erwarteten Bewegung)
   • widersprüchlich: Markt seit Post ≥ 50 % der erwarteten Bewegung GEGEN das
     Signal gelaufen, oder 20-Tage-Gegentrend > 10 % und auch 5 Tage noch dagegen
   ▼
Produktsuche: onvista Derivate-Finder (aggregiert SG, Goldman, JPM, BNP,
HSBC, Morgan Stanley, UBS, UniCredit, Vontobel, … hinter einem Endpunkt),
serverseitig Open End + je Hebel-Band (2–4 … 16–20) abgefragt
+ Vontobel-eigene API als zusätzliche Emittenten-Quelle
   ▼
Harte Filter: echte Zweiwege-Quote (0 < Bid < Ask), Spread ≤ Limit, KO auf der richtigen Seite
und weit genug weg (Vol-abhängig), Hebel ≤ Limit, Kurs frisch genug
   ▼
Je Kandidat: Monte-Carlo (Bootstrap der Tages-Log-Renditen aus ~1 Jahr
Historie, Signal-Drift aufgesetzt) + Brownsche-Brücke-Korrektur für
Intraday-KO-Berührungen → P(KO), Expected Net Return, konservative
Rendite (Signal-Drift um Konfidenz × Unsicherheit geschrumpft)
   ▼
Score = (Expected Net Return − Spread-Kosten − Finanzierungskosten) / Hebel
        − KO_PENALTY·P(KO) − UNCERTAINTY_PENALTY·Unsicherheit
   = Effizienz je Einheit Basiswert-Exposure (score_per_exposure: true), damit
     nicht automatisch der höchste Hebel gewinnt. Empfohlen werden nur Produkte
     mit konservativer Rendite > 0 (erst filtern, dann ranken).
   ▼
Bestes Produkt (+ Median der Vergleichbaren, beste Alternative eines
anderen Emittenten) → ACTIONABLE / WATCH / NO_TRADE, deutscher HTML-Block
in der bestehenden Alert-Mail; Persistenz in SQLite-Tabelle
`turbo_selections` (main.py legt sie wie die anderen Tabellen per
CREATE TABLE IF NOT EXISTS an).
```

**Wichtige config.yml-Schlüssel (Abschnitt `turbo_selector:`):**

| Schlüssel | Bedeutung |
|---|---|
| `horizons_days` | erlaubte Horizonte, muss zu Sonnets `HORIZON_DAYS` passen |
| `priced_in_fraction` | Schwelle für "bereits eingepreist" |
| `contradiction_threshold`, `against_reaction_fraction` | Schwellen für "Signal widersprüchlich" (Gegentrend / Gegenreaktion seit Post) |
| `max_spread_pct`, `max_leverage`, `min_ko_distance_pct`, `min_ko_distance_vol_mult` | harte Produktfilter |
| `freshness_minutes`, `stale_relax_factor` | Kursfrische während/außerhalb der Handelszeit (grobe Xetra-Heuristik Mo–Fr 07–21 UTC) |
| `n_paths`, `mc_seed` | Monte-Carlo-Parameter |
| `financing_reference_rate`, `financing_issuer_spread`, `short_financing_is_credit` | vereinfachtes Finanzierungskosten-Modell |
| `uncertainty_shrink`, `conservative_ci_percentile` | Konfidenz-Schrumpfung für die konservative Rendite |
| `score_per_exposure`, `ko_penalty`, `uncertainty_penalty`, `spread_cost_weight`, `financing_cost_weight` | Score-Formel und -Gewichte |
| `actionable_confidence_min` | ab welcher Konfidenz ACTIONABLE statt WATCH möglich ist |
| `send_no_trade_alerts` | ob NO-TRADE-Ergebnisse des Selectors trotzdem gemailt werden (klar markiert) |

**Bekannte Grenzen (bewusste Vereinfachungen, dokumentiert im Code):**
- Vontobels kostenlose API liefert keinen Ask-Preis → deren Kandidaten
  fallen praktisch immer durch den `ask > 0`-Filter; onvista bleibt die
  einzig wirklich nutzbare Quelle (aggregiert aber bereits 7+ Emittenten).
- Börse Stuttgart/Frankfurt: kein workables kostenloses Keyless-JSON
  gefunden (403/405 auf einfache GET-Requests) — nicht integriert.
- FX-Effekt (USD-Basiswert, EUR-Produkt) wird nur als unabhängiger
  Bootstrap-Overlay auf EUR/USD angenähert, nicht gemeinsam simuliert.
- Die konservative Rendite nutzt NUR die konfidenz-/unsicherheits-
  geschrumpfte Simulation, nicht zusätzlich das 5 %-Quantil der
  Basissimulation als hartes Gate — bei den üblichen 5–15×-Hebeln ist
  dieses Quantil strukturell fast immer nahe −100 % und hätte jede
  Empfehlung verhindert. Es wird trotzdem berechnet und als Risikohinweis
  ausgegeben ("Ungünstiges Szenario (5%-Quantil)").
- Finanzierungskosten sind ein grobes p.a.-Modell (Referenzzins +
  Emittenten-Aufschlag, linear auf den Horizont skaliert), keine
  produktgenaue Nachbildung des tatsächlich drifting KO/Strike-Levels.

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
