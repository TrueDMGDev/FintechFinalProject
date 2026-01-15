# Financial News Scraper (Topic 59)

Desktop financial news app (Tkinter GUI) with RSS + crawl discovery, concurrent fetching, intelligent rate limiting + retry logic, keyword extraction/scoring, and deduplication.

## Quickstart (Windows)

### 1) Get the code

If you have Git installed:

```powershell
git clone https://github.com/TrueDMGDev/FintechFinalProject.git
cd FintechFinalProject
```

Or download the repo as a ZIP and extract it.

### 2) (Recommended) Create a virtual environment

This keeps project dependencies isolated (so you don't install packages globally).

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3) Install dependencies

```powershell
pip install -r requirements.txt
```

spaCy entity recognition is included in `requirements.txt` (including the `en_core_web_sm` model).

## Desktop GUI

Launch the desktop app (Live / Breaking / Saved):

```powershell
python -m fintech_news_scraper.gui_app
```

Alternative (also works):

```powershell
python fintech_news_scraper/gui_app.py
```

- **Live** continuously polls and shows in-memory results (does not auto-save).
- Toggle **Auto-save CSV** in the toolbar if you want CSV outputs regenerated.
- **Saved** stores articles locally in `data/saved.jsonl` when you click **Save**.

Outputs:
- `data/saved.jsonl` (your saved articles)
- CSVs (when **Auto-save CSV** is ON): `data/news_<source>.csv`

## Configuration

Edit `config/sources.yaml` to enable/disable sources and their RSS URLs.
Edit `config/config.yaml` for concurrency, rate limits, retry policy, and breaking-news thresholds.

Notes:
- `gui.auto_save_csv` controls the default state of the **Auto-save CSV** toggle.
