# GDIAS — Government Data Integration and Analysis System

NECTEC Internship 2025 · Nattakit Chantara-aree & Tondin Poolsawad

Get all CSV files here! https://nectec-internship2025.vercel.app/

A data pipeline that scrapes Thai government sources and displays DMC (Domestic Material Consumption) data on a public website, with a local Electron desktop app for running scrapers.

---

## How it works

```
Scrapers (run locally or via Electron app) → CSV → Supabase Storage → Website (Vercel)
```

Four government sources are scraped, cleaned into CSV, uploaded to Supabase, and displayed on the website with a download button per source.

---

## Data Sources

| Source | Method | Output |
|---|---|---|
| Thai Customs (กรมศุลกากร) | Node.js · POST scrape + Cheerio | `customs.csv` |
| OAE (สำนักงานเศรษฐกิจการเกษตร) | Python · CKAN API | `oae.csv` |
| Fisheries (กรมประมง) | Python · CKAN API | `fisheries.csv` |
| DEDE (กรมพัฒนาพลังงานทดแทน) | Node.js · Puppeteer + XLSX (2015–2024) / PDF (2025+) | `dede.csv` |

---

## Project Structure

```
Internship/
├── index.html                  # Website (deployed on Vercel)
├── README.md                   # This file
├── .gitignore
├── scraper.js                  # DEDE downloader (Puppeteer)
├── cleandata.py                # DEDE XLSX → CSV cleaner
├── oae_api.py                  # OAE CKAN scraper
├── fisheries_api.py            # Fisheries CKAN scraper
├── customs_scrape.js           # Customs scraper (Node.js)
├── config.json                 # Shared config
├── package.json
├── downloads/                  # Downloaded XLSX files + manifest
├── fisheries_importexport_stat_raw/  # Raw fisheries data
└── gdias-electron/             # Electron desktop app
    ├── main.js                 # Electron main process
    ├── preload.js
    ├── renderer.js
    ├── index.html              # App UI
    ├── package.json
    └── scrapers/
        └── dede.js             # Spawns scraper.js + cleandata.py
```

---

## Electron Desktop App

The `gdias-electron/` folder is a standalone Electron app that provides a UI for running scrapers without using the command line.

### Requirements

- Node.js 18+ (system install, not Electron's bundled Node)
- Python 3.10+

### Run in development

```powershell
cd gdias-electron
npm install
npm start
```

### Build as `.exe`

```powershell
cd gdias-electron
npm run build
```

Output will be in `gdias-electron/dist/`.

> ⚠️ Do not push `dist/` to GitHub — it's excluded in `.gitignore`.

---

## Setup (CLI scripts)

### Requirements

- Node.js 18+
- Python 3.10+
- `pip install supabase pandas openpyxl`
- `npm install puppeteer @supabase/supabase-js undici`

### Environment variable

Set this before running any scraper — never hardcode it:

**Windows (PowerShell):**
```powershell
$env:SUPABASE_SERVICE_KEY = "your-secret-key-here"
```

**Mac/Linux:**
```bash
export SUPABASE_SERVICE_KEY="your-secret-key-here"
```

Get the key from: Supabase dashboard → Project Settings → API Keys → Secret key

---

## Supabase Storage

- Project URL: `https://mzzyjtlrbbwqdxpenwne.supabase.co`
- Bucket: `csvs` (public)
- Files: `customs.csv`, `oae.csv`, `fisheries.csv`, `dede.csv`

### Upload snippet — Python (OAE + Fisheries)

Add at the end of `oae_api.py` and `fisheries_api.py`:

```python
import os
from supabase import create_client

def upload_csv_to_supabase(local_path: str, bucket_filename: str):
    client = create_client(
        "https://mzzyjtlrbbwqdxpenwne.supabase.co",
        os.environ["SUPABASE_SERVICE_KEY"]
    )
    with open(local_path, "rb") as f:
        data = f.read()
    client.storage.from_("csvs").remove([bucket_filename])
    client.storage.from_("csvs").upload(
        path=bucket_filename,
        file=data,
        file_options={"content-type": "text/csv"}
    )
    print(f"✅ Uploaded → csvs/{bucket_filename}")

# upload_csv_to_supabase("output/oae.csv", "oae.csv")
# upload_csv_to_supabase("output/fisheries.csv", "fisheries.csv")
```

### Upload snippet — Node.js (Customs + DEDE)

Add at the end of `scraper.js`:

```js
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "node:fs";

async function uploadCsvToSupabase(localPath, bucketFilename) {
  const client = createClient(
    "https://mzzyjtlrbbwqdxpenwne.supabase.co",
    process.env.SUPABASE_SERVICE_KEY
  );
  const data = readFileSync(localPath);
  await client.storage.from("csvs").remove([bucketFilename]);
  const { error } = await client.storage.from("csvs").upload(bucketFilename, data, {
    contentType: "text/csv",
  });
  if (error) throw error;
  console.log(`✅ Uploaded → csvs/${bucketFilename}`);
}

// await uploadCsvToSupabase("downloads/customs.csv", "customs.csv");
// await uploadCsvToSupabase("downloads/dede.csv", "dede.csv");
```

---

## Website

Deployed on Vercel. Fetches the 4 CSVs from Supabase on load and renders them as tables with download buttons.

To update the data: just run the scrapers. The website always shows whatever is currently in Supabase.

---

## Advisors

- นายเฉลิมพล ชาญศรีภิญโญ  
- นายฤทธิ์ณรงค์ พรมยา
