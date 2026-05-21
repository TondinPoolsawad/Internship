# GDIAS Electron App

Desktop scraper UI for all 4 government data sources.

## Setup

```bash
npm install
npm start
```

## Requirements

- Node.js 18+
- Python 3.10+ (for OAE, Fisheries, DEDE tabs)
- `pip install supabase pandas openpyxl requests`

## Folder structure

```
gdias-electron/
├── main.js          # Electron main process
├── preload.js       # Context bridge
├── index.html       # UI
├── renderer.js      # UI logic
├── package.json
├── scrapers/        # IPC scraper wrappers
│   ├── customs.js
│   ├── oae.js
│   ├── fisheries.js
│   └── dede.js
└── scripts/         # Your actual scraper files (copy here)
    ├── oae_api.py
    ├── fisheries_api.py
    ├── scraper.js   (DEDE downloader)
    └── cleandata.py (DEDE parser)
```

## How to use

1. Copy your scraper files into the `scripts/` folder
2. `npm install`
3. `npm start`
4. Pick a tab, fill in parameters, choose output file, click Run

## Build as .exe / .dmg

```bash
npm run build
```

Output will be in the `dist/` folder.
