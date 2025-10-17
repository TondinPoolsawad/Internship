# Customs Scraper (Electron GUI)

A minimal Electron app to scrape Thai Customs monthly totals and export to CSV.

## Requirements
- Node.js 18+
- Windows/macOS/Linux

## Install & Run (Dev)
```bash
npm install
npm start
```

## Build installers
```bash
# Windows .exe installer
npm run build:win

# macOS app
npm run build:mac

# Linux AppImage
npm run build:linux
```

## Notes
- Network requests happen in the **main process**.
- CSV is saved via **Save As...** dialog.
- If the website structure changes, selector logic may need updates in `scraper.js`.
