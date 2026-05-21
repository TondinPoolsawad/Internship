// scrapers/customs.js — uses built-in fetch (Node 18+), no undici
const fs = require('fs');
const path = require('path');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function* monthRange(start, end) {
  const [sy, sm] = start.split('-').map(Number);
  const [ey, em] = end.split('-').map(Number);
  let y = sy, m = sm;
  while (y < ey || (y === ey && m <= em)) {
    yield { year: y, month: m };
    if (++m > 12) { m = 1; y++; }
  }
}

async function fetchMonth({ hsCode, year, month, type }) {
  const url = 'https://www.customs.go.th/statistic_report.php?tab=by_statistic_code&s=mSvduqH0ktXplwFY';
  const body = new URLSearchParams({
    top_menu: 'menu_homepage', show_search: '1',
    tab: 'by_statistic_code', imex_type: type,
    tariff_code: hsCode, country_code: '',
    month: String(month), year: String(year),
  });

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': url, 'User-Agent': 'Mozilla/5.0',
    },
    body: body.toString(),
  });

  const html = await res.text();

  // Simple regex to find the last row total — avoids cheerio dependency issues in Electron
  const rowMatches = [...html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  let total = null;
  for (const row of rowMatches) {
    const cells = [...row[1].matchAll(/<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/gi)]
      .map(m => m[1].replace(/<[^>]+>/g, '').trim());
    if (!cells.length) continue;
    const label = (cells[0] || '').toLowerCase();
    if (label.includes('total') || label.includes('รวม')) continue;
    for (const idx of [1, 4, 2, 3]) {
      const raw = (cells[idx] || '').replace(/,/g, '');
      const num = parseFloat(raw);
      if (!isNaN(num) && num > 0) { total = (total || 0) + num; break; }
    }
  }
  return total;
}

process.on('message', async ({ config }) => {
  const { yearMonth, type, hsCodes, output } = config;
  const [startYM, endYM] = yearMonth;

  process.stdout.write(`Config: HS=${hsCodes.join(',')} | Types=${type.join(',')} | ${startYM}→${endYM}\n`);

  let csv = 'HS Code,Type,Year,Month,Quantity\n';

  for (const t of type) {
    for (const hs of hsCodes) {
      process.stdout.write(`\n↓ HS ${hs} (${t})\n`);
      for (const { year, month } of monthRange(startYM, endYM)) {
        const mm = String(month).padStart(2, '0');
        try {
          const q = await fetchMonth({ hsCode: hs, year, month, type: t });
          process.stdout.write(`  ${year}-${mm}: ${q != null ? q.toLocaleString() : 'N/A'}\n`);
          csv += `${hs},${t},${year},${month},${q ?? ''}\n`;
        } catch (e) {
          process.stdout.write(`  ${year}-${mm}: ERROR ${e.message}\n`);
          csv += `${hs},${t},${year},${month},\n`;
        }
        await sleep(350);
      }
    }
  }

  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, csv, 'utf8');
  process.stdout.write(`\n✅ Saved → ${output}\n`);
  process.exit(0);
});
