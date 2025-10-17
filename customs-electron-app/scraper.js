const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

function* monthRange(start, end) {
  const [sy, sm] = start.split('-').map(Number);
  const [ey, em] = end.split('-').map(Number);
  let y = sy, m = sm;
  while (y < ey || (y === ey && m <= em)) {
    yield { year: y, month: m };
    m++;
    if (m > 12) { m = 1; y++; }
  }
}

async function fetchMonthTotal({ hsCode, year, month, type }) {
  const url = 'https://www.customs.go.th/statistic_report.php?tab=by_statistic_code&s=mSvduqH0ktXplwFY';

  const body = new URLSearchParams({
    top_menu: 'menu_homepage',
    show_search: '1',
    tab: 'by_statistic_code',
    imex_type: type,            // 'import' | 'export'
    tariff_code: hsCode,        // HS code
    country_code: '',
    month: String(month),
    year: String(year)
  });

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': url,
      'User-Agent': 'Mozilla/5.0'
    },
    body
  });

  const html = await res.text();
  const $ = cheerio.load(html);

  // Try to find the last row (total). Adjust selector if site changes.
  const $rows = $('div.table-responsive tbody tr');
  if ($rows.length === 0) return null;

  // Heuristic: the last row often contains totals.
  const cells = $rows.last().find('th,td').map((_, el) => $(el).text().trim()).get();

  // Try multiple candidate columns for "quantity"
  // Adjust if schema differs (index may change).
  const candidates = [1, 4, 2, 3];
  for (const idx of candidates) {
    const raw = cells[idx];
    if (!raw) continue;
    const num = parseFloat(String(raw).replace(/,/g, ''));
    if (!Number.isNaN(num)) return num;
  }
  return null;
}

async function runScraper(configPathOrObj) {
  const cfg = typeof configPathOrObj === 'string'
    ? JSON.parse(fs.readFileSync(configPathOrObj, 'utf8'))
    : configPathOrObj;

  const { yearMonth, type, hsCodes, output } = cfg;
  const [startYM, endYM] = yearMonth;

  let csv = 'HS Code,Type,Year,Month,Quantity\n';

  for (const t of type) {
    for (const hs of hsCodes) {
      for (const { year, month } of monthRange(startYM, endYM)) {
        const q = await fetchMonthTotal({ hsCode: hs, year, month, type: t });
        csv += `${hs},${t},${year},${month},${q ?? ''}\n`;
        await sleep(400);
      }
    }
  }

  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, csv, 'utf8');
}

async function runScraperWithLogger(configPathOrObj, logger = () => {}) {
  const cfg = typeof configPathOrObj === 'string'
    ? JSON.parse(fs.readFileSync(configPathOrObj, 'utf8'))
    : configPathOrObj;

  const { yearMonth, type, hsCodes, output } = cfg;
  const [startYM, endYM] = yearMonth;

  logger(`Config loaded: HS=${hsCodes.join(', ')} | Types=${type.join(', ')} | Range=${startYM}..${endYM}`);

  let csv = 'HS Code,Type,Year,Month,Quantity\n';
  let count = 0;

  for (const t of type) {
    for (const hs of hsCodes) {
      logger(`\nHS ${hs} (${t})`);
      for (const { year, month } of monthRange(startYM, endYM)) {
        logger(`  Fetching ${year}-${String(month).padStart(2,'0')}...`);
        const q = await fetchMonthTotal({ hsCode: hs, year, month, type: t });
        if (q != null) {
          logger(`   → ${q.toLocaleString()}`);
        } else {
          logger('   → not found / N/A');
        }
        csv += `${hs},${t},${year},${month},${q ?? ''}\n`;
        count++;
        await sleep(300);
      }
    }
  }

  fs.mkdirSync(path.dirname(output), { recursive: true });
  fs.writeFileSync(output, csv, 'utf8');
  logger(`\nSaved ${count} rows → ${output}`);
}

module.exports = { runScraper, runScraperWithLogger };
