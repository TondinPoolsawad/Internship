const fs = require("fs");
const cheerio = require("cheerio");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchCustomsData({ hsCode, year, month, type }) {
  const url = "https://www.customs.go.th/statistic_report.php?tab=by_statistic_code&s=mSvduqH0ktXplwFY";

  async function getMonthData(m) {
    const body = new URLSearchParams({
      top_menu: "menu_homepage",
      show_search: "1",
      tab: "by_statistic_code",
      imex_type: type,
      tariff_code: hsCode,
      country_code: "",
      month: m,
      year
    });

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": url,
        "User-Agent": "Mozilla/5.0"
      },
      body
    });

    const html = await res.text();
    const $ = cheerio.load(html);
    const sumRow = $("div.table-responsive tbody tr").last().find("th, td").map((_, el) => $(el).text().trim()).get();
    const quantitySum = sumRow[1] || sumRow[4] || null;
    return quantitySum ? parseFloat(quantitySum.replace(/,/g, "")) : null;
  }

  const q = await getMonthData(month);
  await sleep(800);
  return q;
}

function* monthRange(start, end) {
  const [startYear, startMonth] = start.split("-").map(Number);
  const [endYear, endMonth] = end.split("-").map(Number);
  let year = startYear, month = startMonth;
  while (year < endYear || (year === endYear && month <= endMonth)) {
    yield { year, month };
    month++;
    if (month > 12) {
      month = 1;
      year++;
    }
  }
}

async function main() {
  const config = JSON.parse(fs.readFileSync("config.json", "utf8"));
  const { yearMonth, type, hsCodes, output } = config;
  const [startYM, endYM] = yearMonth;

  let csv = "HS Code,Type,Year,Month,Quantity\n";

  for (const t of type) {
    for (const hs of hsCodes) {
      console.log(`\nFetching HS ${hs} (${t}) from ${startYM} to ${endYM}`);
      for (const { year, month } of monthRange(startYM, endYM)) {
        const q = await fetchCustomsData({ hsCode: hs, year, month, type: t });
        if (q) {
          console.log(`  ${year}-${String(month).padStart(2, "0")}: ${q.toLocaleString()}`);
          csv += `${hs},${t},${year},${month},${q}\n`;
        } else {
          console.log(`  ${year}-${String(month).padStart(2, "0")}: not found`);
          csv += `${hs},${t},${year},${month},N/A\n`;
        }
      }
    }
  }

  fs.mkdirSync(require("path").dirname(output), { recursive: true });
  fs.writeFileSync(output, csv, "utf8");
  console.log(`\n Data saved to ${output}`);
}

main();
