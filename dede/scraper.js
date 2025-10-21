// labubu.js (ESM, Node >= 18)
// Annual-only DEDE XLSX downloader w/ Puppeteer rendering.
// - Skips monthly/half-year pages
// - Prefers "Physical" on year pages
// - Saves original filename under downloads/<product>/<year>/...
// - TLS relaxed only for dede.go.th

import fs from "node:fs";
import path from "node:path";
import puppeteer from "puppeteer";
import { fetch as ufetch, Agent } from "undici";

// ===== CONFIG =====
const CONFIG = {
  mainUrl: "https://www.dede.go.th/articles?id=452&menu_id=1",
  outputDir: "downloads",
  productName: "energy_balance",
  throttleMs: 600,
  minYear: 2010,
  preferPhysicalOnly: true,
  DEBUG: true,
  forceDownload: false, // set true for one run to re-place files despite manifest
  extraHeaders: {
    // "accept-language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    // "cookie": "_ga=...; _ga_0EWWK0LWX6=..."
  },
  headless: "new" // set false to watch Chromium
};

// ===== TLS agent ONLY for dede.go.th (for file downloads) =====
const insecureAgent = new Agent({ connect: { rejectUnauthorized: false } });
function needsInsecure(url) {
  const host = new URL(url).hostname.toLowerCase();
  return host.endsWith("dede.go.th") || host.endsWith("www.dede.go.th");
}
function myFetch(url, opts = {}) {
  return ufetch(url, needsInsecure(url) ? { ...opts, dispatcher: insecureAgent } : opts);
}

// ===== utils =====
const ROOT_DIR = path.join(process.cwd(), CONFIG.outputDir);
const MANIFEST = path.join(ROOT_DIR, "manifest.json");
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const sanitizeFilename = (s = "") => s.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, " ").trim();

const MONTHS_EN = ["january","february","march","april","may","june","july","august","september","october","november","december"];
const MONTHS_TH = ["มกราคม","กุมภาพันธ์","มีนาคม","เมษายน","พฤษภาคม","มิถุนายน","กรกฎาคม","สิงหาคม","กันยายน","ตุลาคม","พฤศจิกายน","ธันวาคม"];
const MONTH_WORDS = [...MONTHS_EN, ...MONTHS_TH];
const MONTH_RE = new RegExp(MONTH_WORDS.join("|"), "i");
const ANNUAL_SPAN_RE = /(january[^a-z]*december|jan[^a-z]*dec|มกราคม[^ก-๙]*ธันวาคม)/i;
const HALF_SPAN_RE   = /(january[^a-z]*june|jan[^a-z]*jun|ครึ่งปี|มกราคม[^ก-๙]*มิถุนายน)/i;

function decodeBaseNameFromHref(href) {
  try {
    const u = new URL(href);
    return decodeURIComponent((u.pathname.split("/").pop() || ""));
  } catch { return href; }
}

function smartYearFrom(text = "", href = "", neighborText = "") {
  const filename = decodeBaseNameFromHref(href).toLowerCase();
  const sources = [filename, String(text).toLowerCase(), String(neighborText).toLowerCase()];
  const entries = [];
  for (const src of sources) {
    const re = /(20\d{2}|25\d{2})/g;
    let m;
    while ((m = re.exec(src))) {
      let y = parseInt(m[1], 10);
      if (y >= 2500) y -= 543; // Thai → Gregorian
      entries.push({ y, idx: m.index, src });
    }
  }
  if (!entries.length) return null;

  // Prefer a year near a Jan–Dec span (e.g., "...January_December_2024...")
  const nearMonthSpan = entries.filter(e => {
    const seg = e.src.slice(Math.max(0, e.idx - 40), e.idx + 40);
    return ANNUAL_SPAN_RE.test(seg);
  });
  if (nearMonthSpan.length) return Math.min(...nearMonthSpan.map(e => e.y));

  // Else prefer smallest from filename (often the true report year vs updated-at)
  const fileHits = entries.filter(e => filename.includes(e.src));
  if (fileHits.length) return Math.min(...fileHits.map(e => e.y));

  // Fallback: smallest overall
  return Math.min(...entries.map(e => e.y));
}

/** Classify period using proximity to chosen report year and overall context.
 *  Returns "annual" | "half" | "monthly" | "unknown".
 */
function classifyPeriod({ text = "", href = "", neighbor = "", year }) {
  const filename = decodeBaseNameFromHref(href).toLowerCase();
  const s = (filename + " " + String(text).toLowerCase() + " " + String(neighbor).toLowerCase());

  // 1) Explicit spans
  if (ANNUAL_SPAN_RE.test(s)) return "annual";
  if (HALF_SPAN_RE.test(s))   return "half";

  // 2) Count distinct years present (handles e.g. "...2023...October...2024...")
  const years = new Set();
  (s.match(/20\d{2}/g) || []).forEach(y => years.add(Number(y)));
  (s.match(/25\d{2}/g) || []).forEach(y => years.add(Number(y) - 543));

  // 3) If month words AND only ONE distinct year near the chosen year → monthly
  if (MONTH_RE.test(s) && years.size <= 1 && typeof year === "number") {
    const idx = s.indexOf(String(year));
    if (idx !== -1) {
      const seg = s.slice(Math.max(0, idx - 25), idx + 25);
      if (MONTH_RE.test(seg) && !/december|ธันวาคม/.test(seg)) {
        return "monthly";
      }
    }
  }

  // 4) Strong annual keywords
  if (/energy[_\s-]?balance|energy[_\s-]?commodity[_\s-]?account|ดุลยภาพพลังงาน/i.test(s)) return "annual";

  // 5) No month words → annual-ish
  if (!MONTH_RE.test(s)) return "annual";

  return "unknown";
}

function loadManifest() { try { return JSON.parse(fs.readFileSync(MANIFEST, "utf8")); } catch { return { items: {} }; } }
function saveManifest(m) { fs.mkdirSync(ROOT_DIR, { recursive: true }); fs.writeFileSync(MANIFEST, JSON.stringify(m, null, 2), "utf8"); }

async function getBuffer(url, referer) {
  const headers = { "User-Agent": "Mozilla/5.0", ...(referer ? { Referer: referer } : {}), ...CONFIG.extraHeaders };
  const res = await myFetch(url, { headers });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return Buffer.from(await res.arrayBuffer());
}

async function downloadIfNew(item, product) {
  let year = item.year ?? smartYearFrom(item.text || "", item.href || "", item.neighbor || "") ?? "unknown";
  if (typeof year === "number" && year < CONFIG.minYear) {
    if (CONFIG.DEBUG) console.log(`  skip (year < minYear):`, year, item.href);
    return;
  }

  // Save with ORIGINAL filename
  const originalName = (() => {
    try { return decodeURIComponent(new URL(item.href).pathname.split("/").pop() || "file.xlsx"); }
    catch { return "file.xlsx"; }
  })();

  const outDir = path.join(ROOT_DIR, product, String(year));
  const outPath = path.join(outDir, sanitizeFilename(originalName));

  // Skip if we've already saved a file to this exact path (prevents same-year spam)
  if (fs.existsSync(outPath) && !CONFIG.forceDownload) {
    if (CONFIG.DEBUG) console.log(`✓ file exists: ${outPath}`);
    // still record manifest per-URL for traceability
  }

  const manifest = loadManifest();
  const key = `${product}::${item.href}`;
  const already = manifest.items[key]?.saved_path;

  if (!CONFIG.forceDownload && already && fs.existsSync(already)) {
    if (CONFIG.DEBUG) console.log(`✓ already by URL: ${item.text || item.href}`);
    return;
  }

  console.log(`↓ downloading: ${item.text || originalName}`);
  const buf = await getBuffer(item.href, CONFIG.mainUrl);
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPath, buf);

  const year_en = typeof year === "number" ? year : null;
  const year_th = year_en ? year_en + 543 : null;

  manifest.items[key] = {
    title: item.text || "",
    href: item.href,
    saved_path: outPath,
    year_en, year_th,
    saved_at: new Date().toISOString()
  };
  saveManifest(manifest);
  console.log(`  saved -> ${outPath}`);
}

// ===== Puppeteer helpers =====
async function withPage(fn) {
  const browser = await puppeteer.launch({
    headless: CONFIG.headless,
    args: ["--lang=th-TH,th,en-US,en"]
  });
  try {
    const page = await browser.newPage();
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36");
    await page.setExtraHTTPHeaders({ ...CONFIG.extraHeaders });
    const out = await fn(page);
    await browser.close();
    return out;
  } catch (e) {
    try { await browser.close(); } catch {}
    throw e;
  }
}

function pickContentSelector() {
  return '#box_wrapper section.ls.section_padding_top_50.section_padding_bottom_50 article';
}

async function renderCollectMain() {
  return withPage(async (page) => {
    await page.goto(CONFIG.mainUrl, { waitUntil: "networkidle0", timeout: 60000 });

    const contentSel = pickContentSelector();
    await page.waitForFunction((sel) => {
      const content = document.querySelector(sel);
      const arr = Array.from((content || document).querySelectorAll('a[href]'));
      return arr.some(a => a.href.includes("/uploads/") || a.href.includes("articles?id="));
    }, { timeout: 15000 }, contentSel).catch(() => {});

    if (CONFIG.DEBUG) {
      const html = await page.content();
      fs.mkdirSync("debug", { recursive: true });
      fs.writeFileSync("debug/main_rendered.html", html, "utf8");
    }

    const results = await page.evaluate((sel) => {
      function collect(scope) {
        return Array.from(scope.querySelectorAll('a[href]')).map(a => ({
          href: a.href,
          text: (a.textContent || "").replace(/\s+/g, " ").trim(),
          neighbor: ((a.closest("li,p,div")?.textContent) || "").replace(/\s+/g, " ").trim()
        }));
      }
      const content = document.querySelector(sel);
      const arr = content ? collect(content) : collect(document);

      const direct = arr.filter(x => /\.xlsx(\?|#|$)/i.test(x.href) || x.href.includes("/uploads/"));
      const articles = arr.filter(x => x.href.includes("articles?id="));
      return { direct, articles };
    }, contentSel);

    return results;
  });
}

async function renderResolveArticle(articleUrl) {
  return withPage(async (page) => {
    await page.goto(articleUrl, { waitUntil: "networkidle0", timeout: 60000 });

    const contentSel = pickContentSelector();
    await page.waitForFunction((sel) => {
      const content = document.querySelector(sel) || document;
      return content.querySelectorAll('a[href*="/uploads/"], a[href$=".xlsx"]').length > 0;
    }, { timeout: 12000 }, contentSel).catch(() => {});

    if (CONFIG.DEBUG) {
      const html = await page.content();
      fs.mkdirSync("debug", { recursive: true });
      fs.writeFileSync("debug/article_rendered.html", html, "utf8");
    }

    const files = await page.evaluate((sel) => {
      function collect(scope) {
        return Array.from(scope.querySelectorAll('a[href]')).map(a => ({
          href: a.href,
          text: (a.textContent || "").replace(/\s+/g, " ").trim(),
          neighbor: ((a.closest("li,p,div")?.textContent) || "").replace(/\s+/g, " ").trim()
        }));
      }
      const content = document.querySelector(sel);
      const arr = content ? collect(content) : collect(document);
      return arr.filter(x => /\.xlsx(\?|#|$)/i.test(x.href) || x.href.includes("/uploads/"));
    }, contentSel);

    if (!files.length) return null;

    // Enrich + keep annual files only; prefer Physical
    const enriched = files.map(f => {
      const year = smartYearFrom(f.text, f.href, f.neighbor);
      const period = classifyPeriod({ ...f, year });
      return { ...f, year, period };
    });
    const annuals = enriched.filter(f => f.period === "annual");
    if (!annuals.length) return null;

    if (CONFIG.preferPhysicalOnly) {
      const physAnnual = annuals.find(f => /physical/i.test(f.href) || /physical/i.test(f.text || ""));
      if (physAnnual) return physAnnual;
    }
    return annuals[0] || null;
  });
}

// ===== main =====
async function main() {
  const { direct, articles } = await renderCollectMain();

  if (CONFIG.DEBUG) {
    console.log(`Direct XLSX (raw): ${direct.length}`);
    direct.forEach(d => console.log("  -", d.text || decodeBaseNameFromHref(d.href)));
    console.log(`Article pages (raw): ${articles.length}`);
    articles.forEach(a => console.log("  -", a.text || a.href));
  }

  // Build, classify, and keep annual only
  const directXlsx = direct
    .filter(d => /\.xlsx(\?|#|$)/i.test(d.href))
    .map(d => {
      const year = smartYearFrom(d.text, d.href, d.neighbor);
      const period = classifyPeriod({ ...d, year });
      return { ...d, year, period };
    })
    .filter(d => (d.year === null || d.year >= CONFIG.minYear) && d.period === "annual");

  let articleLinks = articles
    .map(a => {
      const year = smartYearFrom(a.text, a.href, a.neighbor);
      const period = classifyPeriod({ ...a, year });
      return { ...a, year, period };
    })
    .filter(a => (a.year === null || a.year >= CONFIG.minYear) && a.period === "annual");

  if (CONFIG.DEBUG) {
    console.log(`Direct XLSX (annual): ${directXlsx.length}`);
    directXlsx.forEach(d => console.log("  -", d.year ?? "?", d.text || decodeBaseNameFromHref(d.href)));
    console.log(`Article pages (annual): ${articleLinks.length}`);
    articleLinks.forEach(a => console.log("  -", a.year ?? "?", a.text || a.href));
  }

  // download direct annual files
  for (const x of directXlsx) {
    await downloadIfNew(x, CONFIG.productName);
    await sleep(CONFIG.throttleMs);
  }

  // resolve & download from annual article pages (e.g., 2024 page)
  for (const a of articleLinks) {
    const x = await renderResolveArticle(a.href);
    if (!x) continue;
    x.text ||= a.text;
    x.year = x.year ?? smartYearFrom(x.text, x.href, x.neighbor || "");
    const period = classifyPeriod({ ...x, year: x.year });
    if (period !== "annual") continue;
    await downloadIfNew(x, CONFIG.productName);
    await sleep(CONFIG.throttleMs);
  }

  console.log("Done.");
}

main().catch(err => { console.error("ERROR:", err); process.exit(1); });
