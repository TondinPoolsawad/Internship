import puppeteer from "puppeteer";
import * as cheerio from "cheerio";
import axios from "axios";
import fs from "fs-extra";
import path from "path";

const BASE = "https://oae.go.th";
const MAIN_URL = `${BASE}/home/article/475`;
const SAVE_DIR = "./reports";
const RECORD_PATH = "./record.json";

const blacklist = [
  "ประวัติ",
  "วิสัยทัศน์",
  "พันธกิจ",
  "ผู้บริหาร",
  "โครงสร้างองค์กร",
  "ระเบียบ",
  "งบประมาณ",
  "ผลการดำเนินงาน",
  "หน่วยงานภายใน",
  "เอกสารเผยแพร่",
  "ข่าว",
  "ถาม-ตอบ",
  "OIT",
  "ติดต่อเรา",
  "ระบบราชการ",
  "ข้อมูลเศรษฐกิจ",
  "กฎ",
  "การใช้จ่าย",
  "ข่าวประชาสัมพันธ์",
  "ประกาศ",
  "การจัดซื้อจัดจ้าง",
];

function loadRecord() {
  if (fs.existsSync(RECORD_PATH)) {
    return JSON.parse(fs.readFileSync(RECORD_PATH, "utf-8"));
  }
  return {};
}

function saveRecord(record) {
  fs.writeFileSync(RECORD_PATH, JSON.stringify(record, null, 2), "utf-8");
}

async function scrapeOAE() {
  const record = loadRecord();
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  console.log(`Opening main page: ${MAIN_URL}`);
  await page.goto(MAIN_URL, { waitUntil: "networkidle2", timeout: 0 });
  await page.waitForSelector("app-root a[href^='/home/article/']", { timeout: 20000 });

  const html = await page.content();
  const $ = cheerio.load(html);
  const plantLinks = [];

  $("a[href^='/home/article/']").each((i, el) => {
    const href = $(el).attr("href");
    const name = $(el).text().trim();
    if (!href || !name || href.includes("/home/article/475")) return;
    const isBlacklisted = blacklist.some((word) => name.includes(word));
    if (isBlacklisted) return;
    plantLinks.push({ name, url: BASE + href });
  });

  console.log(`\nFound ${plantLinks.length} plant categories`);
  console.table(plantLinks);

  for (const { name, url } of plantLinks) {
    console.log(`\nVisiting: ${name} (${url})`);
    try {
      await scrapePlantByYear(browser, name, url, record);
    } catch (err) {
      console.warn(`Error in ${name}: ${err.message}`);
    }
  }

  saveRecord(record);
  await browser.close();
  console.log("\nAll reports have been downloaded and recorded successfully.");
}

async function scrapePlantByYear(browser, plantName, plantUrl, record) {
  const page = await browser.newPage();
  await page.goto(plantUrl, { waitUntil: "networkidle2", timeout: 0 });

  try {
    await page.waitForSelector("app-root", { timeout: 15000 });
  } catch {
    console.warn(`No data found for ${plantName}`);
    await page.close();
    return;
  }

  const html = await page.content();
  const $ = cheerio.load(html);
  const yearSections = $("div.section-title");

  if (yearSections.length === 0) {
    console.warn(`No yearly sections found in ${plantName}`);
    await page.close();
    return;
  }

  for (const el of yearSections.toArray()) {
    const yearText = $(el).text().trim();
    const year = yearText.match(/(25\d{2}|20\d{2})/)?.[0] || "unknown";
    const pdfs = [];

    $(el).next("ul").find("a[href$='.pdf']").each((i, a) => {
      const title = $(a).text().trim();
      const url = $(a).attr("href");
      pdfs.push({ title, url: url.startsWith("http") ? url : BASE + url });
    });

    if (pdfs.length > 0) {
      console.log(`Year ${year}: ${pdfs.length} file(s)`);
      await downloadPDFs(plantName, year, pdfs, record);
    } else {
      console.log(`No PDFs found for year ${year}`);
    }
  }

  await page.close();
}

async function downloadPDFs(plantName, year, pdfs, record) {
  const dir = path.join(SAVE_DIR, plantName, year);
  await fs.ensureDir(dir);

  if (!record[plantName]) record[plantName] = {};
  if (!record[plantName][year]) record[plantName][year] = [];

  for (const { title, url } of pdfs) {
    const safeTitle = title.replace(/[\\/:*?"<>|]/g, "_").slice(0, 120);
    const filePath = path.join(dir, `${safeTitle}.pdf`);
    const alreadyRecorded = record[plantName][year].includes(safeTitle);
    if (alreadyRecorded || fs.existsSync(filePath)) {
      console.log(`Already downloaded: ${safeTitle}`);
      continue;
    }

    console.log(`Downloading: ${safeTitle}`);
    try {
      const res = await axios.get(url, { responseType: "arraybuffer" });
      fs.writeFileSync(filePath, res.data);
      record[plantName][year].push(safeTitle);
      saveRecord(record);
    } catch (err) {
      console.error(`Failed to download ${safeTitle}: ${err.message}`);
    }
  }
}

scrapeOAE();
