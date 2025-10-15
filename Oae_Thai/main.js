import puppeteer from "puppeteer";
import * as cheerio from "cheerio";
import axios from "axios";
import fs from "fs-extra";
import path from "path";

const BASE = "https://oae.go.th";
const MAIN_URL = `${BASE}/home/article/475`;
const SAVE_DIR = "./reports";

// รายชื่อ blacklist (ไม่ใช่พืชผล)
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
  "การจัดซื้อจัดจ้าง"
];

// 🔹 ฟังก์ชันหลัก
async function scrapeOAE() {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  console.log(`🌐 กำลังเข้าเว็บไซต์หลัก: ${MAIN_URL}`);
  await page.goto(MAIN_URL, { waitUntil: "networkidle2", timeout: 0 });

  // ✅ รอให้ Angular โหลดลิงก์พืชทั้งหมด
  await page.waitForSelector("app-root a[href^='/home/article/']", { timeout: 20000 });
  const html = await page.content();
  const $ = cheerio.load(html);

  const plantLinks = [];
  $("a[href^='/home/article/']").each((i, el) => {
    const href = $(el).attr("href");
    const name = $(el).text().trim();

    // ข้ามลิงก์ที่ไม่เกี่ยวข้อง
    if (!href || !name || href.includes("/home/article/475")) return;

    const isBlacklisted = blacklist.some((word) => name.includes(word));
    if (isBlacklisted) {
      console.log(`🚫 ข้ามลิงก์: ${name}`);
      return;
    }

    plantLinks.push({ name, url: BASE + href });
  });

  console.log(`\n🌾 พบพืชที่เกี่ยวข้องทั้งหมด ${plantLinks.length} รายการ`);
  console.table(plantLinks);

  // 🔁 เข้าหน้าพืชแต่ละชนิด
  for (const { name, url } of plantLinks) {
    console.log(`\n🔍 กำลังเข้า: ${name} (${url})`);
    try {
      await scrapePlantPDFs(browser, name, url);
    } catch (err) {
      console.warn(`⚠️ เกิดข้อผิดพลาดกับ ${name}: ${err.message}`);
    }
  }

  await browser.close();
  console.log("\n✅ เสร็จสมบูรณ์! โหลดรายงานครบทุกพืชแล้ว");
}

// 🔹 ดึง PDF ของพืชแต่ละชนิด
async function scrapePlantPDFs(browser, plantName, plantUrl) {
  const page = await browser.newPage();
  await page.goto(plantUrl, { waitUntil: "networkidle2", timeout: 0 });

  try {
    await page.waitForSelector("app-root a[href$='.pdf']", { timeout: 15000 });
  } catch {
    console.warn(`⚠️ ไม่มี PDF ใน ${plantName}`);
    await page.close();
    return;
  }

  const html = await page.content();
  const $ = cheerio.load(html);

  const pdfLinks = [];
  $("a[href$='.pdf']").each((i, el) => {
    const link = $(el).attr("href");
    const file = decodeURIComponent(link.split("/").pop());
    const yearMatch = file.match(/(20\d{2}|25\d{2})/);
    const year = yearMatch ? yearMatch[0] : "unknown";
    pdfLinks.push({
      file,
      url: link.startsWith("http") ? link : BASE + link,
      year,
    });
  });

  console.log(`📄 พบ PDF ${pdfLinks.length} ไฟล์สำหรับ "${plantName}"`);
  await downloadPDFs(plantName, pdfLinks);
  await page.close();
}

// 🔹 ดาวน์โหลด PDF พร้อมสร้างโฟลเดอร์
async function downloadPDFs(plantName, pdfs) {
  const dir = path.join(SAVE_DIR, plantName);
  await fs.ensureDir(dir);

  for (const { file, url } of pdfs) {
    const filePath = path.join(dir, file);
    if (fs.existsSync(filePath)) {
      console.log(`🟢 มีไฟล์แล้ว: ${file}`);
      continue;
    }

    console.log(`⬇️ กำลังดาวน์โหลด: ${file}`);
    try {
      const res = await axios.get(url, { responseType: "arraybuffer" });
      fs.writeFileSync(filePath, res.data);
    } catch (err) {
      console.error(`❌ โหลดไม่สำเร็จ: ${file} (${err.message})`);
    }
  }
}

// 🔹 เริ่มต้นรันโปรแกรม
scrapeOAE();
