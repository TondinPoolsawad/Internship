import fs from "fs";
import path from "path";

const CKAN = "https://catalog.oae.go.th/api/3/action";
const CONFIG = JSON.parse(fs.readFileSync("./config.json", "utf8"));

// ชื่อไทย -> ชื่อไฟล์อังกฤษ (อ่านง่าย ชัดเจน)
const EN_NAME_MAP = {
  "ปริมาณการผลิตข้าวโพดเลี้ยงสัตว์": "maize_production.csv",
  "ปริมาณการผลิตข้าว": "rice_production.csv",
  "ปริมาณการผลิตมันสำปะหลัง": "cassava_production.csv",
  "ปริมาณการผลิตปาล์มน้ำมัน": "oilpalm_production.csv",
  "ปริมาณการผลิตยางพารา": "rubber_production.csv",
  "มูลค่าของสินค้าเกษตรชีวภาพ": "bio_agri_products_value.csv",
  "มูลค่าของสินค้าเกษตรปลอดภัย": "safe_agri_products_value.csv",
  "มูลค่าของผลไม้เมืองร้อน": "tropical_fruits_value.csv",
  "มูลค่าผลผลิตข้าวหอมมะลิ": "jasmine_rice_value.csv"
};


// คำบ่งชี้ว่าเป็น Data Dictionary (ทั้งไทย/อังกฤษ)
const DICT_KEYWORDS = [
  "dictionary",
  "data dictionary",
  "datadic",
  "dict",
  "metadata",
  "พจนานุกรม",
  "คำอธิบาย",
  "พจนานุกรมข้อมูล"
];

function looksLikeDictionary(name = "", desc = "", format = "") {
  const hay = `${name} ${desc} ${format}`.toLowerCase();
  return DICT_KEYWORDS.some(k => hay.includes(k.toLowerCase()));
}

async function fetchJSON(url, opts = {}, tries = 3) {
  for (let i = 1; i <= tries; i++) {
    const r = await fetch(url, opts);
    if (r.ok) return r.json();
    if (i === tries) throw new Error(`Fetch failed: ${r.status} ${r.statusText} -> ${url}`);
    await new Promise(res => setTimeout(res, 800 * i));
  }
}

async function downloadFile(url, dest) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status} on ${url}`);
  const buf = Buffer.from(await r.arrayBuffer());
  fs.writeFileSync(dest, buf);
}

function escapeCSV(v) {
  if (v == null) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes("\r")) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

async function dumpDatastoreToCSV(resource_id, dest) {
  const limit = CONFIG.pageLimit || 10000;
  let offset = 0;
  let wroteHeader = false;
  const fd = fs.openSync(dest, "w");
  try {
    for (;;) {
      const url = `${CKAN}/datastore_search?resource_id=${resource_id}&limit=${limit}&offset=${offset}`;
      const json = await fetchJSON(url);
      const result = json?.result;
      const records = result?.records || [];
      if (records.length === 0) break;

      const fields = (result.fields || []).map(f => f.id).filter(k => k !== "_id");
      if (!wroteHeader) {
        fs.writeFileSync(fd, fields.join(",") + "\n");
        wroteHeader = true;
      }
      for (const rec of records) {
        const row = fields.map(k => escapeCSV(rec[k]));
        fs.writeFileSync(fd, row.join(",") + "\n");
      }
      offset += limit;
    }
  } finally {
    fs.closeSync(fd);
  }
}

async function searchDatasetByTitle(title) {
  const q = encodeURIComponent(`title:"${title}" organization:oae_cai groups:production`);
  const url = `${CKAN}/package_search?q=${q}&rows=50`;
  const json = await fetchJSON(url);
  const all = json?.result?.results || [];
  const exact = all.filter(d => (d.title || "").trim() === title.trim());
  const candidates = (exact.length ? exact : all.filter(d => (d.title || "").includes(title))).sort(
    (a, b) => new Date(b.metadata_modified || 0) - new Date(a.metadata_modified || 0)
  );
  return candidates[0] || null;
}

function pickMainCSV(resources = []) {
  // 1) กรองเฉพาะ CSV
  const csvs = resources.filter(r => {
    const fmt = String(r.format || "").toLowerCase();
    const mime = String(r.mimetype || "").toLowerCase();
    return fmt === "csv" || mime.includes("csv");
  });

  // 2) ตัดตัวที่เป็น Dictionary ทิ้ง
  const dataOnly = csvs.filter(r => !looksLikeDictionary(r.name, r.description, r.format));

  // 3) ถ้ามีหลายไฟล์: ให้ priority กับ datastore_active
  const dsActiveFirst = dataOnly.sort((a, b) => {
    const ad = a.datastore_active ? 1 : 0;
    const bd = b.datastore_active ? 1 : 0;
    if (ad !== bd) return bd - ad; // true มาก่อน
    // fallback: ขนาดไฟล์ใหญ่ก่อน (ถ้า CKAN คืน size มาด้วย)
    const as = Number(a.size || 0);
    const bs = Number(b.size || 0);
    return bs - as;
  });

  // 4) คืนไฟล์แรกเป็นตัวหลัก (หรือ null ถ้าไม่มี)
  return dsActiveFirst[0] || null;
}

async function downloadForTitle(title) {
  const dataset = await searchDatasetByTitle(title);
  if (!dataset) {
    console.log(`❌ ไม่พบ dataset: ${title}`);
    return;
  }

  const resources = Array.isArray(dataset.resources) ? dataset.resources : [];
  console.log(`\n📦 ${dataset.title} — resources: ${resources.length}`);

  // เลือก CSV หลัก (ตัด dictionary ออก)
  const mainCSV = pickMainCSV(resources);

  const outDir = CONFIG.outputDir;
  fs.mkdirSync(outDir, { recursive: true });

  const outName = EN_NAME_MAP[title] || "dataset.csv";
  const outPath = path.join(outDir, outName);

  if (mainCSV) {
    try {
      await downloadFile(mainCSV.url, outPath);
      console.log(`   ✅ Saved main CSV → ${outName}`);
      return;
    } catch (e) {
      console.log(`   ⚠️ ดาวน์โหลด CSV หลักไม่สำเร็จ (${e.message})`);
    }
  }

  // กรณีไม่มี CSV หลัก แต่มี datastore เปิด → ดึงผ่าน API แล้ว export เป็น CSV
  if (CONFIG.useDatastoreIfNoCSV) {
    const dsRes = resources.find(r => r.datastore_active);
    if (dsRes?.id) {
      try {
        await dumpDatastoreToCSV(dsRes.id, outPath);
        console.log(`   ✅ API→CSV (datastore) → ${outName}`);
        return;
      } catch (e) {
        console.log(`   ⚠️ ดึง datastore ไม่สำเร็จ (${e.message})`);
      }
    }
  }

  console.log("   ⚠️ ไม่พบ CSV ที่ใช้งานได้ และ datastore ไม่พร้อม");
}

async function main() {
  for (const title of CONFIG.titles) {
    await downloadForTitle(title);
  }
  console.log("\n🎉 เสร็จสิ้น");
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
