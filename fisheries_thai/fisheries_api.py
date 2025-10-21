
# -*- coding: utf-8 -*-
"""
กรมประมง (Fisheries CKAN) → กลุ่ม importexport (เฉพาะ 'สถิติ')
1) ดึง datasets/resources ที่ชื่อมีคำว่า 'สถิติ' แล้วรวมเป็น ALL_importexport_STAT_RAW.csv / .parquet
2) อ่านไฟล์รวม → พิมพ์รายชื่อประเทศทั้งหมด + บันทึก country_list_from_stat.csv
3) สรุปผลผลิต (ปริมาณ/ตัน) 'ประเทศ × ชนิดปลา' → by_country_fish_production.csv
4) รวมผลผลิตทั้งโลกต่อชนิดปลา → world_fish_production.csv
5) ✅ เพิ่ม: รวมผลผลิตทั้งโลกแยกตามปี × ชนิดปลา → world_fish_production_by_year.csv
"""

import os, io, csv, json, time, re, unicodedata
from datetime import datetime
import requests
import pandas as pd

# ---------------- Config ----------------
CKAN = "https://catalog.fisheries.go.th/api/3/action"
GROUP = "importexport"

RAW_OUTDIR = "fisheries_importexport_stat_raw"
SUMMARY_OUTDIR = "summaries"
RAW_FILENAME = f"ALL_{GROUP}_STAT_RAW.csv"
RAW_PARQUET = f"ALL_{GROUP}_STAT_RAW.parquet"

# ---------------- HTTP / CKAN helpers ----------------
def http_get(url, params=None, retries=3, backoff=1.5):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(backoff ** (i+1))

def package_search_by_group(group="importexport", rows=500, start=0):
    params = {"fq": f"groups:{group}", "rows": rows, "start": start}
    r = http_get(f"{CKAN}/package_search", params=params)
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(data)
    return data["result"]

def iter_all_datasets_in_group(group="importexport"):
    start = 0
    while True:
        result = package_search_by_group(group, start=start)
        results = result.get("results", [])
        if not results:
            break
        for ds in results:
            yield ds
        start += len(results)
        if start >= result.get("count", 0):
            break

def datastore_fetch_all(resource_id, page_size=50000):
    all_records = []
    offset = 0
    while True:
        params = {"resource_id": resource_id, "limit": page_size, "offset": offset}
        r = http_get(f"{CKAN}/datastore_search", params=params)
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(data)
        res = data["result"]
        recs = res.get("records", [])
        all_records.extend(recs)
        total = res.get("total", len(all_records))
        offset += len(recs)
        if len(recs) == 0 or offset >= total:
            break
    return all_records

# ---------------- Utils for summary ----------------
def norm(s: str) -> str:
    return unicodedata.normalize("NFC", (s or "")).strip().lower()

def detect_col(columns, keywords, prefer_contains=True):
    cols_norm = {norm(c): c for c in columns}
    # exact first
    for k in keywords:
        if norm(k) in cols_norm:
            return cols_norm[norm(k)]
    # fallback contains/startswith
    for c in columns:
        cn = norm(c)
        for k in keywords:
            kn = norm(k)
            if prefer_contains and kn in cn:
                return c
            if not prefer_contains and cn.startswith(kn):
                return c
    return None

def parse_number(s):
    if pd.isna(s):
        return None
    x = str(s).replace("\u200b", "").strip()
    if not x:
        return None
    x = re.sub(r"[^0-9\-\.,]", "", x).replace(",", "")
    if x in {"", "-", ".", "-.", ".-"}:
        return None
    try:
        return float(x)
    except Exception:
        return None

UNIT_TO_TON = {
    "กก.": 1/1000, "กก": 1/1000, "กิโลกรัม": 1/1000, "kg": 1/1000, "kilogram": 1/1000,
    "ตัน": 1.0, "t": 1.0, "ton": 1.0, "metric ton": 1.0,
}

def to_ton(value, unit: str):
    if value is None:
        return None
    u = norm(unit)
    for k, factor in UNIT_TO_TON.items():
        if norm(k) == u:
            return value * factor
    return value  # unknown unit → keep as is

def first_nonnull_numeric(series_list):
    out = None
    for s in series_list:
        if s is None:
            continue
        if out is None:
            out = s.apply(parse_number)
        else:
            cand = s.apply(parse_number)
            out = out.where(pd.notna(out), cand)
    return out

def clean_year(y):
    if pd.isna(y):
        return None
    s = str(y).strip()
    s = re.sub(r"[^0-9]", "", s)
    if not s:
        return None
    try:
        val = int(s)
    except Exception:
        return None
    # แปลงปี พ.ศ. → ค.ศ. ถ้าอยู่ช่วงสมเหตุสมผล
    if 2400 <= val <= 2600:
        val -= 543
    if 1900 <= val <= 2100:
        return val
    return None

# ---------------- Step 1: Fetch 'สถิติ' only & build RAW ----------------
def dump_group_stat_only(group="importexport", outdir=RAW_OUTDIR):
    os.makedirs(outdir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    catalog_rows, all_data = [], []
    print(f"📊 ดึงเฉพาะข้อมูลที่มีคำว่า 'สถิติ' ในชื่อ dataset/resource จาก group: {group}")

    for ds in iter_all_datasets_in_group(group):
        ds_title = ds.get("title") or ds.get("name") or ""
        if "สถิติ" not in ds_title:
            continue

        print(f"\n▶️ Dataset: {ds_title}")
        for r in ds.get("resources", []):
            rname = r.get("name") or ""
            if "สถิติ" not in rname and "stat" not in rname.lower():
                continue

            rid = r.get("id")
            fmt = (r.get("format") or "").upper()
            url = r.get("url") or ""
            active = r.get("datastore_active", False)
            print(f"  → Resource: {rname} [{fmt}] datastore_active={active}")

            catalog_rows.append({
                "dataset": ds_title,
                "resource_name": rname,
                "resource_id": rid,
                "format": fmt,
                "datastore_active": active,
                "url": url
            })

            df = None
            try:
                if active:
                    records = datastore_fetch_all(rid)
                    df = pd.DataFrame(records)
                    print(f"    ✅ datastore rows={len(df)}")
                elif url:
                    resp = http_get(url)
                    content = resp.content
                    if fmt in {"CSV", ""} or url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="utf-8", on_bad_lines="skip")
                    elif fmt in {"XLSX", "XLS"} or url.lower().endswith((".xlsx", ".xls")):
                        df = pd.read_excel(io.BytesIO(content), dtype=str, engine="openpyxl")
                    print(f"    ✅ file rows={len(df)}")
            except Exception as e:
                print(f"    ⚠️ Error: {e}")
                continue

            if df is not None and not df.empty:
                df["_dataset"] = ds_title
                df["_resource"] = rname
                df["_rid"] = rid
                all_data.append(df)

    # save catalog
    cat_df = pd.DataFrame(catalog_rows)
    cat_csv = os.path.join(outdir, f"catalog_{group}_stat_{ts}.csv")
    cat_df.to_csv(cat_csv, index=False, encoding="utf-8-sig")
    print(f"\n📑 catalog saved: {cat_csv}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        out_csv = os.path.join(outdir, RAW_FILENAME)
        out_parq = os.path.join(outdir, RAW_PARQUET)
        combined.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        try:
            combined.to_parquet(out_parq, index=False)
        except Exception as e:
            print(f"⚠️ parquet error: {e}")
        print(f"✅ saved RAW:\n - {out_csv}\n - {out_parq}\nรวมทั้งหมด {len(combined):,} rows")
        return out_csv
    else:
        print("⚠️ ไม่มีข้อมูลสถิติใน group นี้")
        return None

# ---------------- Step 2+: Summaries from RAW ----------------
def summarize_from_raw(raw_csv_path):
    os.makedirs(SUMMARY_OUTDIR, exist_ok=True)

    print(f"\n📂 อ่าน RAW: {raw_csv_path}")
    df = pd.read_csv(raw_csv_path, dtype=str, encoding="utf-8", on_bad_lines="skip")
    df.columns = [c.strip().replace("\u200b", "") for c in df.columns]

    # detect columns (รองรับชื่อที่พบจริง)
    country_col = detect_col(df.columns, ["ประเทศ", "country"])
    fish_candidates = [
        "ชื่อไทย", "ชื่อสามัญ", "ชื่อวิทยาศาสตร์", "กลุ่มสัตว์น้ำ",
        "ชนิดสัตว์น้ำ", "ชนิด", "ปลา", "สินค้า", "commodity", "product"
    ]
    fish_col = detect_col(df.columns, fish_candidates)

    qty_candidates = [
        "ปริมาณ", "จำนวนปริมาณ", "จำนวน/ปริมาณ",
        "น้ำหนัก", "quantity", "weight", "volume"
    ]
    qty_col = detect_col(df.columns, qty_candidates)
    unit_col = detect_col(df.columns, ["หน่วย", "หน่วย.1", "unit", "units"])
    value_col = detect_col(df.columns, ["มูลค่า (บาท)", "มูลค่าบาท", "value", "value_thb"])

    # ✅ year detection
    year_col = detect_col(df.columns, ["ปี", "พ.ศ.", "ปี (พ.ศ.)", "year", "ปีพ.ศ."])

    print("\n🔍 Column detection:")
    print("  ประเทศ     :", country_col)
    print("  ชนิดปลา    :", fish_col)
    print("  ปริมาณ     :", qty_col)
    print("  หน่วย       :", unit_col)
    print("  มูลค่า      :", value_col)
    print("  ปี          :", year_col)

    if not country_col:
        print("\n❌ หา column 'ประเทศ' ไม่เจอ")
        print("คอลัมน์ทั้งหมด:", list(df.columns)); return
    if not fish_col:
        print("\n❌ หา column 'ชนิดปลา/สัตว์น้ำ' ไม่เจอ")
        print("คอลัมน์ทั้งหมด:", list(df.columns)); return

    # 1) ประเทศทั้งหมด
    countries = sorted(df[country_col].dropna().unique().tolist())
    print(f"\n🌍 พบประเทศทั้งหมด {len(countries)} ประเทศในคอลัมน์ '{country_col}':")
    for i, c in enumerate(countries, 1):
        print(f"{i:02d}. {c}")
    out_country = os.path.join(SUMMARY_OUTDIR, "country_list_from_stat.csv")
    pd.DataFrame({"ประเทศ": countries}).to_csv(out_country, index=False, encoding="utf-8-sig")
    print(f"💾 บันทึกประเทศ: {out_country}")

    # 2) ปริมาณ (ตัน)
    qty_series_list = [df[c] for c in qty_candidates if c in df.columns]
    qty_num = first_nonnull_numeric(qty_series_list)
    if qty_num is None:
        print("\n⚠️ ไม่พบคอลัมน์ปริมาณที่ใช้ได้"); return
    df["_qty_num"] = qty_num
    if unit_col:
        df["_qty_ton"] = [to_ton(v, u) for v, u in zip(df["_qty_num"], df[unit_col])]
    else:
        df["_qty_ton"] = df["_qty_num"]
    df_prod = df[pd.notna(df["_qty_ton"])].copy()

    # ✅ สร้างคอลัมน์ year_clean (ถ้ามี year_col)
    if year_col:
        df_prod["year_clean"] = df_prod[year_col].apply(clean_year)
    else:
        df_prod["year_clean"] = pd.NA

    # 3) สรุป ประเทศ × ชนิดปลา
    grp_cols = [country_col, fish_col]
    by_country_fish = (
        df_prod.groupby(grp_cols, dropna=False)["_qty_ton"]
        .sum()
        .reset_index()
        .rename(columns={country_col: "country", fish_col: "fish", "_qty_ton": "production_ton"})
    )
    out_by = os.path.join(SUMMARY_OUTDIR, "by_country_fish_production.csv")
    by_country_fish.to_csv(out_by, index=False, encoding="utf-8-sig")
    print(f"\n💾 saved: {out_by} (rows={len(by_country_fish)})")

    # 4) รวมทั้งโลก ต่อชนิดปลา (รวมทุกปี)
    world_fish = (
        by_country_fish.groupby("fish", dropna=False)["production_ton"]
        .sum()
        .reset_index()
        .sort_values("production_ton", ascending=False)
    )
    out_world = os.path.join(SUMMARY_OUTDIR, "world_fish_production.csv")
    world_fish.to_csv(out_world, index=False, encoding="utf-8-sig")
    print(f"💾 saved: {out_world} (rows={len(world_fish)})")

    # 5) ✅ รวมทั้งโลก แยกตามปี × ชนิดปลา
    if df_prod["year_clean"].notna().any():
        world_fish_by_year = (
            df_prod.groupby(["year_clean", fish_col], dropna=False)["_qty_ton"]
            .sum()
            .reset_index()
            .rename(columns={"year_clean": "year", fish_col: "fish", "_qty_ton": "production_ton"})
            .sort_values(["year", "production_ton"], ascending=[True, False])
        )
        out_world_year = os.path.join(SUMMARY_OUTDIR, "world_fish_production_by_year.csv")
        world_fish_by_year.to_csv(out_world_year, index=False, encoding="utf-8-sig")
        print(f"💾 saved: {out_world_year} (rows={len(world_fish_by_year)})")
    else:
        print("ℹ️ ไม่พบคอลัมน์ปีที่ใช้งานได้ จึงไม่สร้าง world_fish_production_by_year.csv")

    # Preview
    print("\n🏁 Top 10 world fish by production (ton):")
    print(world_fish.head(10))

# ---------------- Main ----------------
if __name__ == "__main__":
    # Step 1: ดึงสถิติและรวม RAW
    raw_csv_path = dump_group_stat_only()
    # Step 2+: ถ้าดึงสำเร็จ → ทำสรุปจากไฟล์ RAW ที่เพิ่งสร้าง
    if raw_csv_path and os.path.exists(raw_csv_path):
        summarize_from_raw(raw_csv_path)
    else:
        # ถ้าดึงไม่สำเร็จ แต่มีไฟล์เก่าอยู่แล้ว ก็อ่านไฟล์เดิม
        fallback_path = os.path.join(RAW_OUTDIR, RAW_FILENAME)
        if os.path.exists(fallback_path):
            print("\nℹ️ ใช้ไฟล์ RAW เดิมที่มีอยู่แล้ว")
            summarize_from_raw(fallback_path)
        else:
            print("\n❌ ไม่มีไฟล์ RAW ให้สรุปผล")
