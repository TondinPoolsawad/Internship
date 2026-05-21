"""
OAE CKAN → group=production → AUTO:
1) Inspect & fetch all
2) Slim to 8 cols: id, year, province, commod, subcommod, atrriburte, value, unit
3) Clean
4) Keep ONLY rows where atrriburte in {"production", "value"}
5) Save per-resource + combined (CSV & Parquet)
6) PRUNE: keep only ALL_*.csv and ALL_*.parquet (ลบไฟล์อื่นทั้งหมดใน outdir)

Usage:
  pip install requests pandas pyarrow openpyxl
  python oae_auto_production_slim.py --outdir oae_prod_slim
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
import os, io, csv, json, time, argparse, re, unicodedata
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
import pandas as pd

# ---------------------- Supabase upload ----------------------

def upload_csv_to_supabase(local_path: str, bucket_filename: str):
    """Upload a local CSV to Supabase Storage, overwriting the existing file."""
    try:
        from supabase import create_client
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not key:
            print("⚠️  SUPABASE_SERVICE_KEY not set — skipping upload")
            return
        client = create_client("https://mzzyjtlrbbwqdxpenwne.supabase.co", key)
        with open(local_path, "rb") as f:
            data = f.read()
        client.storage.from_("csvs").remove([bucket_filename])
        client.storage.from_("csvs").upload(
            path=bucket_filename,
            file=data,
            file_options={"content-type": "text/csv"}
        )
        print(f"☁️  Uploaded → csvs/{bucket_filename}")
    except ImportError:
        print("⚠️  supabase not installed — run: pip install supabase")

# ---------------------- Format conversion ----------------------

# Maps commod → source filename (matching advisor-approved format)
SOURCE_FILE_MAP = {
    "มันสำปะหลัง":       "cassava_production.csv",
    "ข้าวนาปี":           "jasmine_rice_value.csv",
    "ข้าวโพดเลี้ยงสัตว์": "maize_production.csv",
    "ปาล์มน้ำมัน":        "oilpalm_production.csv",
    "ข้าว":               "rice_production.csv",
    "ยางพารา":            "rubber_production.csv",
    "ลิ้นจี่":            "tropical_fruits_value.csv",
    "ลองกอง":             "tropical_fruits_value.csv",
    "สับปะรดปัตตาเวีย":  "tropical_fruits_value.csv",
    "ลำไย":               "tropical_fruits_value.csv",
    "เงาะ":               "tropical_fruits_value.csv",
    "ทุเรียน":            "tropical_fruits_value.csv",
    "มังคุด":             "tropical_fruits_value.csv",
}

# Maize uses split-year format e.g. 2020 → "2563/64"
MAIZE_COMMODS = {"ข้าวโพดเลี้ยงสัตว์"}

def to_buddhist(year_ce):
    """Convert Gregorian year to Buddhist era string."""
    try:
        y = int(year_ce)
        return str(y + 543)
    except:
        return str(year_ce)

def to_buddhist_split(year_ce):
    """Convert e.g. 2020 → '2563/64'"""
    try:
        y = int(year_ce)
        be = y + 543
        return f"{be}/{str(be + 1)[-2:]}"
    except:
        return str(year_ce)

def convert_to_approved_format(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv, encoding="utf-8")
    rows = []
    for _, row in df.iterrows():
        commod = str(row.get("commod", "")).strip()
        year_ce = row.get("year")
        value = row.get("value")
        unit = str(row.get("unit", "ตัน")).strip()

        if commod in MAIZE_COMMODS:
            year_str = to_buddhist_split(year_ce)
        else:
            year_str = to_buddhist(year_ce)

        source_file = SOURCE_FILE_MAP.get(commod, "oae.csv")

        try:
            val_fmt = f"{float(value):,.2f}"
        except:
            val_fmt = str(value)

        rows.append({
            "Year": year_str,
            "Commodity": commod,
            "Value": val_fmt,
            "Unit": unit,
            "Source_File": source_file,
        })

    out = pd.DataFrame(rows, columns=["Year", "Commodity", "Value", "Unit", "Source_File"])
    out.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Converted to approved format → {output_csv} (rows={len(out)})")

CKAN = "https://catalog.oae.go.th/api/3/action"
TARGET_ORDER = ["id", "year", "province", "commod", "subcommod", "atrriburte", "value", "unit"]
FINAL_ATTR_SET = {"production", "value"}  # ⬅️ คงไว้เฉพาะ 2 ค่านี้

# ---------------------- Helpers ----------------------

def safe_filename(s: str) -> str:
    keep = "-_()[]{} ."
    s = (s or "").strip()
    s = "".join(ch if ch.isalnum() or ch in keep else "_" for ch in s)
    return s or "file"

def http_get(url: str, params: dict=None, timeout: int=60, retries: int=3, backoff: float=1.6):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(backoff ** (i + 1))

def package_search_by_group(group="production", rows=500, start=0, q=None) -> Dict[str, Any]:
    params = {"fq": f"groups:{group}", "rows": rows, "start": start}
    if q:
        params["q"] = q
    r = http_get(f"{CKAN}/package_search", params=params)
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(data)
    return data["result"]

def iter_all_datasets_in_group(group="production", q=None):
    start = 0
    while True:
        result = package_search_by_group(group=group, rows=500, start=start, q=q)
        results = result.get("results", [])
        if not results:
            break
        for ds in results:
            yield ds
        start += len(results)
        if start >= result.get("count", 0):
            break

def datastore_fields(resource_id: str) -> List[Dict[str, Any]]:
    params = {"resource_id": resource_id, "limit": 0}
    r = http_get(f"{CKAN}/datastore_search", params=params)
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(data)
    fields = data["result"].get("fields", [])
    if not fields:
        r = http_get(f"{CKAN}/datastore_search", params={"resource_id": resource_id, "limit": 1})
        data = r.json()
        fields = data["result"].get("fields", [])
    return fields

def datastore_fetch_all(resource_id: str, page_size: int=50000, filters: Optional[dict]=None) -> List[Dict[str, Any]]:
    all_records = []
    offset = 0
    while True:
        params = {"resource_id": resource_id, "limit": page_size, "offset": offset}
        if filters:
            params["filters"] = json.dumps(filters, ensure_ascii=False)
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

# ---------------------- Column mapping ----------------------

def norm(s: str) -> str:
    return unicodedata.normalize("NFC", (s or "")).strip().lower()

ALIASES = {
    "id": {"id", "_id", "recordid", "record_id", "ลำดับ", "รหัส"},
    "year": {"year", "ปี", "พ.ศ.", "พศ", "year_be", "be_year", "ปี(พ.ศ.)", "ปีพ.ศ.", "ปีพศ"},
    "province": {"province", "จังหวัด", "prov", "prov_name", "province_name"},
    "commod": {"commod", "commodity", "สินค้า", "ชนิดสินค้า", "พืช", "สินค้าเกษตร"},
    "subcommod": {"subcommod", "sub_commod", "subcommodity", "ชนิดย่อย", "พันธุ์", "ประเภทย่อย"},
    "atrriburte": {"atrriburte", "attribute", "attr", "ตัวแปร", "รายการ", "ตัวชี้วัด"},
    "value": {"value", "val", "ค่าที่วัดได้", "ปริมาณ", "ผลผลิต", "ปริมาณผลผลิต", "มูลค่า"},
    "unit": {"unit", "units", "หน่วย", "หน่วยนับ"},
}

def build_col_map(columns: List[str]) -> Dict[str, str]:
    norm_cols = {norm(c): c for c in columns}
    mapping = {}
    for target, cands in ALIASES.items():
        hit = None
        for cand in cands:
            if norm(cand) in norm_cols:
                hit = norm_cols[norm(cand)]
                break
        if not hit:
            for nc, orig in norm_cols.items():
                if any(nc.startswith(norm(c)) for c in cands):
                    hit = orig
                    break
        if not hit:
            for nc, orig in norm_cols.items():
                if any((norm(c) in nc and len(norm(c)) >= 3) for c in cands):
                    hit = orig
                    break
        if hit:
            mapping[target] = hit
    return mapping

def slim_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=TARGET_ORDER)
    cols = list(df.columns)
    cmap = build_col_map(cols)
    present_targets = [t for t in TARGET_ORDER if t in cmap]
    if not present_targets:
        return pd.DataFrame(columns=TARGET_ORDER)
    slim = df[[cmap[t] for t in present_targets]].copy()
    slim.columns = present_targets
    for t in TARGET_ORDER:
        if t not in slim.columns:
            slim[t] = pd.NA
    slim = slim[TARGET_ORDER]
    return slim

# ---------------------- Cleaning ----------------------

WS_RE = re.compile(r"\s+")
NUM_RE = re.compile(r"[^0-9\-\.,]")

UNIT_MAP = {
    "กก.": "กิโลกรัม",
    "กก": "กิโลกรัม",
    "kg": "กิโลกรัม",
    "ตัน": "ตัน",
    "t": "ตัน",
    "พันตัน": "ตัน",  # Will be converted to ตัน with value * 1000
    "บาท": "บาท",
    "bt": "บาท",
    "baht": "บาท",
    "ไร่": "ไร่",
    "ตัน/ไร่": "ตัน/ไร่",
    "กิโลกรัม/ไร่": "กิโลกรัม/ไร่",
    "ลบ.ม.": "ลูกบาศก์เมตร",
    "ลบม.": "ลูกบาศก์เมตร",
}

def convert_value_by_unit(value: float, unit: str) -> tuple[float, str]:
    """Convert value based on unit and return new value and unit."""
    if pd.isna(value):
        return value, unit
    
    unit = clean_text(unit).lower()  # Use clean_text to normalize the unit string
    if "พันตัน" in unit:  # Check if unit contains พันตัน
        return float(value) * 1000, "ตัน"
    return value, UNIT_MAP.get(unit, unit)

# ⬇️ ทำให้ "มูลค่า" ถูกแมปเป็น value (เพื่อผ่านเงื่อนไข FINAL_ATTR_SET)
ATTR_MAP = {
    "ผลผลิต": "production",
    "ผลผลิตต่อไร่": "yield_per_rai",
    "เนื้อที่เพาะปลูก": "planted_area",
    "เนื้อที่เก็บเกี่ยว": "harvested_area",
    "เนื้อที่ยืนต้น": "perennial_area",
    "เนื้อที่ให้ผล": "bearing_area",
    "จำนวนครัวเรือนผู้ปลูก": "num_households",
    "มูลค่า": "value",   # 🔥 เปลี่ยนเป็น "value"
}

def clean_text(x: Any) -> str:
    s = unicodedata.normalize("NFC", str(x)) if pd.notna(x) else ""
    s = s.replace("\u200b", "")
    s = WS_RE.sub(" ", s).strip()
    return s

def clean_year(y: Any) -> Optional[int]:
    if pd.isna(y):
        return None
    s = clean_text(y)
    if not s:
        return None
    s = re.sub(r"[^0-9\-]", "", s)
    if not s:
        return None
    try:
        val = int(s)
    except Exception:
        return None
    if 2400 <= val <= 2600:
        val = val - 543
    if 1900 <= val <= 2100:
        return val
    return None

def clean_province(p: Any) -> str:
    s = clean_text(p)
    if not s:
        return ""
    s = re.sub(r"^\s*จังหวัด\s*", "", s)
    s = " ".join(w[:1].upper() + w[1:] if w else "" for w in s.split(" "))
    return s

def clean_unit(u: Any) -> str:
    s = clean_text(u)
    # Return the exact string "พันตัน" if it matches case-insensitively
    if s.lower() == "พันตัน".lower():
        return "พันตัน"
    return s

def clean_attrib(a: Any) -> str:
    s = clean_text(a)
    return ATTR_MAP.get(s, s).strip().lower()

def parse_value(v: Any) -> Optional[float]:
    if pd.isna(v):
        return None
    s = clean_text(v)
    if not s:
        return None
    s = NUM_RE.sub("", s).replace(",", "")
    if s in {"", "-", ".", "-.", ".-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def convert_value_and_unit(value: float, unit: str) -> tuple[float, str]:
    """Convert value based on unit and return the converted value and standardized unit."""
    if pd.isna(value):
        return value, unit
    
    # Check for พันตัน with exact match
    if unit == "พันตัน":
        print(f"Converting {value} พันตัน to {value * 1000} ตัน")  # Debug print
        return value * 1000, "ตัน"
    return value, unit

def clean_slim_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=TARGET_ORDER)

    out = pd.DataFrame()
    out["id"] = df["id"].apply(clean_text)
    out["year"] = df["year"].apply(clean_year)
    out["province"] = df["province"].apply(clean_province)
    out["commod"] = df["commod"].apply(clean_text)
    out["subcommod"] = df["subcommod"].apply(clean_text)
    out["atrriburte"] = df["atrriburte"].apply(clean_attrib)
    
    # First clean the values and units
    out["value"] = df["value"].apply(parse_value)
    out["unit"] = df["unit"].apply(clean_unit)
    
    # Then convert values based on units (e.g., พันตัน -> ตัน)
    converted_values = []
    converted_units = []
    for val, unit in zip(out["value"], out["unit"]):
        new_val, new_unit = convert_value_and_unit(val, unit)
        converted_values.append(new_val)
        converted_units.append(new_unit)
    
    out["value"] = converted_values
    out["unit"] = converted_units

    # Filter and clean up
    out = out[out["atrriburte"].isin(FINAL_ATTR_SET)].copy()
    out = out.dropna(subset=["value"]).copy()

    for c in ["province", "commod", "subcommod", "atrriburte", "unit", "id"]:
        out[c] = out[c].apply(lambda x: x if x is not None else "").astype(str).str.strip()

    out = out[TARGET_ORDER].drop_duplicates().reset_index(drop=True)
    return out

# ---------------------- PRUNE ----------------------

def prune_outdir(outdir: str, group: str, parquet_saved: bool):
    """ลบทุกไฟล์ใน outdir ยกเว้นไฟล์รวม ALL_{group}_SLIM_CLEAN_ATTR.(csv|parquet)"""
    keep = {f"ALL_{group}_SLIM_CLEAN_ATTR.csv"}
    if parquet_saved:
        keep.add(f"ALL_{group}_SLIM_CLEAN_ATTR.parquet")

    for name in os.listdir(outdir):
        path = os.path.join(outdir, name)
        if os.path.isfile(path) and name not in keep:
            try:
                os.remove(path)
            except Exception as e:
                print(f"⚠️ ลบไฟล์ไม่สำเร็จ: {name} → {e}")

# ---------------------- Main dumping ----------------------

def dump_group_slim_clean(group="production", outdir="oae_prod_slim", include_non_datastore=True):
    os.makedirs(outdir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    catalog_rows = []
    combined_slim_clean = []

    print(f"🔎 Inspecting group: {group}")
    for ds in iter_all_datasets_in_group(group=group):
        ds_title = ds.get("title") or ds.get("name")
        ds_name = ds.get("name")
        print(f"\n📦 Dataset: {ds_title} (name={ds_name})")

        resources = ds.get("resources", []) or []
        if not resources:
            print("  • ไม่มี resources")
            continue

        for r in resources:
            rid = r.get("id")
            rname = r.get("name") or ""
            fmt = (r.get("format") or "").upper()
            url = r.get("url") or ""
            active = r.get("datastore_active", False)

            print(f"  → Resource: {rname} [{fmt}] rid={rid} datastore_active={active}")

            field_ids = []
            if active and rid:
                try:
                    fields = datastore_fields(rid)
                    field_ids = [f.get("id") for f in fields if f.get("id")]
                except Exception as e:
                    print(f"    ⚠️ อ่าน fields ไม่ได้: {e}")

            catalog_rows.append({
                "dataset": ds_title,
                "dataset_name": ds_name,
                "resource_name": rname,
                "resource_id": rid,
                "format": fmt,
                "datastore_active": active,
                "fields": "|".join(field_ids),
                "n_fields": len(field_ids),
                "preview_api": f"{CKAN}/datastore_search?resource_id={rid}&limit=5" if active and rid else "",
                "download_url": url,
            })

            slim_df = None

            # 1) Try DataStore
            if active and rid:
                try:
                    records = datastore_fetch_all(rid, page_size=50000)
                    raw = pd.DataFrame(records)
                    slim_df = slim_dataframe(raw)
                    print(f"    ✅ datastore pulled rows={len(raw)} slim_rows={len(slim_df)}")
                except Exception as e:
                    print(f"    ❌ datastore pull error: {e}")

            # 2) File fallback for non-datastore
            if slim_df is None and include_non_datastore and url:
                try:
                    resp = http_get(url)
                    content = resp.content
                    if fmt in {"CSV", ""} or url.lower().endswith(".csv"):
                        raw = pd.read_csv(io.BytesIO(content), dtype=str, encoding="utf-8", on_bad_lines="skip")
                    elif fmt in {"XLSX", "XLS"} or url.lower().endswith((".xlsx", ".xls")):
                        raw = pd.read_excel(io.BytesIO(content), dtype=str, engine="openpyxl")
                    else:
                        raw = None
                    if raw is not None:
                        slim_df = slim_dataframe(raw)
                        print(f"    ✅ file pulled rows={len(raw)} slim_rows={len(slim_df)}")
                except Exception as e:
                    print(f"    ⏭️ skip file pull: {e}")

            # Save per-resource SLIM + CLEAN (with attr filter)
            if isinstance(slim_df, pd.DataFrame) and not slim_df.empty:
                slim_clean = clean_slim_df(slim_df)
                if slim_clean.empty:
                    print("    ⚠️ no rows after clean (attr filter or value parse)")
                    continue
                fname = f"{safe_filename(rname or ds_title)}__{(rid or '')[:8]}__SLIM_CLEAN_ATTR.csv"
                out_path = os.path.join(outdir, fname)
                slim_clean.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
                print(f"    💾 saved: {out_path} (rows={len(slim_clean)})")
                combined_slim_clean.append(slim_clean)
            else:
                print("    ⚠️ no slim rows (columns not matched or empty data)")

    # save catalog (จะถูกลบทิ้งภายหลังโดย prune)
    df_catalog = pd.DataFrame(catalog_rows)
    cat_csv = os.path.join(outdir, f"catalog_{group}_{ts}.csv")
    cat_json = os.path.join(outdir, f"catalog_{group}_{ts}.json")
    df_catalog.to_csv(cat_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df_catalog.to_json(cat_json, orient="records", indent=2, force_ascii=False)
    print(f"\n🗂️ Catalog saved:\n   - {cat_csv}\n   - {cat_json}")

    # save combined (ONLY production/value)
    parquet_ok = False
    if combined_slim_clean:
        df_all = pd.concat(combined_slim_clean, ignore_index=True)
        df_all = df_all[TARGET_ORDER].drop_duplicates().reset_index(drop=True)
        all_csv = os.path.join(outdir, f"ALL_{group}_SLIM_CLEAN_ATTR.csv")
        all_parquet = os.path.join(outdir, f"ALL_{group}_SLIM_CLEAN_ATTR.parquet")
        df_all.to_csv(all_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        try:
            df_all.to_parquet(all_parquet, index=False)
            parquet_ok = True
            print(f"📦 Combined saved:\n   - {all_csv} (rows={len(df_all)})\n   - {all_parquet} (rows={len(df_all)})")
        except Exception as e:
            print(f"📦 Combined saved:\n   - {all_csv} (rows={len(df_all)})\n   ⚠️ Parquet not saved (pyarrow required): {e}")
        # 🔥 เก็บเฉพาะไฟล์รวม ลบที่เหลือทั้งหมดใน outdir
        prune_outdir(outdir, group, parquet_saved=parquet_ok)

        # 🔄 Convert to advisor-approved format then upload
        approved_csv = os.path.join(outdir, "oae_approved.csv")
        convert_to_approved_format(all_csv, approved_csv)
        upload_csv_to_supabase(approved_csv, "oae.csv")
    else:
        print("\n⚠️ No combined data (maybe no rows matched production/value).")

# ---------------------- CLI ----------------------

def main():
    ap = argparse.ArgumentParser(description="Auto pull OAE production → slim 8 cols → clean → keep only production/value.")
    ap.add_argument("--group", default="production", help="CKAN group (default: production)")
    ap.add_argument("--outdir", default="oae_prod_slim", help="Output directory")
    ap.add_argument("--no-file-fallback", action="store_true",
                    help="Do NOT download CSV/XLSX when datastore is inactive")
    args = ap.parse_args()

    dump_group_slim_clean(
        group=args.group,
        outdir=args.outdir,
        include_non_datastore=not args.no_file_fallback
    )

if __name__ == "__main__":
    main()