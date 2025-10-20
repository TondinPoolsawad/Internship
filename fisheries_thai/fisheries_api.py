#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
กรมประมง (Fisheries CKAN)
Group: importexport
→ ดึงข้อมูลทุก resource ทั้งหมด (ยังไม่ clean)
→ รวมเป็น ALL_importexport_RAW.csv / .parquet
"""

import os, io, csv, json, time, argparse
from datetime import datetime
import requests
import pandas as pd

CKAN = "https://catalog.fisheries.go.th/api/3/action"
GROUP = "importexport"

def safe_filename(s):
    keep = "-_()[]{} ."
    return "".join(ch if ch.isalnum() or ch in keep else "_" for ch in (s or "")).strip() or "file"

def http_get(url, params=None, retries=3, backoff=1.5):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:
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

def dump_group_raw(group="importexport", outdir="fisheries_importexport_raw"):
    os.makedirs(outdir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    catalog_rows = []
    all_data = []

    print(f"📦 กำลังดึง group: {group}")
    for ds in iter_all_datasets_in_group(group):
        ds_title = ds.get("title") or ds.get("name")
        print(f"\n▶️ Dataset: {ds_title}")
        for r in ds.get("resources", []):
            rid = r.get("id")
            rname = r.get("name") or ""
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
                    print(f"    ✅ ดึงจาก datastore rows={len(df)}")
                elif url:
                    resp = http_get(url)
                    content = resp.content
                    if fmt in {"CSV", ""} or url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="utf-8", on_bad_lines="skip")
                    elif fmt in {"XLSX", "XLS"} or url.lower().endswith((".xlsx", ".xls")):
                        df = pd.read_excel(io.BytesIO(content), dtype=str, engine="openpyxl")
                    print(f"    ✅ ดาวน์โหลดไฟล์ rows={len(df)}")
            except Exception as e:
                print(f"    ⚠️ Error: {e}")

            if df is not None and not df.empty:
                df["_dataset"] = ds_title
                df["_resource"] = rname
                df["_rid"] = rid
                all_data.append(df)

    # catalog summary
    cat_df = pd.DataFrame(catalog_rows)
    cat_csv = os.path.join(outdir, f"catalog_{group}_{ts}.csv")
    cat_df.to_csv(cat_csv, index=False, encoding="utf-8-sig")
    print(f"\n📑 catalog saved: {cat_csv}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        out_csv = os.path.join(outdir, f"ALL_{group}_RAW.csv")
        out_parq = os.path.join(outdir, f"ALL_{group}_RAW.parquet")
        combined.to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        try:
            combined.to_parquet(out_parq, index=False)
        except Exception as e:
            print(f"⚠️ parquet error: {e}")
        print(f"✅ saved:\n - {out_csv}\n - {out_parq}\nรวมทั้งหมด {len(combined):,} rows")
    else:
        print("⚠️ ไม่มีข้อมูลใน group นี้")

if __name__ == "__main__":
    dump_group_raw()
