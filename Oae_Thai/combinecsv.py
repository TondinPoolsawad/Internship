#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, csv, time, argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
import pandas as pd

CKAN = "https://catalog.oae.go.th/api/3/action"

def safe_filename(s: str) -> str:
    keep = "-_()[]{} ."
    return "".join(ch if ch.isalnum() or ch in keep else "_" for ch in s).strip()

def http_get(url: str, params: dict=None, timeout: int=60, retries: int=3, backoff: float=1.5):
    """GET ที่มี retry/backoff ง่ายๆ"""
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(backoff ** (i+1))

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
    # limit=0 เพื่อตะกร้าฟิลด์; ถ้าไม่ได้จะลอง limit=1
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
    """ดึง records ทั้งหมดด้วยการ paginate ตาม offset"""
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

def dump_group(group="production", outdir="oae_production_dump"):
    os.makedirs(outdir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    catalog_rows = []
    combined_records = []

    print(f"🔎 Inspecting group: {group}")
    for ds in iter_all_datasets_in_group(group=group):
        ds_title = ds.get("title") or ds.get("name")
        ds_name = ds.get("name")
        print(f"\n📦 Dataset: {ds_title}  (name={ds_name})")
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

            print(f"  → Resource: {rname} [{fmt}]  rid={rid}  datastore_active={active}")

            field_ids = []
            if active:
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
                "preview_api": f"{CKAN}/datastore_search?resource_id={rid}&limit=5" if active else "",
                "download_url": url,
            })

            # ดึงข้อมูลจริงเฉพาะที่ query ได้ (datastore_active)
            if active and rid:
                try:
                    recs = datastore_fetch_all(rid, page_size=50000)
                    # enrich meta
                    for rec in recs:
                        rec["_source_dataset"] = ds_title
                        rec["_resource_name"] = rname
                        rec["_resource_id"] = rid
                        rec["_pulled_at"] = datetime.utcnow().isoformat()
                    # save per-resource CSV
                    df = pd.DataFrame(recs)
                    if df.empty:
                        # กันไฟล์ว่างเกินไป
                        df = pd.DataFrame([{
                            "_source_dataset": ds_title,
                            "_resource_name": rname,
                            "_resource_id": rid,
                            "_pulled_at": datetime.utcnow().isoformat()
                        }])
                    fname = f"{safe_filename(rname or ds_title)}__{rid[:8]}.csv"
                    out_path = os.path.join(outdir, fname)
                    df.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
                    print(f"    ✅ saved {out_path} (rows={len(df)})")
                    combined_records.extend(df.to_dict(orient="records"))
                except Exception as e:
                    print(f"    ❌ pull error: {e}")
            else:
                print("    ⏭️ ข้ามการดึง (ไม่มี DataStore) — ถ้าต้องโหลดไฟล์ดิบใช้ download_url")

    # save catalog
    df_catalog = pd.DataFrame(catalog_rows)
    catalog_csv = os.path.join(outdir, f"catalog_{group}_{ts}.csv")
    catalog_json = os.path.join(outdir, f"catalog_{group}_{ts}.json")
    df_catalog.to_csv(catalog_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    df_catalog.to_json(catalog_json, orient="records", indent=2, force_ascii=False)
    print(f"\n🗂️ Catalog saved:\n   - {catalog_csv}\n   - {catalog_json}")

    # save combined
    if combined_records:
        df_all = pd.DataFrame(combined_records)
        all_csv = os.path.join(outdir, f"ALL_{group}_combined.csv")
        all_parquet = os.path.join(outdir, f"ALL_{group}_combined.parquet")
        df_all.to_csv(all_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        try:
            df_all.to_parquet(all_parquet, index=False)
            print(f"📦 Combined saved:\n   - {all_csv} (rows={len(df_all)})\n   - {all_parquet} (rows={len(df_all)})")
        except Exception as e:
            print(f"📦 Combined saved:\n   - {all_csv} (rows={len(df_all)})\n   ⚠️ Parquet not saved (install pyarrow): {e}")
    else:
        print("\n⚠️ ไม่มีข้อมูลรวม (อาจเพราะไม่มี resource ที่ datastore_active)")

def main():
    ap = argparse.ArgumentParser(description="Dump all production data (inspect + fetch all) without hardcoding")
    ap.add_argument("--group", default="production", help="CKAN group (default: production)")
    ap.add_argument("--outdir", default="oae_production_dump", help="output directory")
    args = ap.parse_args()
    dump_group(group=args.group, outdir=args.outdir)

if __name__ == "__main__":
    main()
