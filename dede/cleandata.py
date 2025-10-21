# labubu.py — TPES + flows extractor for DEDE Energy Balance (Physical)
# - Scans downloads/energy_balance/<year>/*.xlsx (2015–2024)
# - Fixes "Wood fuel" by summing TPES column (41) + (49)
# - Prefers TOTAL/รวม for Coal flows
# - Writes a single CSV: Name, 2015..2024
#
# Run:
#   python labubu.py --only-year 2015 --debug
#   python labubu.py

import os, re, sys, argparse
import pandas as pd
import numpy as np
from glob import glob

# ---------- Config ----------
INPUT_ROOT = os.path.join(os.getcwd(), "downloads", "energy_balance")
OUTPUT_DIR = os.path.join(os.getcwd(), "downloads")
MIN_YEAR = 2015
HOP_RIGHT_MAX = 4
SHEET_CANDIDATES = ("Physical", "มค-ธค", "Jan-Dec", "January-December")

# ---------- Utilities ----------
def norm(v):
    if pd.isna(v): return ""
    return str(v).strip()

def is_numlike(v):
    if isinstance(v, (int, float)) and not pd.isna(v): return True
    s = norm(v).replace(",", "")
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", s))

def to_float(v):
    if isinstance(v, (int, float)) and not pd.isna(v): return float(v)
    s = norm(v).replace(",", "")
    return float(s) if re.fullmatch(r"-?\d+(\.\d+)?", s) else np.nan

def thai_or_greg_year(s):
    m = re.search(r"(20\d{2}|25\d{2})", str(s))
    if not m: return None
    y = int(m.group(1))
    return y - 543 if y >= 2500 else y

def year_from_filename(name):
    ys = [thai_or_greg_year(tok) for tok in re.findall(r"(20\d{2}|25\d{2})", name)]
    ys = [y for y in ys if y]
    return min(ys) if ys else None

def read_sheet_matrix(xlsx_path):
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    # try exact candidates then fallbacks containing those tokens
    for cand in SHEET_CANDIDATES:
        if cand in xl.sheet_names:
            return xl.parse(cand, header=None).values.tolist(), cand
    for s in xl.sheet_names:
        if any(c.lower() in s.lower() for c in SHEET_CANDIDATES):
            return xl.parse(s, header=None).values.tolist(), s
    # default: first sheet
    return xl.parse(xl.sheet_names[0], header=None).values.tolist(), xl.sheet_names[0]

def find_tpes_row(rows):
    """Find the row with 'Total Primary Energy Supply' / 'รวมการจัดหาพลังงานขั้นต้นทั้งหมด' (Thai/EN)."""
    KEYS = [
        "รวมการจัดหาพลังงานขั้นต้นทั้งหมด",
        "รวม การ จัดหา พลังงาน ขั้นต้น",
        "total primary energy supply",
        "tpes"
    ]
    for r, row in enumerate(rows):
        line = " ".join(norm(x) for x in row[:12]).lower()
        if any(k in line for k in KEYS):
            return r
    return None

def build_col_id_map_and_units(rows, header_end):
    """
    Scan rows[0:header_end] (i.e., up to + around TPES) for '(NN)' and unit hints.
    Return:
      col_by_id: {41: colIndex, 49: colIndex, ...}  (last occurrence wins)
      units: {colIndex: 'thousand tons' | 'tons' | 'mmscf' | None}
    """
    col_by_id = {}
    units = {}

    def unit_from_text(s):
        s = s.lower()
        if "พันตัน" in s or "thousand ton" in s or "thousand tons" in s:
            return "thousand tons"
        if "ตัน" in s and "พันตัน" not in s:
            return "tons"
        if "ล้านลูกบาศก์ฟุต" in s or "mm scf" in s or "mmscf" in s:
            return "mmscf"
        return None

    height = min(max(0, header_end), len(rows))
    width = max((len(r) for r in rows[:height]), default=0)

    for r in range(height):
        for c in range(width):
            s = norm(rows[r][c])
            if not s: continue
            u = unit_from_text(s)
            if u: units[c] = u
            for m in re.finditer(r"\((\d+)\)", s):
                nn = int(m.group(1))
                col_by_id[nn] = c  # keep last (deepest)

    return col_by_id, units

def first_num_right(rows, r, c, hop=HOP_RIGHT_MAX):
    if r is None or c is None: return np.nan
    if r < 0 or r >= len(rows): return np.nan
    row = rows[r]
    for cc in range(c, min(c + 1 + hop, len(row))):
        if is_numlike(row[cc]):
            return to_float(row[cc])
    return np.nan

def scale_by_unit(val, unit):
    if not np.isfinite(val): return val
    if unit == "thousand tons":
        return val * 1000.0
    return val  # leave others as-is (we only label as 'ton' for display consistency)

def collect_headers(rows, scan_rows):
    width = max((len(r) for r in rows[:scan_rows]), default=0)
    headers = [""] * width
    for c in range(width):
        parts = []
        for r in range(min(scan_rows, len(rows))):
            if c < len(rows[r]):
                v = norm(rows[r][c])
                if v: parts.append(v)
        headers[c] = " ".join(parts).strip()
    return headers

# ---------- What to extract ----------
FLOW_SPECS = [
    ("Coal/Lignite",             r"(รวม|total).*(ลิกไนต์|lignite|coal|ถ่านหิน)|\b(lignite|coal|ถ่านหิน)\b",          "PRODUCTION", True),
    ("Coal/Lignite (Import)",    r"(รวม|total).*(ลิกไนต์|lignite|coal|ถ่านหิน)|\b(lignite|coal|ถ่านหิน)\b",          "IMPORTS",    True),
    ("Coal/Lignite (Export)",    r"(รวม|total).*(ลิกไนต์|lignite|coal|ถ่านหิน)|\b(lignite|coal|ถ่านหิน)\b",          "EXPORTS",    True),

    ("Crude Oil (Production)",   r"(crude\s*oil|น้ำมันดิบ)",                 "PRODUCTION", False),
    ("Crude Oil (Import)",       r"(crude\s*oil|น้ำมันดิบ)",                 "IMPORTS",    False),
    ("Crude Oil (Export)",       r"(crude\s*oil|น้ำมันดิบ)",                 "EXPORTS",    False),

    ("Condensate (Production)",  r"(condensate|คอนเดนเซต|คอนเดนเสท)",       "PRODUCTION", False),
    ("Condensate (Import)",      r"(condensate|คอนเดนเซต|คอนเดนเสท)",       "IMPORTS",    False),
    ("Condensate (Export)",      r"(condensate|คอนเดนเซต|คอนเดนเสท)",       "EXPORTS",    False),

    ("Natural gas (Production)", r"(natural\s*gas|ก๊าซธรรมชาติ)",           "PRODUCTION", False),
    ("Natural gas (Import)",     r"(natural\s*gas|ก๊าซธรรมชาติ)",           "IMPORTS",    False),
    ("Natural gas (Export)",     r"(natural\s*gas|ก๊าซธรรมชาติ)",           "EXPORTS",    False),

    ("Gasoline RON 91 Import",   r"(gasoline|gaso(h)?ol|เบนซิน|แก๊สโซฮอล์).*(ron\s*91|91)", "IMPORTS", False),
    ("Gasoline RON 91 Export",   r"(gasoline|gaso(h)?ol|เบนซิน|แก๊สโซฮอล์).*(ron\s*91|91)", "EXPORTS", False),
    ("Gasoline RON 95 Export",   r"(gasoline|gaso(h)?ol|เบนซิน|แก๊สโซฮอล์).*(ron\s*95|95)", "EXPORTS", False),

    ("HSD Import",               r"(HSD|high\s*speed\s*diesel|ดีเซลหมุนเร็ว)",             "IMPORTS", False),
    ("LSD Export",               r"(LSD|low\s*sul(ph|f)ur\s*diesel|ดีเซล(กำมะถัน)?ต่ำ)",   "EXPORTS", False),

    ("JET FUEL Import",          r"(jet\s*fuel|ATF|อากาศยาน|เชื้อเพลิงอากาศยาน)",         "IMPORTS", False),
    ("JET FUEL Export",          r"(jet\s*fuel|ATF|อากาศยาน|เชื้อเพลิงอากาศยาน)",         "EXPORTS", False),

    ("KEROSENE Export",          r"(kerosene|ก๊าด)",                                      "EXPORTS", False),

    ("LPG Import",               r"(LPG|liquefied\s*petroleum\s*gas|ก๊าซปิโตรเลียมเหลว)",  "IMPORTS", False),
    ("LPG Export",               r"(LPG|liquefied\s*petroleum\s*gas|ก๊าซปิโตรเลียมเหลว)",  "EXPORTS", False),
]

TPES_FORMULAS = {
    "Wood fuel": [41, 49],  # (SOLID BIOMASS – ฟืน) + (Traditional renewable – ฟืน)
}

FLOW_ROW_KEYS = {
    "PRODUCTION": ["การผลิตภายในประเทศ", "domestic production", "production"],
    "IMPORTS":    ["นำเข้า", "imports", "import"],
    "EXPORTS":    ["ส่งออก", "exports", "export"],
}

def find_flow_row(rows, flow):
    keys = FLOW_ROW_KEYS[flow]
    for r, row in enumerate(rows):
        line = " ".join(norm(x) for x in row[:12]).lower()
        if any(k in line for k in keys):
            return r
    return None

def find_product_cols_by_regex(headers, product_re):
    rx = re.compile(product_re, re.I)
    return [i for i, h in enumerate(headers) if rx.search(h)]

def prefer_total_cols(cols, headers):
    if not cols: return cols
    totalish = [c for c in cols if re.search(r"(รวม|total)", headers[c], re.I)]
    return totalish or cols

# ---------- Core extraction ----------
def extract_one_year(xlsx_path, debug=False, only_year=None):
    year = year_from_filename(os.path.basename(xlsx_path))
    if not year or year < MIN_YEAR:
        return None
    if only_year and year != only_year:
        return None

    rows, sheet = read_sheet_matrix(xlsx_path)
    tpes_row = find_tpes_row(rows)
    scan_rows = (tpes_row + 2) if tpes_row is not None else 30
    headers = collect_headers(rows, scan_rows)
    col_by_id, units_by_col = build_col_id_map_and_units(rows, header_end=scan_rows)

    if debug:
        print(f"[{year}] sheet={sheet}, tpes_row={tpes_row}")

    out = {}

    # 1) TPES formulas
    for label, ids in TPES_FORMULAS.items():
        total = 0.0
        found_any = False
        for cid in ids:
            c = col_by_id.get(cid)
            raw = first_num_right(rows, tpes_row, c)
            val = scale_by_unit(raw, units_by_col.get(c))
            if np.isfinite(val):
                total += val
                found_any = True
            if debug:
                h = headers[c] if (c is not None and c < len(headers)) else ""
                print(f"[{year}] {label}: col({cid})={c}, unit={units_by_col.get(c)}, header=«{h}», raw={raw}, scaled={val}")
        out[label] = total if found_any else np.nan
        if debug and found_any:
            print(f"[{year}] {label} (TPES sum): {int(round(total)):,}")

    # 2) Flows
    flow_rows = {}
    for _, _, flow, _ in FLOW_SPECS:
        if flow not in flow_rows:
            flow_rows[flow] = find_flow_row(rows, flow)

    for label, product_re, flow, prefer_total in FLOW_SPECS:
        r = flow_rows.get(flow)
        cols = find_product_cols_by_regex(headers, product_re)
        if prefer_total:
            cols = prefer_total_cols(cols, headers)
        val = np.nan
        raw = None
        c0 = cols[0] if cols else None
        if cols and r is not None:
            raw = first_num_right(rows, r, c0)
            val = scale_by_unit(raw, units_by_col.get(c0))
        out[label] = val
        if debug:
            h = headers[c0] if c0 is not None and c0 < len(headers) else ""
            print(f"[{year}] {label}: row={r}, col={c0}, header=«{h}», raw={raw}, val={val}")

    return {"year": year, "values": out}

def gather_files(root):
    out = []
    if not os.path.isdir(root): return out
    for ent in os.listdir(root):
        p = os.path.join(root, ent)
        if os.path.isdir(p):
            out.extend([f for f in glob(os.path.join(p, "*.xlsx")) if not os.path.basename(f).startswith("~$")])
    out.extend([f for f in glob(os.path.join(root, "*.xlsx")) if not os.path.basename(f).startswith("~$")])
    return out

# ---------- CLI / main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only-year", type=int, default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    files = gather_files(INPUT_ROOT)
    if not files:
        print(f"No .xlsx found under {INPUT_ROOT}")
        sys.exit(0)

    parsed = []
    for f in files:
        item = extract_one_year(f, debug=args.debug, only_year=args.only_year)
        if item: parsed.append(item)

    if not parsed:
        print("No rows parsed.")
        sys.exit(0)

    years = sorted({p["year"] for p in parsed})

    labels = list(TPES_FORMULAS.keys()) + [spec[0] for spec in FLOW_SPECS]

    rows_out = [["Name", *years]]
    for label in labels:
        line = [label]
        for y in years:
            v = next((p["values"].get(label) for p in parsed if p["year"] == y), np.nan)
            if v is None or not np.isfinite(v):
                line.append("")
            else:
                line.append(f"{int(round(v))}")
        rows_out.append(line)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_csv = os.path.join(OUTPUT_DIR, f"energy_balance_clean_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(out_csv, "w", encoding="utf-8") as f:
        for r in rows_out:
            f.write(",".join(map(str, r)) + "\n")

    # Gaps
    gaps = []
    for label in labels:
        for y in years:
            v = next((p["values"].get(label) for p in parsed if p["year"] == y), np.nan)
            if v is None or not np.isfinite(v):
                gaps.append([label, y])

    gap_csv = os.path.join(OUTPUT_DIR, f"energy_balance_gaps_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(gap_csv, "w", encoding="utf-8") as f:
        f.write("Label,Year\n")
        for g in gaps:
            f.write(f"{g[0]},{g[1]}\n")

    print(f"✅ Wrote {out_csv}")
    if gaps:
        print(f"⚠️  Wrote gap report {gap_csv} (items I could not find).")
    else:
        print("✅ No gaps")

if __name__ == "__main__":
    main()
