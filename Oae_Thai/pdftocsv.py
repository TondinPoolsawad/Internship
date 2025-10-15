'''This code changes pdfs that were xlsx before to .csv file'''
import pdfplumber
import pandas as pd
import os
import re

input_root = "pdfs"
output_root = "csv_output"
os.makedirs(output_root, exist_ok=True)

for root, dirs, files in os.walk(input_root):
    for file in files:
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(root, file)
        relative_path = os.path.relpath(root, input_root)
        output_dir = os.path.join(output_root, relative_path)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, file.replace(".pdf", ".csv"))

        print(f"\nProcessing: {pdf_path}")

        rows = []
        found = False

        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                table = page.extract_table()
                if not table:
                    continue

                df = pd.DataFrame(table)
                df = df.applymap(lambda x: re.sub(r"\s+", " ", x.strip()) if isinstance(x, str) else x)

                mask = df.apply(lambda r: r.astype(str).str.contains("รวม.?ทั้ง.?ประเทศ").any(), axis=1)
                if mask.any():
                    idx = mask[mask].index[0]
                    found = True

                    header_rows = df.iloc[:2].values.tolist()
                    country_row = df.iloc[idx].values.tolist()
                    rows = header_rows + [country_row]
                    print(f"Found on page {page_num}")
                    break

        if not found:
            pass
        else:
            pd.DataFrame(rows).to_csv(output_path, index=False, header=False, encoding="utf-8-sig")
            print(f"Saved {output_path}")
