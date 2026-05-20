# Supabase Upload Snippets

Project URL: `https://mzzyjtlrbbwqdxpenwne.supabase.co`  
Bucket: `csvs`

You need your **service_role key** (not the anon key) to upload.  
Get it from: Supabase dashboard → Project Settings → API → `service_role` secret

---

## 1. OAE + Fisheries (Python)

Add this to the end of `oae_api.py` and `fisheries_api.py`:

```python
import os
from supabase import create_client

SUPABASE_URL = "https://mzzyjtlrbbwqdxpenwne.supabase.co"
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # set as env var, never hardcode

def upload_csv_to_supabase(local_path: str, bucket_filename: str):
    """Upload a local CSV to Supabase Storage, overwriting the existing file."""
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    with open(local_path, "rb") as f:
        data = f.read()
    # remove first so upsert works cleanly
    client.storage.from_("csvs").remove([bucket_filename])
    client.storage.from_("csvs").upload(
        path=bucket_filename,
        file=data,
        file_options={"content-type": "text/csv"}
    )
    print(f"✅ Uploaded {local_path} → csvs/{bucket_filename}")

# Call at the end of your script, e.g.:
# upload_csv_to_supabase("output/oae.csv", "oae.csv")
# upload_csv_to_supabase("output/fisheries.csv", "fisheries.csv")
```

Install: `pip install supabase`

---

## 2. Customs + DEDE (Node.js)

Add this to `scraper.js` (already uses `undici` so just add at the bottom):

```js
import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "node:fs";

const SUPABASE_URL = "https://mzzyjtlrbbwqdxpenwne.supabase.co";
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY; // set as env var

async function uploadCsvToSupabase(localPath, bucketFilename) {
  const client = createClient(SUPABASE_URL, SUPABASE_KEY);
  const data = readFileSync(localPath);
  // remove first so upsert works cleanly
  await client.storage.from("csvs").remove([bucketFilename]);
  const { error } = await client.storage.from("csvs").upload(bucketFilename, data, {
    contentType: "text/csv",
  });
  if (error) throw error;
  console.log(`✅ Uploaded ${localPath} → csvs/${bucketFilename}`);
}

// Call at the end of main(), e.g.:
// await uploadCsvToSupabase("downloads/customs.csv", "customs.csv");
// await uploadCsvToSupabase("downloads/dede.csv", "dede.csv");
```

Install: `npm install @supabase/supabase-js`

---

## 3. Setting the env var

**Windows (PowerShell):**
```powershell
$env:SUPABASE_SERVICE_KEY = "your-service-role-key-here"
```

**Mac/Linux:**
```bash
export SUPABASE_SERVICE_KEY="your-service-role-key-here"
```

Or put it in a `.env` file and use `python-dotenv` / `dotenv` for Node — just make sure `.env` is in `.gitignore`.

---

## 4. File names expected by the website

| Source     | Upload as        |
|------------|------------------|
| Customs    | `customs.csv`    |
| OAE        | `oae.csv`        |
| Fisheries  | `fisheries.csv`  |
| DEDE       | `dede.csv`       |
