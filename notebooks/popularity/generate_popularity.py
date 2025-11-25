# ============================================================
# 1. Kütüphaneler
# ============================================================
import requests
import pandas as pd
import time
import random
import numpy as np
from deltalake import DeltaTable, write_deltalake
from datetime import datetime

# ============================================================
# 2. Bağlantılar ve Tokenlar
# ============================================================
table_path = 'abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/stg/stg_contentid_version0'
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}

dt = DeltaTable(table_path, storage_options=storage_options)
contentIDs = dt.to_pyarrow_table().to_pandas()['ID'].tolist()

TMDB_api_key = "<YOUR_TMDB_TOKEN>"
IMDB_api_key = "<YOUR_RAPIDAPI_KEY>"

TMDB_header = {
    "Authorization": f"Bearer {TMDB_api_key}",
    "Accept": "application/json"
}

IMDB_header = {
    "x-rapidapi-host": "imdb236.p.rapidapi.com",
    "x-rapidapi-key": IMDB_api_key
}

TMDB_base_url = "https://api.themoviedb.org/3/"
IMDB_base_url = "https://imdb236.p.rapidapi.com/api/imdb/"

# ============================================================
# 3. Retry fonksiyonu
# ============================================================
def fetch_with_retry(DB_type, movie_id, retries=5):
    for attempt in range(retries):
        try:
            if DB_type == "IMDB":
                url = f"{IMDB_base_url}{movie_id}/tmdb-id"
                r = requests.get(url, headers=IMDB_header, timeout=10)
            else:
                url = f"{TMDB_base_url}{movie_id}"
                r = requests.get(url, headers=TMDB_header, timeout=10)

            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                break
        except Exception as e:
            print(f"[{DB_type}] Hata: {e}")

        time.sleep(1 + attempt * 0.5)
    return None

# ============================================================
# 4. Batch fonksiyonu
# ============================================================
def fetch_in_batches(DB, movie_ids, batch_size=50):
    all_data = {}
    for i in range(0, len(movie_ids), batch_size):
        batch = movie_ids[i:i+batch_size]
        for mid in batch:
            data = fetch_with_retry(DB_type=DB, movie_id=mid)
            if data:
                all_data[mid] = data
        time.sleep(random.uniform(2, 4))
    return all_data

# ============================================================
# 5. IMDb → TMDB ID eşleştir
# ============================================================
IMDB_data = fetch_in_batches(DB="IMDB", movie_ids=contentIDs)

id_map = {}
tmdb_ids = []
for cid, data in IMDB_data.items():
    if isinstance(data, dict) and "tmdbId" in data:
        id_map[cid] = {"tmdbId": str(data["tmdbId"])}
        tmdb_ids.append(str(data["tmdbId"]))
    else:
        id_map[cid] = {"tmdbId": None}

# ============================================================
# 6. TMDB verilerini çek
# ============================================================
TMDB_data = fetch_in_batches(DB="TMDB", movie_ids=tmdb_ids)

# ============================================================
# 7. Popularity eşleşmesi
# ============================================================
for cid, val in id_map.items():
    tmdb_id = val["tmdbId"]
    if tmdb_id and tmdb_id in TMDB_data:
        tmdb_json = TMDB_data[tmdb_id]
        val["popularity"] = tmdb_json.get("popularity", None)
    else:
        val["popularity"] = None

# ============================================================
# 8. DataFrame oluştur
# ============================================================
df = pd.DataFrame([{"ID": cid, "popularity": v["popularity"]} for cid, v in id_map.items()])
df.replace(["", " ", "NaN", "nan"], np.nan, inplace=True)
df.dropna(inplace=True)
df.reset_index(drop=True, inplace=True)

df['loadDate'] = datetime.today().date()
df['popularity_rank'] = df['popularity'].rank(method='dense', ascending=False).astype("int32")

write_deltalake(
    "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/dbo/FactContentPopularity",
    df,
    mode="append",
    schema_mode="additive"
)
