# ============================================================
# 1. Kütüphaneler
# ============================================================
import requests
import pandas as pd
import time
import random
import json
from deltalake import write_deltalake, DeltaTable

# ============================================================
# 2. Log Fonksiyonu
# ============================================================
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

# ============================================================
# 3. IMDb API Ayarları
# ============================================================
API_URL = "https://imdb236.p.rapidapi.com/api/imdb/"
API_HEADERS = {
    "x-rapidapi-host": "imdb236.p.rapidapi.com",
    "x-rapidapi-key": "<YOUR_RAPIDAPI_KEY>"
}

# ============================================================
# 4. Yardımcı Fonksiyonlar
# ============================================================
def fetch_with_retry(movie_id, retries=5):
    for attempt in range(retries):
        try:
            r = requests.get(f"{API_URL}{movie_id}", headers=API_HEADERS, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log(f"Hata ({movie_id}) -> {e}")
        time.sleep(1 + attempt * 0.5)
    return None


def fetch_in_batches(movie_ids, batch_size=20):
    all_data = []
    for i in range(0, len(movie_ids), batch_size):
        batch = movie_ids[i:i+batch_size]
        log(f"Batch {i//batch_size + 1}/{(len(movie_ids)//batch_size)+1} çekiliyor...")
        for mid in batch:
            data = fetch_with_retry(mid)
            if data:
                all_data.append(data)
        time.sleep(random.uniform(2, 4))
    return all_data

# ============================================================
# 5. Movie ID Listesi
# ============================================================
table_path = "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/stg/stg_contentid_version0"
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}

dt = DeltaTable(table_path, storage_options=storage_options)
movie_ids = dt.to_pyarrow_table().to_pandas()['ID']

# ============================================================
# 6. API'den Çekme
# ============================================================
log("🎬 IMDb verileri çekiliyor...")
raw_data = fetch_in_batches(movie_ids, 10)
log(f"Toplam {len(raw_data)} içerik alındı.")

raw_df = pd.json_normalize(raw_data)

# ============================================================
# 7. Kolon Seçimi
# ============================================================
single_cols = [
    "id","type","url","primaryTitle","description","primaryImage","trailer",
    "contentRating","startYear","endYear","budget","grossWorldwide",
    "runtimeMinutes","averageRating","numVotes","totalSeasons","totalEpisodes"
]

list_cols = [
    "interests","countriesOfOrigin","spokenLanguages",
    "filmingLocations","genres"
]

dict_cols = [
    "directors","writers","cast","productionCompanies"
]

raw_df = raw_df[single_cols + list_cols + dict_cols].rename(columns={'id':'content_id'})

# ============================================================
# 8. Liste Kolonlarını Normalize Et
# ============================================================
def normalize_list_col(df, column):
    exploded = df[['content_id', column]].explode(column)
    exploded = exploded[exploded[column].notna()].reset_index(drop=True)

    dim_df = exploded[[column]].drop_duplicates().reset_index(drop=True)
    dim_df[f"{column}_id"] = range(1, len(dim_df)+1)

    merged = exploded.merge(dim_df, on=column, how='left')
    return merged[['content_id', f"{column}_id"]], dim_df


dim_tables = {}
fact_tables = {}

for col in list_cols:
    fact, dim = normalize_list_col(raw_df, col)
    dim_tables[col] = dim
    fact_tables[col] = fact[['content_id', f"{col}_id"]]

# ============================================================
# 9. Dict Kolonları Normalize Et
# ============================================================
def _stringify_list_or_dict(x):
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("name", "fullName", "title"):
            if k in x:
                return x[k]
        return json.dumps(x)
    if isinstance(x, list):
        return ", ".join([_stringify_list_or_dict(i) for i in x])
    return str(x) if x is not None else ""

def explode_dict_col(df, column):
    exploded = df[['content_id', column]].explode(column).dropna(subset=[column])

    exploded_fields = exploded[column].apply(lambda x: x if isinstance(x, dict) else {})
    exploded_fields = exploded_fields.apply(pd.Series)

    exploded_flat = pd.concat([exploded.drop(columns=[column]), exploded_fields], axis=1)

    for c in exploded_flat.columns:
        if c != "content_id":
            exploded_flat[c] = exploded_flat[c].apply(_stringify_list_or_dict)

    dim_cols = [c for c in exploded_flat.columns if c != 'content_id']
    dim_df = exploded_flat[dim_cols].drop_duplicates().reset_index(drop=True)

    fact = exploded_flat.merge(dim_df, on=dim_cols, how='left')

    fact.rename(columns={'id':f"{column}_id"}, inplace=True)

    if column != 'cast':
        return fact[['content_id', f"{column}_id"]], dim_df
    else:
        return fact[['content_id', f"{column}_id","characters","job"]], dim_df


for col in dict_cols:
    fact, dim = explode_dict_col(raw_df, col)
    dim_tables[col] = dim
    if col != 'cast':
        fact_tables[col] = fact[['content_id', f"{col}_id"]]
    else:
        fact_tables[col] = fact[['content_id', f"{col}_id",'characters','job']]

# ============================================================
# 10. DimContent Oluşturma
# ============================================================
def build_dim_content(raw_df):
    df = raw_df.copy()

    for col in list_cols:
        df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else "")

    for col in dict_cols:
        df[col] = df[col].apply(lambda x: ', '.join([d.get('name') or d.get('fullName') for d in x]) if isinstance(x, list) else "")
        df.rename(columns={col: f"{col}_fullName"}, inplace=True)

    keep_cols = [
        'content_id','type','primaryTitle','description','primaryImage','trailer',
        'contentRating','startYear','endYear','budget','grossWorldwide',
        'runtimeMinutes','averageRating','numVotes','totalSeasons','totalEpisodes'
    ] + list_cols + [f"{col}_fullName" for col in dict_cols]

    df = df[keep_cols]

    for c in df.columns:
        if df[c].dtype == 'O':
            df[c] = df[c].fillna("").astype(str)
        else:
            df[c] = df[c].replace("", pd.NA)

    return df

dim_content = build_dim_content(raw_df)

# ============================================================
# 11. Delta'ya Yazma
# ============================================================
base_dim = "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/dbo/"
base_bridge = "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/brg/"

write_deltalake(base_dim + "DimContent", dim_content, mode="overwrite")

for name, df in dim_tables.items():
    write_deltalake(base_dim + "Dim_" + name, df, mode="overwrite")

for name, df in fact_tables.items():
    write_deltalake(base_bridge + "bridge_" + name, df, mode="overwrite")
