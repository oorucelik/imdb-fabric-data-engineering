import requests
import pandas as pd
import time
import random
from deltalake import write_deltalake, DeltaTable

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

API_URL = "https://imdb236.p.rapidapi.com/api/imdb/"
API_HEADERS = {
    "x-rapidapi-host": "imdb236.p.rapidapi.com",
    "x-rapidapi-key": "<YOUR_RAPIDAPI_KEY>"
}

def fetch_with_retry(movie_id, retries=5):
    for attempt in range(retries):
        try:
            r = requests.get(f"{API_URL}{movie_id}", headers=API_HEADERS, timeout=10)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        time.sleep(1 + attempt * 0.5)
    return None

def fetch_in_batches(movie_ids, batch_size=20):
    all_data = []
    for i in range(0, len(movie_ids), batch_size):
        for mid in movie_ids[i:i+batch_size]:
            result = fetch_with_retry(mid)
            if result:
                all_data.append(result)
        time.sleep(random.uniform(2, 4))
    return all_data

table_path = "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/stg/stg_new_contentID"
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}

dt = DeltaTable(table_path, storage_options=storage_options)
movie_ids = dt.to_pyarrow_table().to_pandas()['ID']

log("New content metadata is being fetched...")
raw_data = fetch_in_batches(movie_ids)
raw_df = pd.json_normalize(raw_data)

single_cols = [
    "id","type","url","primaryTitle","description","primaryImage","trailer",
    "contentRating","startYear","endYear","budget","grossWorldwide",
    "runtimeMinutes","averageRating","numVotes","totalSeasons","totalEpisodes"
]

list_cols = ["interests","countriesOfOrigin","spokenLanguages","filmingLocations","genres"]
dict_cols = ["directors","writers","cast","productionCompanies"]

raw_df = raw_df[single_cols + list_cols + dict_cols].rename(columns={'id':'content_id'})

def build_dim_content(df):
    new_df = df.copy()
    for col in list_cols:
        new_df[col] = new_df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else "")
    for col in dict_cols:
        new_df[col] = new_df[col].apply(lambda x: ', '.join([d.get('name') for d in x]) if isinstance(x, list) else "")
        new_df.rename(columns={col: f"{col}_fullName"}, inplace=True)
    return new_df

dim_content = build_dim_content(raw_df)

base_dim = "abfss://IMDB_DEV@onelake.dfs.fabric.microsoft.com/movieLakehouse.Lakehouse/Tables/dbo/"

write_deltalake(base_dim + "dimcontenthistorical", dim_content, mode="append")
