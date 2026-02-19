import polars as pl
import json
from pathlib import Path
import pandas as pd

dfs = []
for i in range(1, 26):
    file_path = Path(f"data/raw/biolit_page_{i}.json")
    if file_path.exists():
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
            if data:
                dfs.append(pl.DataFrame(data))

if dfs:
    df = pl.concat(dfs, how="vertical")
    print(f"Nombre de lignes : {len(df)}")
    print("Colonnes disponibles :")
    print(df.columns)
    print("\nAperçu des données :")
    print(df.head())
else:
    print("Aucun fichier JSON chargé.")

df = df.with_columns(pl.col("id").cast(pl.Utf8).str.strip_chars())

df_csv = df_csv = pl.read_csv("data/export_biolit.csv").with_columns(pl.col("ID - observation").cast(pl.Utf8).str.strip_chars())


ids_csv = set(df_csv["ID - observation"])
ids_json = set(df["id"])



# IDs présents dans CSV mais pas dans JSON
missing_in_json = ids_csv - ids_json

# IDs présents dans JSON mais pas dans CSV
missing_in_csv = ids_json - ids_csv

print(f"IDs dans CSV mais pas dans JSON : {len(missing_in_json)}")
print(f"IDs dans JSON mais pas dans CSV : {len(missing_in_csv)}")
print(df_csv.shape)




diff_csv = df_csv.join(
    df.select("id"),
    left_on="ID - observation",
    right_on="id",
    how="anti"
)
print("Présents dans CSV mais pas dans JSON :")
print(diff_csv.shape)
print(diff_csv.head())

diff_json = df.join(
    df_csv.select("ID - observation"),
    left_on="id",
    right_on="ID - observation",
    how="anti"
)
print("Présents dans JSON mais pas dans CSV :")
print(diff_json.shape)
print(diff_json.select("id").head())
