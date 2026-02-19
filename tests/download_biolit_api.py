import requests
import json
import os

os.makedirs("data/raw", exist_ok=True)
url = "https://biolit.fr/wp-json/biolitapi/v1/observations/all"
per_page = 1000
page = 1
total = 0

os.makedirs("data/raw", exist_ok=True)

while True:
    params = {"per_page": per_page, "page": page}
    print(f"Téléchargement page {page}...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if not data:
        print("Aucune donnée supplémentaire. Arrêt du téléchargement.")
        break
    with open(f"data/raw/biolit_page_{page}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    total += len(data)
    page += 1

print(f"Téléchargement terminé. {total} observations téléchargées.")
