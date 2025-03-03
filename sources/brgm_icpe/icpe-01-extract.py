import geopandas as gpd
import pandas as pd
from shapely.wkt import loads as wkt_loads
from shapely.geometry import shape
from shapely.geometry import Point
import os
import requests
import sys
from utils import load_config, create_output_path, get_proxies, load_env
import duckdb
from pathlib import Path
from datetime import datetime

# Obtenir la date actuelle au format "AAAA-MM"
current_date = datetime.today().strftime("%Y-%m")

# Charger la configuration et les variables d'environnement
proxy_url = load_env()
proxies = get_proxies(proxy_url)
config = load_config()

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "brgm_icpe"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définir les chemins à partir de la configuration
BASE_PATH = config["base_path"]
SOURCE_PATH = config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]
OUTPUT_PATH = create_output_path(BASE_PATH, SOURCE_PATH, config["sources"][source]["paths"]["parquet_raw"])

print(f"Téléchargement depuis : {URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")

# Connexion à la base de données pour récupérer la liste des départements
base_path = Path.cwd()
path_db = base_path / "data" / "ngeo2024.duckdb"
path_sql = base_path / "shared" / "sql" / "query_dep.sql"

db_ngeofr = duckdb.connect(str(path_db))

with path_sql.open("r", encoding="utf-8") as file:
    sql_query = file.read()

# Récupérer la liste des départements sous forme de DataFrame
dep_df = db_ngeofr.execute(sql_query).fetchdf()
db_ngeofr.close()

departements = {dep if dep.startswith("9") else dep.zfill(2) for dep in dep_df["dep_insee"].tolist()}


def fetch_data_for_department(dept_code):
    all_data = []
    page = 1

    while True:
        params = {
            "departement": dept_code,
            "page_size": 1000,
            "page": page
        }
        response = requests.get(URL, params=params, proxies=proxies)

        if response.status_code != 200:
            print(f"❌ Erreur API pour département {dept_code} : {response.status_code}")
            return None

        data = response.json()

        # Afficher la réponse brute pour debug
        print(f"🔍 Réponse API pour {dept_code}, page {page} : {data}")

        # Vérifications de la structure de la réponse
        if "data" not in data or not isinstance(data["data"], list):
            print(f"⚠️ Pas de données valides pour le département {dept_code} (structure incorrecte)")
            break

        if not data["data"]:
            print(f"⚠️ Aucune donnée pour {dept_code} à la page {page}.")
            break

        all_data.extend(data["data"])

        total_pages = data.get("total_pages", 1)
        print(f"✅ Page {page}/{total_pages} récupérée pour {dept_code}.")

        if page >= total_pages:
            break

        page += 1

    return all_data

           





# Récupération et enregistrement des données
for dept in sorted(departements):
    print(f"Récupération des données pour le département {dept}...")
    data = fetch_data_for_department(dept)

    if not data:
        continue

    df = pd.DataFrame(data)

    # Si 'longitude' et 'latitude' existent, créer la géométrie Point
    if 'longitude' in df.columns and 'latitude' in df.columns:
        df['geom'] = df.apply(lambda row: Point(row['longitude'], row['latitude']) if pd.notnull(row['longitude']) and pd.notnull(row['latitude']) else None, axis=1)
        gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")
        dept_code_formatted = dept.zfill(3)
        file_path = os.path.join(OUTPUT_PATH, f"{current_date}-icpe-{dept_code_formatted}.parquet")
        gdf.to_parquet(file_path, engine="pyarrow", compression="gzip")
    else:
        dept_code_formatted = dept.zfill(3)
        file_path = os.path.join(OUTPUT_PATH, f"{current_date}-icpe-{dept_code_formatted}.parquet")
        df.to_parquet(file_path, engine="pyarrow", compression="gzip")

    print(f"Données enregistrées pour {dept} : {file_path}")

print("Processus terminé !")
