import geopandas as gpd
import pandas as pd
from shapely.wkt import loads as wkt_loads
from shapely.geometry import shape
import os
import requests
import sys
from utils import load_config, create_output_path, get_proxies, load_env
import duckdb
from pathlib import Path
from datetime import datetime
import re

# Obtenir la date actuelle au format "AAAA-MM"
current_date = datetime.today().strftime("%Y-%m")

# Charger la configuration et les variables d'environnement
proxy_url = load_env()
proxies = get_proxies(proxy_url)
config = load_config()

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "brgm_ssp"
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

# Extraire la liste des codes départementaux au format d'origine
departements = set(dep_df["dep_insee"].tolist())  # Ex: {'01', '2A', '2B', '75', '93'}

# Étape 1 : Identifier les fichiers existants du mois courant
existing_files = os.listdir(OUTPUT_PATH)
existing_deps = set()

for file in existing_files:
    if file.startswith(f"{current_date}-ssp-") and file.endswith(".parquet"):
        parts = file.split("-")  # Exemple : "2025-02-ssp-075-casias.parquet"

        if len(parts) >= 5:  # S'assurer que le fichier a bien la structure attendue
            file_dep_code = parts[3]  # Récupérer le code département

            # Si le département commence par "9", on laisse tel quel
            # Sinon, si c'est un code sur 3 caractères avec un zéro en tête, on enlève ce zéro
            if file_dep_code.startswith("9"):
                existing_deps.add(file_dep_code)
            elif len(file_dep_code) == 3 and file_dep_code.startswith("0"):
                existing_deps.add(file_dep_code[1:])  # Supprime le zéro en tête

print(f"📌 Départements déjà enregistrés : {sorted(existing_deps)}")

# Étape 2 : Supprimer les départements déjà traités
# Formater les départements à traiter : garder 3 caractères pour les départements commençant par "9", sinon les mettre sur 2 caractères
departements = {
    dep if dep.startswith("9") else dep.zfill(2) for dep in departements
}

# Filtrer les départements déjà enregistrés
departements = departements - existing_deps

print(f"📌 Départements restants à traiter après filtrage : {sorted(departements)}")


# Fonction pour récupérer les données pour un département donné
def fetch_data_for_department(dept_code):
    all_data = {"casias": [], "instructions": []}
    page = 1

    while True:
        params = {
            "code_departement": dept_code,
            "page_size": 1000,
            "page": page
        }

        response = requests.get(URL, params=params, proxies=proxies)

        if response.status_code != 200:
            print(f"Erreur API pour département {dept_code} : {response.status_code}")
            return None

        data = response.json()

        # Vérifier si des données sont retournées
        if not any(key in data for key in all_data.keys()):
            break

        # Ajouter les données de chaque section
        for key in all_data.keys():
            if key in data:
                all_data[key].extend(data[key]["data"])

        # Vérifier si on a atteint la dernière page
        if len(data["casias"]["data"]) < params["page_size"]:
            break

        page += 1

    return all_data

# **Étape 3 : Récupération et enregistrement des données**
for dept in sorted(departements):  # On trie pour un affichage plus propre
    print(f"Récupération des données pour le département {dept}...")
    data = fetch_data_for_department(dept)

    if not data:
        continue  # Passer au département suivant en cas d'erreur

    for key in ["casias", "instructions"]:
        df = pd.DataFrame(data[key])

        if 'geom' in df.columns:
            print(f"Analyse de la colonne 'geom' pour {key} - Département {dept}:")

            def convert_geom(x):
                if pd.isnull(x):
                    return None
                elif isinstance(x, dict):
                    return shape(x)
                elif isinstance(x, str):
                    try:
                        return wkt_loads(x)
                    except Exception as e:
                        print(f"Erreur conversion WKT pour {x}: {e}")
                        return None
                return None

            df['geom'] = df['geom'].apply(convert_geom)

            # Création du GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry='geom', crs="EPSG:4326")

            print(f"Nombre de géométries NULL dans {key} - Département {dept} : {gdf['geom'].isnull().sum()}")

            # Construire le nom du fichier
            dept_code_formatted = dept.zfill(3)  # Le code département est déjà sur 2 ou 3 caractères
            filename = f"{current_date}-ssp-{dept_code_formatted}-{key.replace('instructions', 'sis')}.parquet"
            file_path = os.path.join(OUTPUT_PATH, filename)

            print(f"Enregistrement des données {key} - Département {dept} en GeoParquet : {file_path}")
            gdf.to_parquet(file_path, engine="pyarrow", compression="gzip")

        else:
            dept_code_formatted = dept.zfill(3)  # Le code département est déjà sur 2 ou 3 caractères
            filename = f"{current_date}-ssp-{dept_code_formatted}-{key.replace('instructions', 'sis')}.parquet"
            file_path = os.path.join(OUTPUT_PATH, filename)

            print(f"Enregistrement des données {key} - Département {dept} en Parquet : {file_path}")
            df.to_parquet(file_path, engine="pyarrow", compression="gzip")

print("Processus terminé !")
