import pandas as pd
from pathlib import Path
from datetime import datetime
import yaml
import requests
import sys
import geopandas as gpd
from shapely.geometry import shape
import os
from dotenv import load_dotenv

# Trouver le chemin du fichier .env
env_path = os.path.abspath(os.path.join(os.getcwd(), ".env"))
print(f"Chargement de : {env_path}")  # Debug

# Charger les variables d'environnement depuis le fichier .env
load_dotenv(env_path)

# Récupérer l'URL du proxy depuis le fichier .env
proxy_url = os.getenv("PROXY_URL")
print(f"Valeur de PROXY_URL : {proxy_url}")  # Debug pour vérifier que la variable est bien chargée

# Définir le proxy pour les requêtes si disponible
proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None


# Charger la configuration
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_gpu"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]  # URL de l'API
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["parquet_raw"]

# Création du dossier de sortie s'il n'existe pas
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Téléchargement depuis : {URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")

# Chargement du fichier et chaînage de méthodes
df_final = (
    pd.read_csv(SOURCE_PATH / "doc_urba.csv", sep=",", encoding="utf-8")
    .loc[:, ["gpu_doc_id", "partition", "idurba", "datappro", "typedoc"]]  # Sélection des colonnes utiles
    .query("typedoc in ['PLU', 'PLUI']")  # Filtrage sur typedoc
    .assign(
        datappro=lambda x: (
            x["datappro"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .pipe(pd.to_datetime, format="%Y%m%d", errors="coerce")
        ),  # Conversion de datappro
        datdoc=lambda x: (
            x["idurba"]
            .str.extract(r"(\d{8})(?=\D*$)")
            .pipe(pd.to_datetime, format="%Y%m%d", errors="coerce")
        ),  # Extraction de la date depuis idurba
    )
    .assign(
        datdoc=lambda x: x.apply(
            lambda row: row["datappro"] if (
                pd.isna(row["datdoc"]) and
                not pd.isna(row["datappro"]) and
                1990 <= row["datappro"].year <= datetime.today().year
            ) else row["datdoc"],
            axis=1,
        )  # Remplacer datdoc par datappro si nécessaire
    )
    .loc[:, ["gpu_doc_id", "partition", "idurba", "datappro", "datdoc", "typedoc"]]  # Réorganiser les colonnes
    .sort_values(by="datdoc", ascending=False)  # Trier par datdoc
    .groupby(["gpu_doc_id", "partition"], as_index=False)
    .apply(lambda group: group.nlargest(1, "datdoc"))  # Garder la dernière date par groupe
    .reset_index(drop=True)
    .query('~partition.str.startswith("PSMV")')  # Exclure les partitions commençant par PSMV
)

# Vérification des doublons sur partition
if not df_final.duplicated(subset=["partition"]).any():
    print("\n=== Aucun doublon trouvé sur 'partition'. ===")
else:
    print("\n=== Doublons trouvés sur 'partition'. ===")
    print(df_final[df_final.duplicated(subset=["partition"], keep=False)])

# Fonction pour interroger l'API et récupérer les données
def fetch_resource(partition):
    """Interroge l'API pour une partition donnée et retourne un GeoDataFrame."""
    params = {"partition": partition}
    response = requests.get(URL, params=params, proxies=proxies)
    
    if response.status_code == 200:
        data = response.json()
        features = data.get("features", [])
        if features:
            geometries = [shape(feature["geometry"]) for feature in features]
            properties = [feature["properties"] for feature in features]
            gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")
            return gdf
        else:
            print(f"Aucune donnée trouvée pour la partition {partition}.")
            return None
    else:
        print(f"Erreur {response.status_code} pour la partition {partition} : {response.text}")
        return None

# Filtrer les partitions à traiter
df_final = df_final.assign(nodata_document="non")  # Initialiser la colonne à "Non" par défaut

for _, row in df_final.iterrows():
    partition = row["partition"]
    datdoc = row["datdoc"]
    output_file = OUTPUT_PATH / f"{partition}.parquet"

    # Vérifier si le fichier existe déjà et s'il est à jour
    if os.path.exists(output_file):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(output_file))
        if file_mod_time >= datdoc:
            print(f"Le fichier {output_file} est à jour (datdoc: {datdoc}, modifié le: {file_mod_time}).")
            continue  # Passer au fichier suivant

    # Télécharger les données
    gdf = fetch_resource(partition)
    if gdf is not None:
        gdf.to_parquet(output_file, engine="pyarrow", compression="gzip")  # Compression GZIP
        print(f"Les données pour la partition {partition} ont été exportées dans {output_file} (compression GZIP)")
    else:
        # Marquer la partition comme n'ayant aucune donnée
        df_final.loc[df_final["partition"] == partition, "nodata_document"] = "oui"
        print(f"Aucune donnée trouvée pour la partition {partition}.")

# Sauvegarder df_final en CSV
output_csv = OUTPUT_PATH / "doc_urba_final.csv"
df_final.to_csv(output_csv, index=False, encoding="utf-8")
print(f"\n=== df_final sauvegardé en CSV dans {output_csv} ===")

print("\n=== Traitement terminé ===")