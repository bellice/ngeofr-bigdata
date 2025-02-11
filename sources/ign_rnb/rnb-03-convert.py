# Importation des librairies
import os
os.environ["USE_PYGEOS"] = "0"

import time
import yaml
import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
from shapely import wkt
import re

# Charger la configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Déterminer la source
source = sys.argv[1] if len(sys.argv) > 1 else "ign_rnb"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
INPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["unzip"]
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["parquet_dep"]

# Parcourir les fichiers CSV dans le dossier unzip
for input_file in INPUT_PATH.glob("*.csv"):
    file_name = input_file.stem  # Nom du fichier sans extension

    # Extraction de la date (AAAA-MM) et du code département
    match = re.search(r"(\d{4}-\d{2})-\d{2}_RNB_(\d{2,3})", file_name)
    if not match:
        print(f"Erreur : Format du fichier non reconnu pour {file_name}. Skipping.")
        continue

    date_str, dept_code = match.groups()
    dept_code = dept_code.zfill(3)  # 🔹 Mettre le code département sur 3 caractères

    # Définition du fichier de sortie
    output_file = OUTPUT_PATH / f"{date_str}-rnb-{dept_code}-batiments.parquet"

    # Vérifier si le fichier de sortie existe déjà
    if output_file.exists():
        print(f"Le fichier {output_file} existe déjà. Skipping.")
        continue

    print(f"Conversion de {input_file} vers {output_file}")

    # Lire le fichier CSV avec Dask
    df = dd.read_csv(
        input_file,
        sep=";",
        on_bad_lines="skip",
        dtype={"rnb_id": "string", "status": "string", "ext_ids": "string", "addresses": "string"}
    )

    # Fonction pour extraire le SRID et convertir en objet Shapely
    def ewkt_to_geometry(ewkt_str):
        if pd.isna(ewkt_str) or not isinstance(ewkt_str, str):
            return None  # Retourne None si la valeur est invalide
        
        match = re.match(r"SRID=(\d+);(.+)", ewkt_str)
        if match:
            _, wkt_str = match.groups()
            return wkt.loads(wkt_str)  # Retourne uniquement la géométrie
        
        return wkt.loads(ewkt_str)

    # Appliquer la conversion sur la colonne "shape" si elle existe
    if "shape" in df.columns:
        df["shape"] = df["shape"].map_partitions(
            lambda s: s.apply(lambda x: ewkt_to_geometry(x) if x else None),
            meta=("shape", "object")
        )

    # Appliquer la conversion en WKT pour la colonne "point" si elle existe
    if "point" in df.columns:
        df["point"] = df["point"].map_partitions(
            lambda s: s.apply(lambda x: wkt.dumps(ewkt_to_geometry(x)) if x else None),
            meta=("point", "object")
        )

    # Conversion explicite en DataFrame (Dask → Pandas)
    df = df.compute()

    # Création du GeoDataFrame
    if "shape" in df.columns:
        gdf = gpd.GeoDataFrame(df, geometry="shape", crs="EPSG:4326")
    else:
        print(f"Aucune colonne 'shape' trouvée dans {file_name}. Skipping.")
        continue

    # Export en GeoParquet avec geopandas
    start_time = time.time()
    gdf.to_parquet(
        path=output_file,  # Utilisation de votre chemin de sortie
        compression="gzip"  # Utilisation de votre paramètre de compression
    )
    print(f"Exportation terminée en {time.time() - start_time:.2f}s : {output_file}")