import pandas as pd
from pathlib import Path
from datetime import datetime
import requests
import sys
import geopandas as gpd
from shapely.geometry import shape
import paramiko
import os
from utils import load_config, load_env, get_proxies, create_output_path

# Charger la configuration et les variables d'environnement
proxy_url = load_env()
proxies = get_proxies(proxy_url)
config = load_config()

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_gpu"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = config["base_path"]
SOURCE_PATH = config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]
OUTPUT_PATH = create_output_path(BASE_PATH, SOURCE_PATH, config["sources"][source]["paths"]["parquet_raw"])

print(f"Téléchargement depuis : {URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")

# Téléchargement du fichier via SFTP
file_to_download = "doc_urba.csv"
remote_dir = "/pub/export-wfs/latest/csv/wfs_du"
local_file_path = OUTPUT_PATH / file_to_download

if not local_file_path.exists():
    try:
        transport = paramiko.Transport(("sftp-public.ign.fr", 2200))
        transport.connect(username="gpu_depot_exports", password="yegh5EdeeFeegahz")
        sftp = paramiko.SFTPClient.from_transport(transport)

        if file_to_download in sftp.listdir(remote_dir):
            print(f"Téléchargement de {file_to_download} vers {local_file_path}...")
            sftp.get(f"{remote_dir}/{file_to_download}", str(local_file_path))
            print("Téléchargement terminé !")
        else:
            print(f"Le fichier {file_to_download} n'existe pas sur le serveur.")

        sftp.close()
        transport.close()
    except Exception as e:
        print(f"Erreur : {e}")

# Chargement et transformation du DataFrame
df_final = (
    pd.read_csv(Path(OUTPUT_PATH) / "doc_urba.csv", sep=",", encoding="utf-8", usecols=["gpu_doc_id", "partition", "idurba", "datappro", "typedoc"])
    .query("typedoc in ['PLU', 'PLUI']")  # Filtrer typedoc
    .assign(
        datappro=lambda x: pd.to_datetime(x["datappro"].astype(str).str.extract(r"(\d{8})")[0], format="%Y%m%d", errors="coerce"),
        datdoc=lambda x: pd.to_datetime(x["idurba"].str.extract(r"(\d{8})(?=\D*$)")[0], format="%Y%m%d", errors="coerce"),
    )
    .assign(datdoc=lambda x: x["datdoc"].fillna(x["datappro"]).where(x["datappro"].between("1990", str(datetime.today().year))))
    .sort_values("datdoc", ascending=False)
    .groupby(["gpu_doc_id", "partition"], as_index=False)
    .first()
    .loc[:, ["gpu_doc_id", "partition", "idurba", "datappro", "datdoc", "typedoc"]]  # Réorganiser colonnes
)

print(f"Min datdoc: {df_final['datdoc'].min()}, Max datdoc: {df_final['datdoc'].max()}")
print(df_final.head())

# Vérification des doublons
if df_final.duplicated(subset=["partition"]).any():
    print("\n=== Doublons trouvés sur 'partition'. ===")
    print(df_final[df_final.duplicated(subset=["partition"], keep=False)])
else:
    print("\n=== Aucun doublon trouvé sur 'partition'. ===")

# Fonction pour interroger l'API
def fetch_resource(partition):
    params = {"partition": partition}
    try:
        response = requests.get(URL, params=params, proxies=proxies)
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        if features:
            return gpd.GeoDataFrame([f["properties"] for f in features], geometry=[shape(f["geometry"]) for f in features], crs="EPSG:4326")
    except requests.RequestException as e:
        print(f"Erreur pour la partition {partition} : {e}")
    return None

# Traitement des partitions
df_final["nodata_document"] = "non"
for _, row in df_final.iterrows():
    partition, datdoc = row["partition"], row["datdoc"]
    output_file = OUTPUT_PATH / f"{partition}.parquet"

    if output_file.exists() and datetime.fromtimestamp(output_file.stat().st_mtime) >= datdoc:
        print(f"{output_file} est déjà à jour.")
        continue

    gdf = fetch_resource(partition)
    if gdf is not None:
        gdf.to_parquet(output_file, engine="pyarrow", compression="gzip")
        print(f"Données exportées : {output_file}")
    else:
        df_final.loc[df_final["partition"] == partition, "nodata_document"] = "oui"

# Sauvegarde finale
df_final.to_csv(OUTPUT_PATH / "doc_urba_final.csv", index=False, encoding="utf-8")
print("\n=== Traitement terminé ===")
