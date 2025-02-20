import paramiko
import os
from pathlib import Path

# Chemin dossier local
path_output = Path("D:/ign/gpu/")
path_output.mkdir(parents=True, exist_ok=True)

# Nom du fichier cible
file_to_download = "doc_urba.csv"

# Initialiser le client SSH
try:
    transport = paramiko.Transport(("sftp-public.ign.fr", 2200))
    transport.connect(username="gpu_depot_exports", password="yegh5EdeeFeegahz")
    
    # Initialiser le client SFTP
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # Chemin distant du fichier
    remote_dir = "/pub/export-wfs/latest/csv/wfs_du"
    remote_file_path = f"{remote_dir}/{file_to_download}"
    local_file_path = path_output / file_to_download

    # Vérifier si le fichier existe sur le serveur
    remote_files = sftp.listdir(remote_dir)
    if file_to_download in remote_files:
        print(f"Téléchargement de {file_to_download} vers {local_file_path}...")
        sftp.get(remote_file_path, str(local_file_path))
        print("Téléchargement terminé !")
    else:
        print(f"Le fichier {file_to_download} n'existe pas sur le serveur.")

    # Fermer la connexion
    sftp.close()
    transport.close()
    
except Exception as e:
    print(f"Erreur : {e}")


# https://www.geoportail-urbanisme.gouv.fr/document/download-by-partition/


import pandas as pd
from pathlib import Path
from datetime import datetime
import yaml
import requests
import sys

# Charger la configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_gpu"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]  # URL de téléchargement
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["zip"]

# Création du dossier de sortie s'il n'existe pas
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Téléchargement depuis : {DOWNLOAD_URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")

# Chargement du fichier
CSV_FILE = SOURCE_PATH / "doc_urba.csv"
df = pd.read_csv(CSV_FILE, sep=",", encoding="utf-8")

# Sélection des colonnes utiles
cols = ["gpu_doc_id", "partition", "idurba", "datappro", "typedoc"]
df = df[cols]

# Filtrer sur typedoc (PLU ou PLUI)
df = df[df["typedoc"].isin(["PLU", "PLUI"])]

# Conversion de datappro (AAAAMMJJ.0 => datetime)
df["datappro"] = (
    df["datappro"]
    .astype(str)  # Convertir en string pour gérer les .0
    .str.replace(r"\.0$", "", regex=True)  # Supprimer le .0 final
    .pipe(pd.to_datetime, format="%Y%m%d", errors="coerce")  # Conversion datetime
)

# Extraction de la date depuis idurba
df["datdoc"] = df["idurba"].str.extract(r"(\d{8})(?=\D*$)")
df["datdoc"] = pd.to_datetime(df["datdoc"], format="%Y%m%d", errors="coerce")

# Remplacer datdoc par datappro si datdoc est NA et datappro est valide
today = datetime.today()
df["datdoc"] = df.apply(
    lambda row: row["datappro"] if (
        pd.isna(row["datdoc"]) and
        not pd.isna(row["datappro"]) and
        1990 <= row["datappro"].year <= today.year
    ) else row["datdoc"],
    axis=1
)

# Réorganiser les colonnes pour placer datdoc après datappro
cols_order = ["gpu_doc_id", "partition", "idurba", "datappro", "datdoc", "typedoc"]
df = df[cols_order]

# Trouver la dernière date par groupe
df_sorted = df.sort_values(by="datdoc", ascending=False)
df_final = df_sorted.loc[df_sorted.groupby(["gpu_doc_id", "partition"])["datdoc"].idxmax()].reset_index(drop=True)

# Vérification des doublons sur partition
if not df_final.duplicated(subset=["partition"]).any():
    print("\n=== Aucun doublon trouvé sur 'partition'. ===")
else:
    print("\n=== Doublons trouvés sur 'partition'. ===")
    print(df_final[df_final.duplicated(subset=["partition"], keep=False)])

# Télécharger les ressources et vérifier le statut
def download_resource(partition, output_folder):
    """Télécharge une ressource à partir de son URL et l'enregistre dans output_folder."""
    url = f"{URL}/{partition}"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        file_path = output_folder / f"{partition}.zip"
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Téléchargé : {file_path}")
        return True
    else:
        print(f"Échec du téléchargement pour {partition} (code {response.status_code})")
        return False

# Ajouter une colonne pour le statut de téléchargement
df_final["download_status"] = df_final["partition"].apply(
    lambda partition: download_resource(partition, OUTPUT_PATH)
)

# Vérifier que la colonne a bien été ajoutée
print("\n=== Colonnes du DataFrame ===")
print(df_final.columns)

# Afficher le statut de téléchargement
print("\n=== Statut de téléchargement ===")
print(df_final[["partition", "download_status"]])