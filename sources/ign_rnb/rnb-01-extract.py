import requests
import yaml
from pathlib import Path
import sys
from tqdm import tqdm
from datetime import datetime
import re

# Charger la configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_rnb"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
URL_DATASET = config["sources"][source]["url"]
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["zip"]

# Création du dossier de sortie s'il n'existe pas
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)



# Récupération des informations du dataset
try:
    response = requests.get(URL_DATASET, headers={"User-Agent": "Custom"})
    response.raise_for_status()
    dataset_info = response.json()

    resources = dataset_info.get("resources", [])
    print(f"{dataset_info['title']} - {len(resources)} ressources trouvées.\n")

    # Filtrer les ressources correspondant au format "RNB_<nombre>.csv.zip"
    pattern = re.compile(r"RNB_(?!98)\d{2,3}\.csv\.zip$")

    files_to_download = [
        res for res in resources if pattern.search(res["url"].split("/")[-1])
    ]

    if not files_to_download:
        print("Aucune ressource correspondant au format demandé.")

except requests.RequestException as e:
    print(f"Erreur lors de la récupération des informations du dataset : {e}")
files_to_download

# Téléchargement des fichiers filtrés
for res in files_to_download:
    file_url = res["url"]
    last_modified = res.get("last_modified", "")[:10]  # Extraire AAAA-MM-JJ

    # Extraire uniquement le nom du fichier depuis l'URL
    file_name = f"{last_modified}_{Path(file_url).name}"

    # Remplacer les caractères interdits dans le nom de fichier (sécurité)
    file_name = re.sub(r'[\/:*?"<>|]', '_', file_name)

    file_dest = OUTPUT_PATH / file_name

    print(f"Téléchargement : {file_name}")

    # Vérification si le fichier existe déjà
    if file_dest.exists():
        print(f"Le fichier {file_name} existe déjà, téléchargement annulé.\n")
        continue

    # Téléchargement du fichier
    try:
        with requests.get(file_url, headers={"User-Agent": "Custom"}, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))

            with open(file_dest, "wb") as f, tqdm(
                total=total_size, unit="B", unit_scale=True, desc=file_name
            ) as pbar:
                for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):  # 10 Mo
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        print(f"Téléchargement terminé : {file_dest}\n")

    except requests.RequestException as e:
        print(f"Erreur lors du téléchargement de {file_name} : {e}")

