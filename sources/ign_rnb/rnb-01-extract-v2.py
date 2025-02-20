import requests
from pathlib import Path
import sys
from tqdm import tqdm
import re
from utils import load_config, load_env, get_proxies, create_output_path

# Charger les variables d'environnement et la configuration
proxy_url = load_env()
proxies = get_proxies(proxy_url)
config = load_config()

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_rnb"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = config["base_path"]
SOURCE_PATH = config["sources"][source]["relative_path"]
URL_DATASET = config["sources"][source]["url"]
OUTPUT_PATH = create_output_path(BASE_PATH, SOURCE_PATH, config["sources"][source]["paths"]["zip"])

# Récupération des informations du dataset
try:
    response = requests.get(URL_DATASET, headers={"User-Agent": "Custom"}, proxies=proxies)
    response.raise_for_status()
    dataset_info = response.json()

    resources = dataset_info.get("resources", [])
    print(f"{dataset_info['title']} - {len(resources)} ressources trouvées.\n")

    # Filtrer les ressources correspondant au format "RNB_<nombre>.csv.zip"
    pattern = re.compile(r"RNB_(?!98)\d{2,3}\.csv\.zip$")
    files_to_download = [res for res in resources if pattern.search(res["url"].split("/")[-1])]

    if not files_to_download:
        print("Aucune ressource correspondant au format demandé.")

except requests.RequestException as e:
    print(f"Erreur lors de la récupération des informations du dataset : {e}")

# Téléchargement des fichiers filtrés
for res in files_to_download:
    file_url = res["url"]
    last_modified = res.get("last_modified", "")[:10]  # Extraire AAAA-MM-JJ
    file_name = f"{last_modified}_{Path(file_url).name}"
    file_name = re.sub(r'[\/:*?"<>|]', '_', file_name)
    file_dest = OUTPUT_PATH / file_name

    print(f"Téléchargement : {file_name}")

    if file_dest.exists():
        print(f"Le fichier {file_name} existe déjà, téléchargement annulé.\n")
        continue

    try:
        with requests.get(file_url, headers={"User-Agent": "Custom"}, stream=True, proxies=proxies) as r:
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