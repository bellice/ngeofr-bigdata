import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path
import sys
from utils import load_config, load_env, get_proxies, create_output_path

# Charger les variables d'environnement et la configuration
proxy_url = load_env()
proxies = get_proxies(proxy_url)
config = load_config()

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_bdtopo"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = config["base_path"]
SOURCE_PATH = config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]
OUTPUT_PATH = create_output_path(BASE_PATH, SOURCE_PATH, config["sources"][source]["paths"]["zip"])

print(f"Téléchargement depuis : {URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")

# Scrapping de la page
r = requests.get(URL, proxies=proxies)
soup = BeautifulSoup(r.content, "html.parser")

# Récupération de tous les liens à télécharger
links = [link["href"] for link in soup.find_all("a", href=re.compile(r"^https://data\.geopf\.fr.*/BDTOPO_3-4_TOUSTHEMES_SHP.*_D\d{3}_.*"))]

# Récupération des fichiers déjà téléchargés
files = [file.name for file in OUTPUT_PATH.rglob("*7z")]

# Filtrer les liens non téléchargés
links_filtered = [el for el in links if not (OUTPUT_PATH / Path(el).stem).with_suffix(".7z").exists()]

for link in links_filtered:
    r = requests.get(link, headers={"User-Agent": "Custom"}, stream=True, proxies=proxies)
    print(f"Réponse du serveur : {r}")

    file_name = Path(link).name
    print(f"Téléchargement de {file_name}")

    file_dest = OUTPUT_PATH / file_name
    with open(file_dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)
    print(f"--- Téléchargement effectué\n")