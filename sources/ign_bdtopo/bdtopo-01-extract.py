# Importation des librairies
import requests
from bs4 import BeautifulSoup
import re
import yaml
from pathlib import Path
import sys

# Charger la configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_bdtopo"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
URL = config["sources"][source]["url"]
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["zip"]

# Création du dossier de sortie s'il n'existe pas
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Téléchargement depuis : {URL}")
print(f"Enregistrement dans : {OUTPUT_PATH}")


# Scrapping de la page
r = requests.get(URL)
soup = BeautifulSoup(r.content, "html.parser")


# Récupération de tous les liens à télécharger
links = []
for link in soup.find_all("a", href=re.compile(r"^https://data\.geopf\.fr.*/BDTOPO_3-4_TOUSTHEMES_SHP.*_D\d{3}_.*")):
    links.append(link["href"])

# Récupération des fichiers déjà téléchargés
files = []
for file in OUTPUT_PATH.rglob("*7z"):
    files.append(file.name)

# Path(el).stem = récupère le nom du fichier sans l'extension
# with_suffix(".7z") pour ajouter l'extension .7z au nom du fichier 
# (path_output / Path(el).stem).with_suffix('.7z').exists() si le fichier existe
links_filtered = [el for el in links if not (OUTPUT_PATH / Path(el).stem).with_suffix(".7z").exists()]

for link in links_filtered:

    # Envoie requête http
    r = requests.get(link, headers={"User-Agent": "Custom"}, stream=True)
    print(f"Réponse du serveur : {r}")

    # Nom du fichier
    file_name = Path(link).name
    print(f"Téléchargement de {file_name}")

    # Chemin de destination
    file_dest = OUTPUT_PATH / file_name

    # Téléchargement
    with open(file_dest, "wb") as f:
        for chunk in r.iter_content(chunk_size= 1024*1024):
            if chunk:
                bytes = f.write(chunk)
    print(f"--- Téléchargement effectué\n")
