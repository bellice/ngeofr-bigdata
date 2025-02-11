# Importation des librairies
from pathlib import Path
import py7zr
import sys
import yaml

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
ZIP_PATH = SOURCE_PATH / config["sources"][source]["paths"]["zip"]
UNZIP_PATH = SOURCE_PATH / config["sources"][source]["paths"]["unzip"]

# Création du dossier de destination s'il n'existe pas
UNZIP_PATH.mkdir(parents=True, exist_ok=True)

print(f"Décompression de {ZIP_PATH} vers {UNZIP_PATH}")

# Récupération des fichiers
files = list(ZIP_PATH.glob('*.7z'))

# Filtre des fichiers .7z qui ne sont pas déjà extraits
files_filtered = [fichier for fichier in files if not (UNZIP_PATH / fichier.stem).exists()]

# Décompression des fichiers
for file in files_filtered:
    with py7zr.SevenZipFile(file, mode='r') as archive:
        archive.extractall(UNZIP_PATH)
        print(f"{file.name} a été extraite")
