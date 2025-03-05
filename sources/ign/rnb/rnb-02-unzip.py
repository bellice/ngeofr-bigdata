# Importation des librairies
from pathlib import Path
import zipfile
import sys
import yaml

# Charger la configuration
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Déterminer la source de données
source = sys.argv[1] if len(sys.argv) > 1 else "ign_rnb"
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

# Récupération des fichiers .zip
files = list(ZIP_PATH.glob('*.zip'))

# Filtre des fichiers .zip qui ne sont pas déjà extraits
files_filtered = [fichier for fichier in files if not (UNZIP_PATH / fichier.stem).exists()]

# Décompression des fichiers
for file in files_filtered:
    # Recherche de la date dans le nom du fichier ZIP
    match = re.search(r"\d{4}-\d{2}-\d{2}", file.name)
    
    if not match:
        raise ValueError(f"Erreur : Aucune date trouvée dans le fichier {file.name}. Vérifiez le nom du fichier.")

    date_str = match.group(0)
    print(f"Date extraite du fichier {file.name} : {date_str}")

    # Décompresser le fichier ZIP
    with zipfile.ZipFile(file, 'r') as archive:
        archive.extractall(UNZIP_PATH)
        print(f"{file.name} a été extraite")