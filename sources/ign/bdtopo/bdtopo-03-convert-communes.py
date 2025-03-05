import os
os.environ["USE_PYGEOS"] = "0"

import re
import time
import yaml
from pathlib import Path
from pyogrio import read_dataframe
import sys

# Charger la configuration
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Déterminer la source
source = sys.argv[1] if len(sys.argv) > 1 else "ign_bdtopo"
if source not in config["sources"]:
    raise ValueError(f"Source {source} non trouvée dans config.yaml")

# Définition des chemins pour la conversion départementale
BASE_PATH = Path(config["base_path"])
SOURCE_PATH = BASE_PATH / config["sources"][source]["relative_path"]
INPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["unzip"]
OUTPUT_PATH = SOURCE_PATH / config["sources"][source]["paths"]["parquet_dep"]

# Création du dossier de destination s'il n'existe pas
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

print(f"Conversion de {INPUT_PATH} vers {OUTPUT_PATH}")

files_input = list(INPUT_PATH.rglob("*/*/*/*/COMMUNE.shp"))
files_output = [file_output.stem for file_output in OUTPUT_PATH.glob("*-communes.*")]

# Regex mise à jour pour gérer 001-101, 02A, 02B, 971-976
regex_dept = re.compile(r"D(\d{2,3}[A-B]?)")  # Gère 02A, 02B, 971-976, 001-101

# Extraction des départements existants dans files_output
departments_output = {name.split('-')[3] for name in files_output}

files_filtered = [
    file for file in files_input
    if (match := regex_dept.search(str(file)))  # Vérifier si le fichier contient un département
    and match.group(1) not in departments_output  # Garder uniquement les nouveaux départements
]


for file in files_filtered:

    code_dep = re.search(r"_D([0-9]{2}[0-9|A-B]).*\\COMMUNE\.shp$", file.__str__()).group(1)
    date = re.search(r"(\d{4}-\d{2})", file.__str__()).group(1)
    print(f"Département {code_dep}, millésime {date}")

    # Importation des données
    start_time = time.time()
    df = read_dataframe(file)

    import_time = time.time() - start_time

    print(f"--- Importation OK en {import_time:.2f}s")

    # Exportation des données
    start_time = time.time()
    df.to_parquet(
        path = OUTPUT_PATH / f"{date}-bdtopo-{code_dep}-communes.parquet",
        compression = "gzip")
    
    export_time = time.time() - start_time
    
    print(f"--- Exportation OK en {export_time:.2f}s")
    print(f"--- Durée totale : {import_time + export_time:.2f}s\n")
    