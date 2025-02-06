# Importation des librairies
import os
os.environ["USE_PYGEOS"] = "0"

import re
import time
from pathlib import Path
from pyogrio import read_dataframe

# Chemin dossier
path_input = Path("D:/ign/parcellaire-express/unzip")
path_output = Path("D:/ign/parcellaire-express/parquet/dep")


# Récupération des chemins des fichiers
files_input = list(path_input.rglob("*/*/*/*/PARCELLE.shp"))
files_output = [file_output.stem for file_output in path_output.glob("*-pci-parcelle-*")]


# Filtre des fichiers qui ne sont pas déjà converti
files_filtered = [
    file
    for file in files_input
    if not any(re.search(r"_D([0-9]{2}[0-9|A-B])\\PARCELLE\.shp$", file.__str__()).group(1) in file_output
               and re.search(r"(\d{4}-\d{2})", file.__str__()).group(1) in file_output
                for file_output in files_output
    )
]

for file in files_filtered:

    code_dep = re.search(r"_D([0-9]{2}[0-9|A-B])\\PARCELLE\.shp$", file.__str__()).group(1)
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
        path = path_output / f"{date}-pci-{code_dep}.parquet",
        compression = "gzip")
    
    export_time = time.time() - start_time
    
    print(f"--- Exportation OK en {export_time:.2f}s")
    print(f"--- Durée totale : {import_time + export_time:.2f}s\n")
