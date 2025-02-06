# Importation des librairies
from pathlib import Path
import py7zr

# Chemin
path_input = Path("D:/ign/parcellaire-express/zip")
path_output = path_input.parent / "unzip"

# Récupération des fichiers
files = list(path_input.glob('*.7z'))

# Filtre des fichiers .7z qui ne sont pas déjà extraits
files_filtered = [fichier for fichier in files if not (path_output / fichier.stem).exists()]

# Décompression des fichiers
for file in files_filtered:
    with py7zr.SevenZipFile(file, mode='r') as archive:
        archive.extractall(path_output)
        print(f"{file.name} a été extraite")
