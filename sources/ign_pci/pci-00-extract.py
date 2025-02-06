# Importation des librairies
import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path

# Chemin
path_url = "https://geoservices.ign.fr/parcellaire-express-pci"
path_output = Path("D:/ign/parcellaire-express/zip/")

# Scrapping de la page
r = requests.get(path_url)
soup = BeautifulSoup(r.content, "html.parser")

# Récupération de tous les liens à télécharger
links = []
for link in soup.find_all("a", attrs={"href": re.compile("^https://data.geopf.fr.*PARCELLAIRE-EXPRESS.*")}):
    links.append(link["href"])

# Récupération des fichiers déjà téléchargés
files = []
for file in path_output.rglob("*7z"):
    files.append(file.name)

# Path(el).stem = récupère le nom du fichier sans l'extension
# with_suffix(".7z") pour ajouter l'extension .7z au nom du fichier 
# (path_output / Path(el).stem).with_suffix('.7z').exists() si le fichier existe
links_filtered = [el for el in links if not (path_output / Path(el).stem).with_suffix(".7z").exists()]

for link in links_filtered:

    # Envoie requête http
    r = requests.get(link, headers={"User-Agent": "Custom"}, stream=True)
    print(f"Réponse du serveur : {r}")

    # Nom du fichier
    file_name = Path(link).name
    print(f"Téléchargement de {file_name}")

    # Chemin de destination
    file_dest = path_output / file_name

    # Téléchargement
    with open(file_dest, "wb") as f:
        for chunk in r.iter_content(chunk_size= 1024*1024):
            if chunk:
                bytes = f.write(chunk)
    print(f"--- Téléchargement effectué\n")
