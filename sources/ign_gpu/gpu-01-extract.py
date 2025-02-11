import requests

# Paramètre de partition (ajuste-le en fonction de ta recherche)
partition_code = "DU_77420"  # Par exemple, pour une partition donnée

# Paramètre de la requête
params = {
    "partition": partition_code  # Remplacer par la partition souhaitée
}

# URL de l'API (remplacer par l'URL correcte de l'API que tu utilises)
url = "https://apicarto.ign.fr/api/gpu/document"

# Envoi de la requête
response = requests.get(url, params=params)

# Vérification de la réponse
if response.status_code == 200:
    data = response.json()  # ou autre méthode selon le format de la réponse
    print(f"Nombre de documents trouvés pour la partition {partition} : {len(data['features'])}")
else:
    print(f"Erreur {response.status_code} : {response.text}")




    # Créer une liste pour stocker les géométries et leurs propriétés
    features = []
    for feature in data['features']:
        document_name = feature['properties'].get('nom', 'Inconnu')  # Nom du document
        document_geometry = feature['geometry']  # Géométrie du document (GeoJSON)
        if document_geometry:
            features.append({
                "nom": document_name,
                "geometry": shape(document_geometry)  # Convertir GeoJSON en objet Shapely
            })

    # Créer un GeoDataFrame à partir des géométries
    gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")  # CRS WGS84

    # Exporter en fichier GeoJSON
    gdf.to_file(f"documents_{partition}.geojson", driver="GeoJSON")
    print(f"Fichier GeoJSON exporté avec succès : documents_{partition}.geojson")

    # Exporter en fichier Shapefile (optionnel)
    gdf.to_file(f"documents_{partition}.shp", driver="ESRI Shapefile")
    print(f"Fichier Shapefile exporté avec succès : documents_{partition}.shp")
else:
    print(f"Erreur lors de la requête : {response.status_code}")










import requests

# Paramètres de la requête
params = {
    "du_type": "PLU"  # Ajoute ici le type de document que tu veux (par exemple "plu" et "plui")
}

# Envoi de la requête avec les paramètres
url = "https://apicarto.ign.fr/api/gpu/document"
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    print(data)  # Inspecter la réponse pour voir comment les partitions sont gérées
else:
    print(f"Erreur {response.status_code} : {response.text}")
