import requests
import geopandas as gpd
from shapely.geometry import shape




# Paramètres de la requête
params = {
    "partition": "DU_200071629"  # Code SIREN de l'intercommunalité
}

# Envoi de la requête avec les paramètres
url = "https://apicarto.ign.fr/api/gpu/zone-urba"
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    
    # Convertir la réponse JSON en GeoDataFrame
    features = data.get("features", [])
    if features:
        # Extraire les géométries et les propriétés
        geometries = [shape(feature["geometry"]) for feature in features]
        properties = [feature["properties"] for feature in features]
        
        # Créer un GeoDataFrame
        gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")
        
        # Exporter en GeoParquet
        output_file = "documents_plui.parquet"
        gdf.to_parquet(output_file, engine="pyarrow")
        print(f"Les données ont été exportées avec succès dans {output_file}")
    else:
        print("Aucune donnée trouvée dans la réponse.")
else:
    print(f"Erreur {response.status_code} : {response.text}")
