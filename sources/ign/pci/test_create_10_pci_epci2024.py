# Importation des librairies
from pathlib import Path
import duckdb
import pandas as pd

# Chemin dossier
path_input = Path("D:/ign/parcellaire-express/parquet/dep")
path_output = Path("D:/ign/parcellaire-express/parquet/epci")
version = "2024-07"


# Récupération des chemins des fichiers
files_input = list(path_input.rglob(version + "*.parquet"))

# Connexion à la base
db = duckdb.connect("")

db.execute("PRAGMA memory_limit='2GB'")  # Limiter l'utilisation de la mémoire
#db.execute(f"""
#CREATE VIEW parcelle_lazy AS 
#SELECT * 
#FROM read_parquet_auto('{path_input.__str__()}/{version}*.parquet')
#""")

# 1/ Charger les fichiers Parquet dans une vue temporaire "parcelle"
db.execute(f"""
           CREATE VIEW parcelle AS 
           SELECT *
           FROM read_parquet('{path_input.__str__()}/{version}*.parquet')
           """)

# Nombre de lignes
db.execute("SELECT COUNT(*) from parcelle").fetchall()

db.execute("""
          SELECT *,
          FROM parcelle
          LIMIT 5
          """).df()


# 2/ Transformation de CODE_COM en insee_com
db.execute("""
CREATE TABLE parcelle_transformed AS
SELECT DISTINCT 
    CODE_DEP || CODE_COM AS com_insee,  
    NOM_COM AS com_lib, 
    IDU AS pci_code, 
    CASE 
        WHEN CODE_DEP = '97' THEN CODE_DEP || SUBSTR(CODE_COM, 1, 1)
        ELSE CODE_DEP 
    END AS dep_insee,
    geometry  
FROM parcelle;
""")


# Prévisualiser les premières lignes pour éviter un calcul coûteux
result_sample = db.execute("SELECT * FROM parcelle_transformed LIMIT 1").fetchall()
print(result_sample)  # Cela permet de vérifier si la transformation s'est bien faite



# 3/ Connexion à la base ngeofr pour récupérer epci_siren et epci_nom
# Nous nous connectons à la base ngeofr
db.execute(f"""
ATTACH DATABASE 'G:/path_to_your_database/ngeofr.duckdb' AS ngeofr
""")

# 4/ Sélection des colonnes de ngeofr : epci_siren, epci_nom et com_insee
db.execute("""
CREATE VIEW ngeofr_data AS 
SELECT 
    epci_siren, 
    epci_nom, 
    com_insee 
FROM ngeofr.table_name -- Remplacez `table_name` par le nom réel de votre table ngeofr
""")

# 5/ Faire la jointure sur insee_com (parcelle) et com_insee (ngeofr)
db.execute("""
CREATE VIEW parcelle_joint AS
SELECT 
    parcelle_transformed.*, 
    ngeofr_data.epci_siren, 
    ngeofr_data.epci_nom
FROM parcelle_transformed
LEFT JOIN ngeofr_data
ON parcelle_transformed.insee_com = ngeofr_data.com_insee
""")

# Vérification de la jointure
print(db.execute("SELECT COUNT(*) FROM parcelle_joint").fetchall())

# 6/ Exporter le résultat en plusieurs fichiers parquet, partitionnés par epci_siren
output_path_str = path_output.__str__()

db.execute(f"""
EXPORT DATABASE '{output_path_str}' (FORMAT PARQUET, PARTITION_BY 'epci_siren');
""")

print("Export terminé avec succès.")

db.close()









# # Connexion à la base
# db = duckdb.connect(database=path_output.__str__() + "/2023-07-parcelle.duckdb", read_only=False)

# db.execute("INSTALL spatial;LOAD spatial;").df()



# # Nouvelle table
# table_parcelle = """CREATE TABLE IF NOT EXISTS parcelle(
#                     idu VARCHAR NOT NULL,
#                     insee_com VARCHAR NOT NULL CHECK(length(insee_com) == 5),
#                     lib_com VARCHAR NOT NULL,
#                     insee_dep VARCHAR NOT NULL CHECK(length(insee_dep) == 2 OR length(insee_dep) == 3),
#                     geom WKB_BLOB,
#                     )"""

# db.execute(table_parcelle)

# db.execute("SET memory_limit='8GB'").df()
# db.execute("SET threads TO 1").df()


# # Nom des colonnes et type des fichiers parquet
# db.execute(f"""DESCRIBE SELECT * FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')""").df()

# N = 0
# while N < 5000000: 
#   db.execute(f"""
#             INSERT INTO parcelle
#             SELECT IDU as idu,
#             CODE_DEP||CODE_COM AS 'insee_com',
#             NOM_COM as lib_com,
#             CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep',
#             ST_AsWKB(ST_GeomFromWKB(geometry)) AS 'geom',
#             FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')
#             LIMIT {N} + 500000
#             OFFSET {N}
#             """)
#   N = N+500000


# #db.execute(f"""CREATE VIEW parcelle AS SELECT * FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')""")




# db.execute("RESET memory_limit").df()



# # Fin de la connexion
# db.close()

