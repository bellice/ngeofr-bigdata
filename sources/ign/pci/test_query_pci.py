# Importation des librairies
from pathlib import Path
import duckdb
import pandas as pd

# Chemin dossier
path_input = Path("G:/ign/parcellaire-express/parquet/")
path_output = Path("O:/Document/carto-engine/ngeo/public/parcelle")


# Récupération des chemins des fichiers
files_input = list(path_input.rglob("2023-07*.parquet"))

# Connexion à la base
db = duckdb.connect()

# Lecture de tous les fichiers parquet
db.execute("SELECT * FROM parquet_scan(?)", [path_input.__str__() + "/2023-07*.parquet"])

# Nom des colonnes et type des fichiers parquet
db.execute(f"""DESCRIBE SELECT * FROM  '{files_input[0].__str__() }'""").df()
## Schéma interne des fichiers parquet
# db.execute("SELECT * FROM parquet_schema(?)", [files_input[0].__str__()]).df()

# 1/ Requêter sur les fichiers parquets

# Créer une vue des fichiers parquet
db.execute(f"""CREATE VIEW parcelle AS SELECT * FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')""")

# Nombre de lignes
db.execute("SELECT COUNT(*) from parcelle").fetchall()

db.execute("""
          SELECT *,
          FROM parcelle
          LIMIT 5
          """).df()



# Harmonisation des colonnes
db.execute("""
          SELECT DISTINCT CODE_DEP||CODE_COM AS 'insee_com',
          NOM_COM AS 'lib_com', COM_ABS AS 'com_abs',
          CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
          FROM parcelle
          """).df()


db.execute("""
          COPY(
          SELECT DISTINCT CODE_DEP||CODE_COM AS 'insee_com',
          NOM_COM AS 'lib_com', COM_ABS AS 'com_abs',
          CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
          FROM parcelle ORDER BY insee_com) TO 'O:/Telechargement/2023-07-com-pci.csv' (HEADER, DELIMITER ',')
          """)


# Harmonisation des colonnes
db.execute("""
          SELECT DISTINCT CODE_DEP||CODE_COM AS 'insee_com',
          NOM_COM AS 'lib_com', COM_ABS AS 'com_abs',
          CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
          FROM parcelle
          """).df()


db.execute("""
          COPY(
          SELECT DISTINCT CODE_DEP||CODE_COM AS 'insee_com',
          NOM_COM AS 'lib_com', COM_ABS AS 'com_abs',
          CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
          FROM parcelle ORDER BY insee_com) TO 'O:/Telechargement/2023-07-com-pci.csv' (HEADER, DELIMITER ',')
          """)







# # Connexion à la base
# db = duckdb.connect(database=path_output.__str__() + "/2023-07-parcelle.duckdb", read_only=False)

# # Nouvelle table
# table_parcelle = """CREATE TABLE IF NOT EXISTS parcelle(
#                     idu VARCHAR NOT NULL,
#                     insee_com VARCHAR NOT NULL CHECK(length(insee_com) == 5),
#                     lib_com VARCHAR NOT NULL,
#                     com_abs VARCHAR NOT NULL CHECK(length(com_abs) == 3),
#                     insee_dep VARCHAR NOT NULL CHECK(length(insee_dep) == 2 OR length(insee_dep) == 3),
#                     )"""

# db.execute(table_parcelle)

# db.execute("SET memory_limit='10GB'").df()
# db.execute("SET threads TO 1").df()

# db.execute(f"""
#            INSERT INTO parcelle
#            SELECT DISTINCT IDU as idu,
#            CODE_DEP||CODE_COM AS 'insee_com',
#            NOM_COM AS 'lib_com', COM_ABS AS 'com_abs',
#            CASE CODE_DEP WHEN '97' THEN CODE_DEP||CODE_COM[0:1] ELSE CODE_DEP END AS 'insee_dep'
#            FROM read_parquet('{path_input.__str__() + "/2023-07*.parquet"}')
#            LIMIT 1000000
#            OFFSET 0
#            """)


# db.execute("RESET memory_limit").df()



# # Fin de la connexion
# db.close()

