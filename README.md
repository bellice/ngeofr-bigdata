# ngeofr-bigdata

## Description

**ngeofr-bigdata** est un projet conçu pour récupérer de grandes bases de données géographiques, les transformer, et les exporter dans des formats performants tels que **Parquet**. Ce projet permet de découper les données par **département** et **EPCI**, en utilisant un référentiel basé sur le projet **ngeofr**.

Le projet gère plusieurs bases de données provenant de diverses sources, chacune étant téléchargée, dézippée, transformée, puis découpée en fichiers **Parquet**.


## Table des matières
- [Installation](#installation)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
- [Structure du projet](#structure-du-projet)
- [FAQ](#faq)
- [Méthodologie](#méthodologie)
- [Sources et bases utilisées](#sources-et-bases-utilisées)
- [Licence](#licence)

## Installation
Pour installer le projet ngeofr-bigdata, clonez le dépôt :

```bash
git clone https://github.com/bellice/ngeofr-bigdata.git
```

## Configuration
Avant d'utiliser le projet, vous devez spécifier le chemin de base où les données seront stockées. Cela se fait dans le fichier `config.yaml`. Le paramètre `base_path` permet de définir le répertoire principal où tous les fichiers et sous-répertoires associés seront créés. 

### Exemple de configuration
Dans le fichier `config.yaml`, définissez le chemin de base comme suit :

```yaml
base_path: "/chemin/vers/les/données"
```

## Utilisation

🚧 En cours de rédaction...

## Structure du projet
```
ngeofr-bigdata/
├── shared/                       # Code partagé entre les modules
│   └── sql/                      # Requêtes SQL du projet
├── sources/                      # Traitement des données par producteur et base
│   │── producteur A/             # Producteur A
│   │── │── base 1                # Base 1
│   │── └── base 2                # Base 2
│   │── producteur B/             # Producteur B
│   └── ...                       # Autre producteurs et bases
│   .gitignore                    # Fichiers et dossiers ignorés par Git
└── config.yaml                   # Fichier de configuration des chemins
└── README.md                     # Documentation du projet
```


## FAQ

🚧 En cours de rédaction...

## Méthodologie

🚧 En cours de rédaction...

## Sources et bases utilisées
Le projet utilise des données provenant de plusieurs producteurs, structurées sous différentes bases. Voici les principales sources et leurs bases associées :

- ![INSEE](https://img.shields.io/badge/Producteur-INSEE-blue)  
  - **Sirene** : Base de données des établissements et des entreprises en France.

- ![IGN](https://img.shields.io/badge/Producteur-IGN-blue)  
  - **BDTopo** : Description des éléments du territoire
  - **GPU** : Géoportail de l'urbanisme.
  - **PCI** : Parcelles cadastrales.
  - **RNB** : Référentiel national du bâtiment.

- ![BRGM](https://img.shields.io/badge/Producteur-BRGM-blue)  
  - **SSP** : Sites et sols pollués.
  - **ICPE** : Installations classées pour la protection de l'environnement.


## Licence
Ce projet est sous licence MIT - voir le fichier [LICENSE](./LICENSE) pour plus de détails