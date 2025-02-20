# utils.py
import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

def load_config():
    """Charge la configuration depuis config.yaml."""
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_env():
    """Charge les variables d'environnement depuis .env."""
    env_path = os.path.abspath(os.path.join(os.getcwd(), ".env"))
    print(f"Chargement de : {env_path}")  # Debug
    load_dotenv(env_path)
    return os.getenv("PROXY_URL")

def get_proxies(proxy_url):
    """Retourne les proxies si proxy_url est défini."""
    return {"http": proxy_url, "https": proxy_url} if proxy_url else None

def create_output_path(base_path, relative_path, sub_path):
    """Crée le chemin de sortie et retourne le chemin complet."""
    output_path = Path(base_path) / relative_path / sub_path
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path