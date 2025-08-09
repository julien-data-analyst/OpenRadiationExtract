###############################-
# Auteur : Julien RENOULT
# Sujet : création d'une application Flask permettant de récupérer facilement une extraction de données
# Date : 24/07/2025 - 04/08/2025
###############################-

# Chargement des librairies
import tarfile
import requests
import pandas as pd
import boto3
from pathlib import Path
import os
from logger_write import log_memory_usage

# --------- CONFIGURATION ----------
URL_TAR = "https://request.openradiation.net/openradiation_dataset.tar.gz" # URL pour récupérer le fichier compressé
LOCAL_DIR = Path("cache") # Dossier cache
LOCAL_TAR = LOCAL_DIR / "openradiation_dataset.tar.gz" # Emplacement du fichier compressé
JSON_FILE = LOCAL_DIR / "openradiation.jsonl" # Emplacement du fichier JSON

# AWS S3
bucket_name = os.environ['S3_BUCKET_NAME'] # Le nom du bucket
S3_OBJECT_NAME = "data/openradiation.jsonl" # Chemin pour mettre le fichier parquet dans le S3

# ----------------------------------

LOCAL_DIR.mkdir(parents=True, exist_ok=True) # Créer le dossier s'il n'existe pas

def download_tar():
    """
    Fonction : permet de télécharger le fichier compressé (TAR) contenant le fichier CSV à extraire.
    """
    msg = "Téléchargement du fichier compressé de la requête HTML..."
    log_memory_usage(msg)
    print(msg)

    # Requête en streaming
    response = requests.get(URL_TAR, stream=True)
    response.raise_for_status()

    # Récupération du contenue
    with open(LOCAL_TAR, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    msg = "Téléchargement terminé."
    log_memory_usage(msg)
    print(msg)

def extract_csv_from_tar():
    """
    Fonction : permet d'extraire le fichier CSV du fichier compressé.
    """

    msg = "Extraction du fichier CSV depuis l'archive..."
    log_memory_usage(msg)
    print(msg)

    # Ouvrir le fichier compressé TAR
    with tarfile.open(LOCAL_TAR, mode="r:gz") as tar:
        
        # Trouver le fichier CSV
        member = next((m for m in tar.getmembers() if m.name.endswith("measurements.csv")), None)
        if not member:
            raise FileNotFoundError("measurements.csv introuvable dans l'archive")

        # Extraction du fichier CSV
        file = tar.extractfile(member)
        if not file:
            raise RuntimeError("Impossible d'extraire le fichier CSV.")
        
        # Retourne le DataFrame du fichier CSV
        return pd.read_csv(file, 
                        sep=";",
                        header=0)
    
    msg = "Extraction du fichier CSV terminé"
    log_memory_usage(msg)
    print(msg)

def convert_to_json(df: pd.DataFrame):
    """
    Fonction : Conversion en JSON les données du DataFrame.
    """

    # Utilisation de Pandas pour convertir vers un fichier PARQUET
    msg = "Conversion en JSON des données..."
    log_memory_usage(msg)
    print(msg)
    df.to_json(JSON_FILE, orient="records", lines=True)
    msg = f"Fichier JSON généré : {JSON_FILE}"
    log_memory_usage(msg)
    print(msg)

def upload_to_s3():
    """
    Fonction : Téléversement du fichier JSON vers le bucket S3
    """
    
    msg = f"Téléversement vers S3 ({bucket_name}/{S3_OBJECT_NAME})..."
    log_memory_usage(msg)
    print(msg)

    # Client du S3 d'AWS
    s3 = boto3.client("s3",
                      aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                      region_name=os.environ['AWS_REGION'])

    # Téléversement du fichier
    with open(JSON_FILE, "rb") as f:
        s3.upload_fileobj(f, bucket_name, S3_OBJECT_NAME)
    
    msg = "Fichier envoyé avec succès sur S3."
    log_memory_usage(msg)
    print(msg)

if __name__ == "__main__":
    # Exécution des différentes étapes
    download_tar() # Téléchargement du fichier compressé
    df = extract_csv_from_tar() # Extraire le fichier CSV du fichier compressé
    convert_to_json(df) # Conversion en PARQUET
    upload_to_s3() # Téléversement du fichier vers le S3
