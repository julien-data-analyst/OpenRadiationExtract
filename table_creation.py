############################-
# Auteur : Julien RENOULT
# Sujet : création des différentes tables en JSON (DeviceUsed, ApparatusUsed, etc)
# Date : 09/08/2025
#############################-

# Chargement des librairies
import pandas as pd
import boto3
from pathlib import Path
import os
from logger_write import log_memory_usage
from actualisation_donnees import convert_to_json
from io import BytesIO, StringIO

# Création du cache
LOCAL_DIR = Path("cache") # Dossier cache
bucket_name = os.environ['S3_BUCKET_NAME'] # Le nom du bucket
S3_OBJECT_NAME = "data/openradiation.jsonl" # Chemin pour mettre le fichier parquet dans le S3
S3_MEASUREMENTS = "data/measurements.jsonl"
S3_DEVICE = "data/devices.jsonl"
S3_APPARATUS = "data/apparatus.jsonl"
S3_FLIGHT = "data/flight.jsonl"
MEASUREMENTS_FILE = LOCAL_DIR / "measurements.jsonl" # les mesures obtenus
DEVICE_FILE = LOCAL_DIR / "devices.jsonl" # les appareils de renseignements
APPARATUS_FILE = LOCAL_DIR / "apparatus.jsonl" # les appareils de mesures
FLIGHT_FILE = LOCAL_DIR / "flight.jsonl" # les vols concernés
ALL_FILE = LOCAL_DIR / "openradiation.jsonl" # les mesures obtenus avec tous le détails pour chaque ligne

## Création des différentes fct pour créer les 4 tables ################################# 
def read_json_s3():
    """
    Fonction : permet de lire le fichier JSON contenant toutes les informations concernant les mesures

    Renvoie :
    - df : renvoie le DataFrame contenant ces données
    """

    #nrows = 10000

    # Client du S3 d'AWS
    s3 = boto3.client("s3",
                      aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                      region_name=os.environ['AWS_REGION'])
    
    # Récupérer les données JSON en question
    response = s3.get_object(Bucket=bucket_name, Key=S3_OBJECT_NAME)

    # Lire seulement les N premières lignes
    #lines = []
    #for i, line in enumerate(response['Body'].iter_lines()):
    #    if i >= nrows:
    #        break
    #    lines.append(line.decode('utf-8'))
    
    # Charger en DataFrame
    #df = pd.read_json(StringIO("\n".join(lines)), lines=True)

    # Lecture sous pandas
    df = pd.read_json(BytesIO(response['Body'].read()), lines=True)

    # Nettoyages des ID
    df["deviceUuid"] = df["deviceUuid"].str.lower()
    df["deviceUuid"] = df["deviceUuid"].str.replace('"', '', regex=False)
    df["deviceUuid"] = df["deviceUuid"].replace('', None)
    df["deviceUuid"] = df["deviceUuid"].str.strip()
    df["apparatusId"] = df["apparatusId"].str.lower()
    df["apparatusId"] = df["apparatusId"].str.replace('"', '', regex=False)
    df["apparatusId"] = df["apparatusId"].replace('', None)
    df["apparatusId"] = df["apparatusId"].str.strip()
    df["flightId"] = df["flightId"].astype("Int64")

    return df

def create_measurements_table(df):
    """
    Fonction : permet de créer la table des mesures

    Renvoie :
    - df_measurements : renvoie le DataFrame contenant seulement les informations concernant ces mesures
    """

    # Sélection des colonnes
    df_measurements = df[["reportUuid", "apparatusId", "temperature", "value", "hitsNumber", 
                    "startTime", "endTime", "latitude", "longitude", "deviceUuid", "userId", 
                    "measurementEnvironment", "rain", "storm", "flightId", "dateAndTimeOfCreation"]]

    # Remplir les valeurs nulles par 0
    df_measurements["rain"] = df_measurements["rain"].fillna(0)  
    df_measurements["storm"] = df_measurements["storm"].fillna(0)

    return df_measurements

def create_device_table(df):
    """
    Fonction : permet de créer la table des appareils utilisés pour les mesures

    Renvoie :
    - df_device : renvoie le DataFrame contenant seulement les appareils utilisés pour les mesures
    """

    # Sélection des colonnes
    df_device = df[["deviceUuid", "devicePlatform", "deviceVersion", "deviceModel"]]

    # Enlever les lignes NA de la colonne devieUuid
    df_device = df_device.dropna(subset=["deviceUuid"])

    # Enlever les doublons
    df_device = df_device.drop_duplicates(subset=["deviceUuid"])

    return df_device

def create_apparatus_table(df):
    """
    Fonction : permet de créer la table des appareils utilisés pour le renseignement de ces mesures

    Renvoie :
    - df_apparatus : renvoie le DataFrame contenant seulement les appareils pour le renseignement de mesures
    """

    # Sélection des colonnes
    df_apparatus = df[["apparatusId", "apparatusVersion", "apparatusSensorType", "apparatusTubeType"]]

    # Enlever les lignes NA de la colonne devieUuid
    df_apparatus = df_apparatus.dropna(subset=["apparatusId"])

    # Enlever doublons par rapport à l'identifiant de l'appareil
    df_apparatus = df_apparatus.drop_duplicates(subset=["apparatusId"])

    return df_apparatus

def create_flight_table(df):
    """
    Fonction : permet de créer la table des vols concernés par ces mesures

    Renvoie :
    - df_flight : renvoie le DataFrame contenant seulement les informations concernant les vols
    """

    # Sélection des colonnes
    df_flight = df[["flightId", "flightNumber", "seatNumber", "windowSeat", 
                    "departureTime", "arrivalTime", "airportOrigin", "airportDestination",
                    "aircraftType"]]
    
    # Enlever les lignes manquantes
    df_flight = df_flight.dropna(subset=["flightId"])

    # Enlever les doublons 
    df_flight = df_flight.drop_duplicates(subset=["flightId"])
    
    # Transformer la colonne windowSeat en integer
    df_flight["windowSeat"] = df_flight["windowSeat"].astype("Int64")

    return df_flight

def upload_to_s3(path_fichier, path_s3):
    """
    Fonction : Téléversement d'un fichier JSON vers le S3

    Arguments :
    - path_fichier : chemin du fichier JSON à trouver
    - path_s3 : chemin du fichier à téléverser vers le S3
    """

    msg = f"Téléversement vers S3 ({bucket_name}/{path_s3})..."
    log_memory_usage(msg)
    print(msg)

    # Client du S3 d'AWS
    s3 = boto3.client("s3",
                      aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                      region_name=os.environ['AWS_REGION'])

    # Téléversement du fichier
    with open(path_fichier, "rb") as f:
        s3.upload_fileobj(f, bucket_name, path_s3)
    
    msg = "Fichier envoyé avec succès sur S3."
    log_memory_usage(msg)
    print(msg)
#########################################################################################

# Application des différentes étapes
if __name__=="__main__":

    # Lecture des données
    df = read_json_s3()

    print(df.head())

    # Création des différentes tables
    df_measure = create_measurements_table(df)
    df_device = create_device_table(df)
    df_apparatus = create_apparatus_table(df)
    df_flight = create_flight_table(df)

    
    print("\n---- Les résultats de nos DataFrames ----\n")
    print("Les cinq premières lignes pour les mesures : ", df_measure.head())
    print("Les cinq premières lignes pour les appareils de renseignement : ", df_device.head())
    print("Les cinq premières lignes pour les appareils de mesures : ", df_apparatus.head())
    print("Les cinq premières lignes pour les vols : ", df_flight.head())

    # Conversion vers le JSON de ces différentes tables
    chemin_measure = convert_to_json(df_measure, MEASUREMENTS_FILE)
    chemin_device = convert_to_json(df_device, DEVICE_FILE)
    chemin_apparatus = convert_to_json(df_apparatus, APPARATUS_FILE)
    chemin_flight = convert_to_json(df_flight, FLIGHT_FILE)
    chemin_all = convert_to_json(df, ALL_FILE)
    
    # Téléversement dans le S3
    upload_to_s3(MEASUREMENTS_FILE, S3_MEASUREMENTS)
    upload_to_s3(DEVICE_FILE, S3_DEVICE)
    upload_to_s3(APPARATUS_FILE, S3_APPARATUS)
    upload_to_s3(FLIGHT_FILE, S3_FLIGHT)
    upload_to_s3(ALL_FILE, S3_OBJECT_NAME)