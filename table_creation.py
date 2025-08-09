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

# Création du cache
LOCAL_DIR = Path("cache") # Dossier cache

## Création des différentes fct pour créer les 4 tables ################################# 
def read_json_s3():
    """
    Fonction : permet de lire le fichier JSON contenant toutes les informations concernant les mesures

    Renvoie :
    - df : renvoie le DataFrame contenant ces données
    """

def create_measurements_table(df):
    """
    Fonction : permet de créer la table des mesures

    Renvoie :
    - df_measurements : renvoie le DataFrame contenant seulement les informations concernant ces mesures
    """

def create_device_table(df):
    """
    Fonction : permet de créer la table des appareils utilisés pour les mesures

    Renvoie :
    - df_device : renvoie le DataFrame contenant seulement les appareils utilisés pour les mesures
    """

def create_apparatus_table(df):
    """
    Fonction : permet de créer la table des appareils utilisés pour le renseignement de ces mesures

    Renvoie :
    - df_apparatus : renvoie le DataFrame contenant seulement les appareils pour le renseignement de mesures
    """

def create_flight_table(df):
    """
    Fonction : permet de créer la table des vols concernés par ces mesures

    Renvoie :
    - df_flight : renvoie le DataFrame contenant seulement les informations concernant les vols
    """

def upload_to_s3(nom_fichier, df):
    """
    Fonction : Téléversement d'un fichier JSON vers le S3
    """
#########################################################################################

# Application des différentes étapes
if __name__=="__main__":

    # Lecture des données
    df = read_json_s3()

    # Création des différentes tables
    df_measure = create_measurements_table(df)
    df_device = create_device_table(df)
    df_apparatus = create_apparatus_table(df)
    df_flight = create_flight_table(df)

    # Conversion vers le JSON de ces différentes tables


    # Téléversement dans le S3