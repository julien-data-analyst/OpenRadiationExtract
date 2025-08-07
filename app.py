###############################-
# Auteur : Julien RENOULT
# Sujet : création d'une application Flask permettant de récupérer facilement une extraction de données
# Date : 24/07/2025
###############################-

# Importation des librairies
from flask import Flask, Response
from pathlib import Path
import sys
from logger_write import log_memory_usage
import boto3

if sys.platform == "win32":
    import psutil

    def get_memory_usage_mb():
        """
        Fonction : utilisation de la mémoire avec psutil en MB sous Windows.
        """
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 ** 2)
else:
    import resource

    def get_memory_usage_mb():
        """
        Fonction : utilisation de la mémoire avec psutil en MB sous Linux.
        """
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / 1024
    
import os

# Création de l'application
app = Flask(__name__)

# URL de la requête HTTP utilisée pour récupérer le fichier compressé
url_open_radiation = "https://request.openradiation.net/openradiation_dataset.tar.gz"

# Chemin local pour le cache (évite le re-téléchargement)
CACHE_DIR = Path("cache")
# AWS S3
bucket_name = os.environ['S3_BUCKET_NAME'] # Le nom du bucket
S3_OBJECT_NAME = "data/measurements.jsonl" # Chemin pour mettre le fichier parquet dans le S3
# Pour les logs
LOG_FILE = CACHE_DIR / "memoire.log"

##########################################-
# Création des différentes routes 
##########################################-

# Ajout d'une page main
@app.route('/', methods=["GET", "POST"]) # Préciser les requêtes http possibles, sinon erreur 405 (si autre que GET)
def index():
    """
    Fonction : Page d'accueil de l'API.
    """
    return '<h1>Bienvenue sur le serveur Flask des données concernant OpenRadiation</h1>'

@app.route("/memoire")
def memoire_utilisee():
    """
    Fonction : Page sur les mémoires utilisées de l'application Flask.
    """

    # Récupérer la mémoire utilisée pour cette page
    mem_used = get_memory_usage_mb()
    log_memory_usage(f"Consultation mémoire actuelle : {mem_used:.2f} MB")

    # Récupérer les différents logs concernant l'extraction
    if LOG_FILE.exists():
        logs = LOG_FILE.read_text(encoding="utf-8")
    else:
        logs = "Aucun log trouvé."

    # Ajout dans un HTML
    html = f"""
        <h1>Utilisation mémoire actuelle : {mem_used:.2f} MB</h1>
        <h2>Historique d'utilisation mémoire</h2>
        <pre>{logs}</pre>
    """

    return html

# Nettoyer le log
@app.route("/memoire/reset")
def reset_log():
    """
    Fonction : Enlever tous les logs enregistrés jusqu'à maintenant
    """
    if LOG_FILE.exists():
        LOG_FILE.unlink()
    return "<h1>Log mémoire réinitialisé</h1>"

# Api pour récupérer les mesures d'OpenRadiation
#@profile
@app.route('/api/data/measurements')
def streaming_json_measurements():
    """
    Fonction : permet d'avoir les données JSON concernant les mesures de radioactivité en Streaming
    Retour : résultat JSON de la requête HTTP
    """

    # Configuration de S3
    s3_key = "data/measurements.jsonl"  # chemin dans S3

    # Création du client S3
    s3 = boto3.client(
        's3',
        region_name=os.environ['AWS_REGION'],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    # Récupération du fichier S3
    s3_object = s3.get_object(Bucket=bucket_name, Key=s3_key)
    s3_body = s3_object['Body']

    # Fonction génératrice pour streamer ligne par ligne
    def generate():
        for line in s3_body.iter_lines():
            if line:
                yield line.decode("utf-8") + "\n"

    # Retourne les données en streaming
    return Response(generate(), mimetype="application/json")

#################################-
#################################-

if __name__=="__main__":
    app.run(debug=True)