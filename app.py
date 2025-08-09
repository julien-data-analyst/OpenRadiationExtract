###############################-
# Auteur : Julien RENOULT
# Sujet : création d'une application Flask permettant de récupérer facilement une extraction de données
# Date : 24/07/2025
###############################-

# Importation des librairies
from flask import Flask, Response, redirect, request
import sys
from logger_write import log_memory_usage, LOG_FILE
import boto3
import json
import datetime

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
# AWS S3
bucket_name = os.environ['S3_BUCKET_NAME'] # Le nom du bucket
S3_OBJECT_NAME = "data/measurements.jsonl" # Chemin pour mettre le fichier parquet dans le S3
EXPIRES_IN = 300  # 5 minutes

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

    # Création du client S3
    s3 = boto3.client(
        's3',
        region_name=os.environ['AWS_REGION'],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    # Lecture du paramètre de requête (par défaut False)
    filter_last_two_years = request.args.get("filter_last_two_years", "false").lower() == "true"

    # Calcul de la date limite si filtrage actif
    if filter_last_two_years:
        now = datetime.datetime.now(datetime.timezone.utc)
        annee_courante = now.year
        annee_precedente = annee_courante - 1

    # Récupération du fichier S3 et du corps du fichier
    s3_object = s3.get_object(Bucket=bucket_name, Key=S3_OBJECT_NAME)
    s3_body = s3_object['Body']

    # Fonction génératrice pour streamer ligne par ligne avec filtrage optionnel
    def generate():
        """
        Fonction : fonction génératrice permettant de streamer ligne par ligne avec filtrage des deux dernières années optionnel.
        """

        # Pour parcourir chaque ligne du fichier JSON en question
        for line in s3_body.iter_lines():
            
            # Si la ligne est non vide
            if line:

                # On essaye de le récupérer pour l'enregistrer dans notre base de données
                try:

                    # Dans le cas où filter_last_two_years=true alors on vérifie si la date de création de cette ligne est dans l'année n ou n-1
                    data = json.loads(line)

                    # Filtrage optionnel si on l'a précisé
                    if filter_last_two_years:

                        # Convertit la date en objet datetime
                        date_mesure = datetime.datetime.fromisoformat(data["dateAndTimeOfCreation"].replace("Z", "+00:00"))

                        # Condition dans les deux années
                        if date_mesure.year in (annee_courante, annee_precedente):
                            yield json.dumps(data) + "\n"
                        else:
                            break # On arrête l'extraction des données car on a atteint les mesures trop vieilles (ordre décroissante)
                    
                    # Si le filtrage n'est pas à appliquer, alors on le mets directement
                    else:
                        yield line.decode("utf-8") + "\n"

                except Exception as e:
                    # En cas de problème de parsing JSON ou date, on passe cette ligne
                    pass

    # Retourne les données en streaming
    return Response(generate(), mimetype="application/json")

@app.route('/s3-url')
def generate_signed_url():
    """
    Fonction : permet de générer une URL du S3 avec accès et de récupérer le fichier JSON en question (cela le télécharge directement)
    """

    # Client boto3
    s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'])

    # Permet de générer une URL temporairer pour télécharger le fichier
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': S3_OBJECT_NAME},
        ExpiresIn=EXPIRES_IN
    )

    # Retourne l'URL générer temporairement
    return redirect(url)
#################################-
#################################-

if __name__=="__main__":
    app.run(debug=True)