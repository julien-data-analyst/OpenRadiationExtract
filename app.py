###############################-
# Auteur : Julien RENOULT
# Sujet : création d'une application Flask permettant de récupérer facilement une extraction de données
# Date : 24/07/2025
###############################-

# Importation des librairies
from flask import Flask, Response
import tarfile
import requests
from pathlib import Path
import time
#from memory_profiler import profile
import pandas as pd
import psutil
import os
import warnings
#from memory_profiler import memory_usage

# Enlever les avertissements de Pandas (notamment pour le read_csv)
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

def get_memory_usage_mb():
    """
    Récupérer l'utilisation en mémoire du programme en question
    """
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 ** 2)  # Convertir en MB

# Création de l'application
app = Flask(__name__)

# URL de la requête HTTP utilisée pour récupérer le fichier compressé
url_open_radiation = "https://request.openradiation.net/openradiation_dataset.tar.gz"

# Chemin local pour le cache (évite le re-téléchargement)
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TAR_FILE = CACHE_DIR / "openradiation_dataset.tar.gz"
CACHE_CSV_FILE = CACHE_DIR / "measurements.csv"
# Pour les logs
LOG_FILE = CACHE_DIR / "memoire.log"

# Durée maximale de validité du cache (en secondes) = 24h
MAX_CACHE_AGE = 24 * 60 * 60  # 86400 secondes

# Fonction : télécharger et extraire le fichier CSV une seule fois
#@profile
def ensure_csv_cached():
    """
    Fonction : Extraction des données CSV de la requête HTTP pour les mesures de radiations (measurements.csv)
    """
    
    # Cache CSV à regarder s'il est encore valide
    update_required = True

    # S'il existe
    if CACHE_CSV_FILE.exists():
        
        # Temps d'âge du fichier (-/+ 24 heure)
        age = time.time() - CACHE_CSV_FILE.stat().st_mtime

        # Si le fichier date de moins de 24 heure
        if age < MAX_CACHE_AGE:

            # Alors on ne le mets pas à jour
            update_required = False
            print("Cache CSV encore valide (moins de 24h)")

    # S'il existe et qu'il a plus de 24 heures qu'il existe ou qu'il n'existe pas, le télécharger et le mettre en cache
    if update_required:

        # Alors, on le mets à jour (en streaming)
        print("Mise à jour du cache CSV (ancien ou absent)")
        response = requests.get(url_open_radiation, stream=True)
            
        with open(CACHE_TAR_FILE, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Ouverture du fichier compressé et extraction du fichier CSV
        with tarfile.open(CACHE_TAR_FILE, mode="r:gz") as tar:

            # Chercher le fichier en question
            member = next((m for m in tar.getmembers() if m.name.endswith("measurements.csv")), None)
            
            # Si pas trouvée, alors on renvoie une erreur de type fichier non trouvé
            if not member:
                raise FileNotFoundError("measurements.csv introuvable dans l'archive")

            # Extraire le fichier CSV et le lire pour en extraire les données / pour remplacer l'ancienne extraction
            with tar.extractfile(member) as f_in, open(CACHE_CSV_FILE, "wb") as f_out:
                f_out.write(f_in.read())
        
        # Appel de la fonction
        mem_used = get_memory_usage_mb()
        print(f"Mémoire utilisée extraction dans le dossier compressé : {mem_used}")
        print("Fichier CSV mis à jour dans le cache")

# Permet d'enregistrer les différents étapes de mémoires utilisées
def log_memory_usage(message: str):
    """
    Fonction : permet d'enregistrer un message dans le log en indiquant la date et l'heure de création qui va avec.

    Arguments :
    - message : le message à mettre dans le fichier

    """
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")

# Vérification de l'utilisation de la mémoire (limite 512 MB)
def get_memory_usage_mb():
    """
    Fonction : permet de revoyer l'utilisation de mémoire de l'application
    """
    # permet de connaître le coût en mémoire du processus
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()

    return mem_info.rss / (1024 ** 2)  # Convertir en MB


##########################################-
# Création des différentes routes 
##########################################-

# Ajout d'une page main
@app.route('/', methods=["GET", "POST"]) # Préciser les requêtes http possibles, sinon erreur 405 (si autre que GET)
def index():
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
        <pre>{logs}</pre>
    """

    return html

@app.route('/api/data/', methods=["GET"])
def renitialiser_cache():
    """
    Fonction : Permet d'enlever le cache pour un refraîchissement des données
    """

    # Supprimer le cache pour garantir des données fraîches (dans le cas du redémarrage du serveur)
    if CACHE_CSV_FILE.exists():
        print("Suppression du cache existant...")
        CACHE_CSV_FILE.unlink()
        return "<h1> Suppression de la cache réussie </h1>"
    else:
        return "<h1> La cache n'existait pas avant </h1>"

# Api pour récupérer les mesures d'OpenRadiation
#@profile
@app.route('/api/data/measurements.csv')
def streaming_csv_measurements():
    """
    Fonction : permet d'avoir les données CSV concernant les mesures de radioactivité en Streaming
    Retour : résultat CSV de la requête HTTP
    """

    # Vérifie que le fichier est en cache, sinon le télécharge et l'extrait du fichier compressé
    ensure_csv_cached()

    # Mémoire plus faible
    def generate():
        for i, chunk in enumerate(pd.read_csv(CACHE_CSV_FILE, sep=";", chunksize=100_000)):
            # Retourne la réponse HTTP
            mem_used = get_memory_usage_mb()
            msg = f"Mémoire utilisée dans la partie extraction CSV : {mem_used} MB"
            #print(msg)
            log_memory_usage(msg) # On enregistre
            yield chunk.to_csv(index=False, header=(i == 0))


    return Response(generate(), mimetype="text/csv")

#################################-
#################################-

if __name__=="__main__":
    app.run(debug=True)