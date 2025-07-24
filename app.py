###############################-
# Auteur : Julien RENOULT
# Sujet : création d'une application Flask permettant de récupérer facilement une extraction de données
# Date : 24/07/2025
###############################-

# Importation des librairies
from flask import Flask, Response
import pandas as pd
import tarfile
import requests
from pathlib import Path
import time

# Création de l'application
app = Flask(__name__)

# URL de la requête HTTP utilisée pour récupérer le fichier compressé
url_open_radiation = "https://request.openradiation.net/openradiation_dataset.tar.gz"

# Chemin local pour le cache (évite le re-téléchargement)
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TAR_FILE = CACHE_DIR / "openradiation_dataset.tar.gz"
CACHE_CSV_FILE = CACHE_DIR / "measurements.csv"

# Durée maximale de validité du cache (en secondes) = 24h
MAX_CACHE_AGE = 24 * 60 * 60  # 86400 secondes

# Fonction : télécharger et extraire le fichier CSV une seule fois
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

        # Alors, on le mets à jour
        print("Mise à jour du cache CSV (ancien ou absent)")
        response = requests.get(url_open_radiation)
        CACHE_TAR_FILE.write_bytes(response.content)

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

        print("Fichier CSV mis à jour dans le cache")

##########################################-
# Création des différentes routes 
##########################################-

# Ajout d'une page main
@app.route('/', methods=["GET", "POST"]) # Préciser les requêtes http possibles, sinon erreur 405 (si autre que GET)
def index():
    return '<h1>Bienvenue sur le serveur Flask des données concernant OpenRadiation</h1>'

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
@app.route('/api/data/measurements.csv')
def streaming_csv_measurements():
    """
    Fonction : permet d'avoir les données CSV concernant les mesures de radioactivité en Streaming
    Retour : résultat CSV de la requête HTTP
    """

    # Vérifie que le fichier est en cache, sinon le télécharge et l'extrait du fichier compressé
    ensure_csv_cached()

    # Fonction de Streaming CSV
    def generate():
        with open(CACHE_CSV_FILE, "rb") as file:
            # Lire l'en-tête séparément
            header = pd.read_csv(file, sep=";", nrows=0).to_csv(index=False)
            yield header

            # Remettre à zéro pour tout relire
            file.seek(0)
            for chunk in pd.read_csv(file, sep=";", chunksize=5_000):
                yield chunk.to_csv(index=False, header=False)

    # Retourne la réponse HTTP
    return Response(generate(), mimetype="text/csv")

#################################-
#################################-

if __name__=="__main__":
    app.run(debug=True)