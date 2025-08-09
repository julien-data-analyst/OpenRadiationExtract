###################-
# Auteur : Julien RENOULT
# Date : 09/08/2025
# Sujet : Création de fonctions pour le logging
###################-

# Chargement de la librairie
import time

# Pour les logs
LOG_FILE = "memoire.log"


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