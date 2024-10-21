#!/usr/bin/env python
# coding: utf-8
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

def setup_logger(log_file: Path, log_level: str = 'INFO'):
    # Créer le répertoire des logs s'il n'existe pas
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Configurer le logger
    logger = logging.getLogger('application_logger')
    logger.setLevel(logging.getLevelName(log_level))

    # Créer un handler de fichier qui écrit les messages de log dans un fichier
    file_handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)  # 10MB par fichier, max 5 fichiers
    file_handler.setLevel(logging.getLevelName(log_level))

    # Créer un handler de console qui écrit les messages de log sur la console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.getLevelName(log_level))

    # Créer un formateur et l'ajouter aux handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Ajouter les handlers au logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Initialiser le logger
LOG_FILE = Path('logs/application.log')
logger = setup_logger(LOG_FILE)

# Fonction pour obtenir le logger
def get_logger():
    return logger