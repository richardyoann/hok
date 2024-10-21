#!/usr/bin/env python
# coding: utf-8
import json
from typing import List
from pathlib import Path
from logger import get_logger
logger_info= get_logger()


def charger_config(fichier_config: str) -> dict:
    """
    Lit le fichier de configuration JSON et le renvoie sous forme de dictionnaire
    :param fichier_config: chemin absolu du fichier de configuration
    :return: dictionnaire contenant les paramètres de configuration
    :raises FileNotFoundError: si le fichier n'a pas été trouvé
    :raises json.JSONDecodeError: si le fichier est mal formé
    :raises PermissionError: si les permissions de lecture sont insuffisantes
    :raises Exception: si une erreur inattendue se produit
    """
    try:
        with open(fichier_config, 'r', encoding='utf-8') as fichier:
            contenu = fichier.read()            
            return json.loads(contenu)        
    except json.JSONDecodeError as e:
        logger_info.error(f"Erreur de décodage JSON dans le fichier de configuration : {str(e)}")
        raise
    except FileNotFoundError:
        logger_info.error(f"Le fichier de configuration '{fichier_config}' n'a pas été trouvé")
        raise
    except PermissionError:
        logger_info.error(f"Permissions insuffisantes pour lire le fichier de configuration '{fichier_config}'")
        raise
    except Exception as e:
        logger_info.error(f"Erreur inattendue lors de la lecture du fichier de configuration : {str(e)}")
        raise    

config = charger_config('config.json')

# Chemins des fichiers et logs
SQLPATH: Path = Path(config['paths']['sql_path'])
LOGPATH: Path = Path(config['paths']['log_path'])
LOGFILE: Path = Path(config['paths']['name_log_path'])
    
# Chemin odbc.ini
ODBC_INI: Path = Path('../.odbc.ini')  

# Chaînes de connexion ODBC
ODBC_STRING_R: str = "DTA_lecture"
ODBC_STRING_W: str = "DTA_lecture"

# Structure des tables de la BDD prod
SCHEMA: str = config['connexion']['schema_preprod']

SQL_MOIS: str = 'indic_mois_kpi'

# Colonnes attendues dans le résultat des requêtes SQL
SQL_FORMAT_LABELS: List[str] = config['module']['sql_format_labels']

# Fichiers flag pour ne pas recalculer les données journalières et mensuelles
FLAG_FILE_DAILY: Path = Path(config['paths']['flag_file_daily'])
FLAG_FILE_MONTHLY: Path = Path(config['paths']['flag_file_monthly'])
