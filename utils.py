#!/usr/bin/env python
# coding: utf-8
import argparse
import gc
from pathlib import Path
import sys
import time
from typing import Optional
import pandas as pd
from contextlib import contextmanager


from config import SQL_FORMAT_LABELS
from errors import HistorisationError
from model import DatabaseModel

from logger import get_logger
logger_info= get_logger()


def log_execution_time(func):
    
    """
    Décorateur qui enregistre le temps d’exécution de la fonction donnée.

    Le temps d’exécution est enregistré au niveau INFO avec le nom de la fonction.

    :param func: The decorated function..
    :return: The decorated function.
    """
    def wrapper(*args, **kwargs):
        """
        Encapsule la fonction donnée pour consigner son temps d’exécution.

        Le temps d’exécution est enregistré au niveau INFO avec le nom de la fonction.

        :param args: Argument passés pour la fonction.
        :param kwargs: Les arguments nommés passés à la fonction.
        :return: Text permettant de connaitre le temps d’exécution de la fonction en secondes .
        """        
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger_info.info(f"Exécution de {func.__name__} terminée en {execution_time:.2f} secondes")
        return result
    return wrapper

@contextmanager
def manage_dataframe(df: pd.DataFrame):
    """
    Gestionnaire de contexte qui supprime le DataFrame donné et effectue le nettoyage de la mémoire lorsqu’il est quitté.    
    """
    try:
        yield df
    finally:
        del df
        gc.collect()

def parse_arguments() -> Optional[argparse.Namespace]:
    """Parse les arguments de ligne de commande ou retourne None si exécuté dans Jupyter."""
    if 'ipykernel' in sys.modules:
        # Exécution dans Jupyter, pas de parsing d'arguments
        return argparse.Namespace(jour=False, mois=False, veille=False, sup=False, rapport=None, user=None, password=None, host=None, port=None, bdd=None)
    else:
        # Exécution comme script autonome, parser les arguments
        parser = argparse.ArgumentParser(description="Traitement des données journalières et mensuelles")
        parser.add_argument('--jour', action='store_true', help="Force le recalcul des données journalières")
        parser.add_argument('--mois', action='store_true', help="Force le recalcul des données mensuelles")
        parser.add_argument('--veille', action='store_true', help="Force le recalcul des données de la veille")
        parser.add_argument('--sup', action='store_true', help="Force la suppression des données journalières")
        parser.add_argument('--rapport', type=Path, help="Chemin vers le répertoire du rapport")
        parser.add_argument('--user', help="Nom d'utilisateur pour la connexion à la base de données")
        parser.add_argument('--password', help="Mot de passe pour la connexion à la base de données")
        parser.add_argument('--host', help="Hôte de la base de données")
        parser.add_argument('--port', type=int, help="Port de la base de données")
        parser.add_argument('--bdd', help="Nom de la base de données")
        return parser.parse_args() 
    
class SQLFileProcessor:
    def __init__(self, db_connection: DatabaseModel, historization_error: HistorisationError):
        """
        Initialise l'objet SQLFileProcessor pour traiter les fichiers SQL.

        :param db_connection: Instance de DatabaseModel pour interagir avec la base de données.
        :param historization_error: Objet HistorisationError pour gérer les erreurs liées à l'historisation.
        """
        self.db_connection = db_connection
        self.historization_error = historization_error

    def process_file(self, sql_file: Path) -> Optional[pd.DataFrame]:
        """
        Traitement d'un fichier SQL :
        
        * Lit le fichier SQL grâce à la méthode read_sql_query_file() de DatabaseModel
        * Vérifie que le résultat est un DataFrame
        * Vérifie que les colonnes obligatoires sont présentes
        * Retourne le DataFrame si tout est OK, ou None si le fichier est vide
        * Lance une erreur si le fichier est mal formé ou si les colonnes obligatoires sont manquantes

        :param sql_file: Chemin du fichier SQL à traiter
        :return: DataFrame résultant de la requête, ou None si le fichier est vide
        :raises HistorisationError: Si le fichier est mal formé ou si les colonnes obligatoires sont manquantes
        """
        try:
            data = self.db_connection.read_sql_query_file(sql_file)
            if data is None:
                logger_info.warning(f"La requête du fichier {sql_file} a retourné None.")
                return None
            
            if not isinstance(data, pd.DataFrame):                
                logger_info.warning(f"La requête n'a pas retourné un DataFrame valide.")
                return None

            if data.empty:
                logger_info.warning(f"Le fichier {sql_file} a retourné un DataFrame vide.")
                return None

            missing_columns = set(SQL_FORMAT_LABELS) - set(data.columns)
            if missing_columns:
                error_msg = f"Colonnes manquantes dans le fichier {sql_file}: {', '.join(missing_columns)}"
                logger_info.error(error_msg)                
                self.historization_error.add_error(error_msg)            

            return data

        except Exception as e:
            error_msg = f"Erreur lors du traitement de {sql_file}: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            return None
            