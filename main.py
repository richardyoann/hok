#!/usr/bin/env python
# coding: utf-8
import gc
from pathlib import Path
import sys
from application import Application
from errors import HistorisationError
from flags import FlagManager
from config import charger_config
from utils import log_execution_time, parse_arguments
from logger import get_logger
logger_info= get_logger()

@log_execution_time
def main():
    """
    Fonction principale qui initialise l’application et exécute l’action appropriée
    basé sur des arguments de ligne de commande.

    La fonction configure le gestionnaire d’erreurs, le gestionnaire d’indicateurs et l’instance d’application.
    Il récupère les paramètres de configuration d’un fichier JSON et les utilise pour initialiser 
    la connexion à la base de données. En fonction des arguments de la ligne de commande, il détermine 
    l’action à effectuer ('jour', 'mois', ou 'rapport') et l’exécute avec le
    paramètres spécifiés. La fonction gère les exceptions, consigne les erreurs et s’assure que
    La connexion à la base de données est correctement fermée.

    Returns:
        tuple: Un tuple contenant le code de sortie (0 pour succès, 1 pour erreur) et un résumé
        des messages du gestionnaire d’erreurs.
    """
    try:
        error_handler = HistorisationError()
        flag_manager = FlagManager(error_handler)
        app = Application(error_handler, flag_manager)
        db_controller = None    
        args = parse_arguments()
        config = charger_config('config.json')
        connection_params = {
            'user': getattr(args, 'user', None) or config['connexion']['user'],
            'password': getattr(args, 'password', None) or config['connexion']['password'],
            'host': getattr(args, 'host', None) or config['connexion']['host'],
            'port': getattr(args, 'port', None) or config['connexion']['port'],
            'bdd': getattr(args, 'bdd', None) or config['connexion']['bdd'],
        }

        app.initialize_database(**connection_params)
        db_controller = app.db_connection

        action = 'default'
        if args:
            if args.jour:
                action = 'jour'
            elif args.mois:
                action = 'mois'
            elif args.rapport:
                action = 'rapport'

        date = args.veille if args and args.veille else None
        rapport_dir = Path(args.rapport) if args and args.rapport else None

        exit_code = app.run(action, date, rapport_dir)
        return exit_code, error_handler.get_summary()
    
    except Exception as e:
        error_msg = f"Erreur générale : {str(e)}"
        logger_info.error(error_msg)
        error_handler.add_error(error_msg)
        return 1, error_handler.get_summary()
    
    finally:
        if db_controller:
            try:
                db_controller.disconnect()
            except Exception as e:
                error_msg = f"Erreur lors de la déconnexion : {str(e)}"
                logger_info.error(error_msg)
                error_handler.add_error(error_msg)
        
        gc.collect()

if __name__ == "__main__":
    exitcode, (messages, _) = main()
    if exitcode > 0:
        print(messages)
    sys.exit(exitcode)