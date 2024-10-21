#!/usr/bin/env python
# coding: utf-8
from datetime import date
from pathlib import Path
from typing import Optional

from controllers import TraitementController
from errors import HistorisationError
from flags import FlagManager
from database import DatabaseController

class Application:
    def __init__(self, historization_error: HistorisationError, flag_manager: FlagManager):
        """
        Initialise l'application.
        
        :param historization_error: objet en charge de la gestion des erreurs de l'historisation
        :param flag_manager: objet en charge de la gestion des flags
        """
        self.db_connection = None
        self.historization_error = historization_error
        self.flag_manager = flag_manager

    def initialize_database(self, user: str, password: str, host: str, port: int, bdd: str):        
        """
        Initialise la connexion à la base de données.
        
        :param user: nom de l'utilisateur pour la connexion
        :param password: mot de passe pour la connexion
        :param host: nom de l'hôte
        :param port: port
        :param bdd: nom de la base de données
        """
        self.db_connection = DatabaseController(user, password, host, port, bdd, self.historization_error)

    def run(self, action: str, date: Optional[date] = None, rapport_dir: Optional[Path] = None) -> int:
        traitement_controller = TraitementController(self.db_connection, self.historization_error, self.flag_manager)
        return traitement_controller.process_action(action, date, rapport_dir)