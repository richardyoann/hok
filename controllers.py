#!/usr/bin/env python
# coding: utf-8
import time
from datetime import datetime, timedelta, date
import os
from pathlib import Path
from typing import List, Optional, Dict, Union, Tuple
from config import config,FLAG_FILE_DAILY, FLAG_FILE_MONTHLY, SCHEMA, SQL_MOIS, SQLPATH
from model import CalcModel, DatabaseModel, JourModel, MailleModel, MoisModel, RapportModel, Tables, TraitementModel
import pandas as pd
import sqlalchemy
from errors import HistorisationError
from flags import FlagManager, FlagAction, FlagType
from utils import SQLFileProcessor, log_execution_time, manage_dataframe
from logger import get_logger
from enum import Enum, auto
logger_info= get_logger()

class DatabaseController:
    def __init__(self, user: str, password: str, host: str, port: int, bdd: str, historization_error: HistorisationError):
        """
        Initialise un objet DatabaseController pour gérer les interactions avec une base PostgreSQL.
        
        :param user: Nom de l'utilisateur pour la connexion
        :param password: Mot de passe pour la connexion
        :param host: Nom de l'hôte
        :param port: Port
        :param bdd: Nom de la base de données
        :param historization_error: Objet en charge de la gestion des erreurs de l'historisation
        """
        self.historization_error = historization_error
        self.connection_string = DatabaseModel.prepare_connection_string(user, password, host, port, bdd)
        self.engine = None
        self.connection = None
        self._initialize_engine()

    def _initialize_engine(self):
        """
        Initialise l'engine SQLAlchemy pour la connexion à la base de données.
        Fait une tentative de création de l'engine, puis logue un message d'erreur
        et relève une erreur d'historisation si cela échoue.
        """
        try:
            self.engine = sqlalchemy.create_engine(self.connection_string)
            logger_info.info("Initialisation réussie de DatabaseController")
        except Exception as e:
            error_msg = f"Erreur lors de l'initialisation de l'engine : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    def connect(self) -> sqlalchemy.engine.base.Connection:
        """
        Crée une connexion avec la base de données.
        
        :return: Un objet Connection de l'engine SQLAlchemy.
        :raises: HistorisationError si la connexion échoue.
        """
        try:
            if not self.connection or self.connection.closed:
                self.connection = self.engine.connect()
            return self.connection
        except Exception as e:
            error_msg = f"Erreur lors de la connexion avec la bdd : {str(e)}. Le script s'arrête."
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    def disconnect(self) -> None:
        """
        Ferme la connexion avec la base de données et libère les ressources associées.

        :raises: HistorisationError si la déconnexion échoue.
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            self.engine.dispose()
            logger_info.info("Connexion fermée avec succès")
        except Exception as e:
            error_msg = f"Erreur lors de la déconnexion avec la bdd : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def read_sql_query(self, sql: str) -> pd.DataFrame:
        """
        Exécute une requête SQL en lecture et renvoie le résultat dans un pandas.DataFrame.
        
        :param sql: La requête SQL à exécuter
        :return: Un pandas.DataFrame contenant le résultat de la requête
        :raises: HistorisationError si l'exécution de la requête échoue
        """
       
        try:
            query = sqlalchemy.text(sql)
            with self.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            error_msg = f"Erreur lors de l'exécution de la requête SQL en lecture : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def execute_query(self, sql: str):
        """
        Exécute une requête SQL en écriture et renvoie le résultat de l'exécution.
        
        :param sql: La requête SQL à exécuter
        :return: Un objet ResultProxy contenant le résultat de l'exécution
        :raises: HistorisationError si l'exécution de la requête échoue
        """
        try:
            with self.connect() as conn:
                return conn.execute(sql)
        except Exception as e:
            error_msg = f"Erreur lors de l'exécution de la requête SQL en écriture : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def read_sql_query_file(self, sqlfilename: Path) -> pd.DataFrame:
        """
        Lit un fichier contenant une requête SQL, l'exécute en lecture et renvoie le résultat dans un pandas.DataFrame.
        
        :param sqlfilename: Le nom du fichier contenant la requête SQL
        :return: Un pandas.DataFrame contenant le résultat de la requête
        :raises: HistorisationError si l'exécution de la requête échoue
        """
        
        try:
            with sqlfilename.open('r', encoding='utf-8') as fd:
                requete = fd.read()
            if not isinstance(requete, str):
                raise ValueError(f"Le contenu du fichier {sqlfilename} n'est pas une chaîne de caractères valide.")
            return self.read_sql_query(requete)
        except Exception as e:
            error_msg = f"Erreur lors de la lecture et exécution du fichier SQL {sqlfilename}: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def insert_dataframe(self, df_out: pd.DataFrame, table: str, schema: str, if_exists: str = 'append') -> None:
        """
        Insére un DataFrame dans une table specifiée dans la base de données.

        :param df_out: DataFrame à inserer.
        :param table: Nom de la table.
        :param schema: Schema de la table.
        :param if_exists: Action à effectuer si la table existe déjà (la valeur par défaut est 'append').
        :raises: HistorizationError si une erreur intervient de l'insertion.
        """
        try:
            df_out.to_sql(table, self.engine, schema=schema, index=False, if_exists=if_exists)
            logger_info.info(f"Insertion réussie de {len(df_out)} lignes dans {schema}.{table}")
        except Exception as e:
            error_msg = f"Erreur lors de l'insertion d'un DataFrame dans {schema}.{table} : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def delete_day_data(self, schema: str, table: str, date: Union[str, datetime, date]) -> None:
        """
        Delete des données de journialieres.
        
        :param table: Nom de la table.
        :param schema: Schema de la table.
        :param date: Date à supprimer
        :raises: HistorisationError si une erreur intervient de la suppression
        """
        try:
            query = DatabaseModel.prepare_delete_day_query(schema, table, date)
            self.execute_query(query)
            logger_info.info(f"Données du {date} supprimées de la table {table}")
        except Exception as e:
            error_msg = f"Erreur lors de la suppression des données du jour : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def delete_month_data(self, schema: str, table: str, year: int, month: int) -> None:
        """
        Supprime les données mensuelles d'une table.

        :param schema: Le schema de la table.
        :param table: Le nom de la table.
        :param year: L'année des données à supprimer.
        :param month: Le mois des données à supprimer.
        :raises: HistorisationError si une erreur intervient de la suppression.
        """
        try:
            query = DatabaseModel.prepare_delete_month_query(schema, table, year, month)
            self.execute_query(query)
            logger_info.info(f"Données du mois {month}/{year} supprimées de la table {table}")
        except Exception as e:
            error_msg = f"Erreur lors de la suppression des données du mois : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    @log_execution_time
    def delete_yesterday_data(self, schema: str, table: str) -> None:
        """
        Supprime les données du jour précédent d'une table.
        
        :param schema: Le schema de la table.
        :param table: Le nom de la table.
        :raises: HistorisationError si une erreur intervient de la suppression.
        """
        yesterday = (datetime.now() - timedelta(days=1)).date()
        self.delete_day_data(schema, table, yesterday) 
    
class BaseTable:    
    def __init__(self, table_name: str, db_connection: DatabaseModel, historization_error: HistorisationError):
        """
        Initialise l'objet BaseTable permettant d'effectuer des opérations sur une table de base de données spécifique.
        :param table_name: Le nom de la table.
        :param db_connection: Instance de DatabaseModel permettant l'interaction dans la base de données.
        :param historization_error: Instance of HistorisationError permettant de gerer les erreurs.
        """
        self.schema = SCHEMA
        self.table_name = table_name
        self.db_connection = db_connection
        self.historization_error = historization_error
        
    def get_data(self) -> pd.DataFrame:
        """Récupère toutes les données de la table."""
        query = f"SELECT * FROM {self.schema}.{self.table_name}"
        return self.db_connection.read_sql_query(query)     
    
    def update(self, new_data: pd.DataFrame) -> None:
        """Méthode abstraite pour mettre à jour les données."""
        raise NotImplementedError("La sous-classe doit implémenter la méthode abstraite")    
        
# Controller Maille: Gère les opérations de base de données et orchestre le processus
class MailleController(BaseTable):
    def __init__(self, db_connection: DatabaseModel, historization_error: HistorisationError):
        """
        Initialise l'objet MailleController permettant d'effectuer des opérations sur la table des mailles.
        :param db_connection: Instance de DatabaseModel permettant l'interaction dans la base de données.
        :param historization_error: Instance of HistorisationError permettant de gerer les erreurs.
        """
        super().__init__(Tables.MAILLES, db_connection, historization_error)
        self.model = MailleModel()

    def update(self, new_data: pd.DataFrame) -> None:
        """
        Méthode pour mettre à jour les mailles avec de nouvelles données.
        Vérifie si les nouvelles données sont valides, les compare aux données existantes,
        et ajoute les nouvelles mailles si nécessaire.
        """
        logger_info.info(f"Nombre de nouvelles mailles à traiter: {len(new_data)}")
        if not isinstance(new_data, pd.DataFrame) or new_data.empty:
            logger_info.warning("Pas de nouvelle maille à ajouter")
            return
        try:
            logger_info.info("Mise à jour de mailles")
            with manage_dataframe(self.get_data()) as existing_mailles:
                with manage_dataframe(new_data.drop_duplicates(subset=['maille'])) as new_data:
                    new_mailles = new_data[~new_data['maille'].isin(existing_mailles['label'])]
                    if not new_mailles.empty:
                        self._insert_new_mailles(new_mailles, existing_mailles)
                    else:
                        logger_info.info("Pas de nouvelles mailles Ajoutées")
        except Exception as e:
            error_msg = f"Erreur lors de la mise à jour des données mailles: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    def _insert_new_mailles(self, new_mailles: pd.DataFrame, existing_mailles: pd.DataFrame) -> None:
        """
        Insère les nouvelles mailles dans la base de données.
        
        :param new_mailles: DataFrame des nouvelles mailles à ajouter.
        :param existing_mailles: DataFrame des mailles existantes dans la base de données.
        :raises: HistorisationError si une erreur intervient de l'insertion.
        """
        try:
            logger_info.info(f"Nombre de mailles existantes: {len(existing_mailles)}")
            logger_info.info(f"Nombre de nouvelles mailles à traiter: {len(new_mailles)}")
            new_mailles_to_insert = self.model.prepare_new_mailles(new_mailles, existing_mailles)            
            if new_mailles_to_insert is not None and len(new_mailles_to_insert) > 0:
                df_to_insert = pd.DataFrame(new_mailles_to_insert)
                self.db_connection.insert_dataframe(df_to_insert, self.table_name, self.schema)
                logger_info.info(f"Ajout de {len(new_mailles_to_insert)} mailles")
        except Exception as e:
            error_msg = f"Erreur d'insertion de mailles: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            
    
# Contrôleur : Gère les opérations de base de données et orchestre le processus
class CalcController(BaseTable):
    def __init__(self, db_connection: DatabaseModel, maille_controller: MailleController, rapport: List[str], historization_error: HistorisationError):
        """
        Initialise le contrôleur des calculs.
        
        :param db_connection: Objet de connection à la base de données.
        :param maille_controller: Contrôleur des mailles.
        :param rapport: Liste des rapports.
        :param historization_error: Objet d'erreur de l'historisation.
        """
        super().__init__(Tables.CALC, db_connection, historization_error)
        self.rapport = rapport
        self.maille_controller = maille_controller
        self.model = CalcModel()

    def update(self, new_calcs: pd.DataFrame) -> None:
        """
        Met à jour les calculs avec de nouvelles données.
        
        Vérifie si les nouvelles données sont valides, les compare aux données existantes,
        et ajoute les nouveaux calculs si nécessaire.
        
        :param new_calcs: DataFrame des nouveaux calculs à mettre à jour.
        :raises: HistorisationError si une erreur intervient de l'insertion.
        """
        if not isinstance(new_calcs, pd.DataFrame) or new_calcs.empty:
            logger_info.warning("Aucune nouvelle donnée à mettre à jour pour les calculs")
            return
        
        try:
            logger_info.info("Mise à jour des calculs")
            with manage_dataframe(self.get_data()) as existing_calcs:
                with manage_dataframe(new_calcs.drop_duplicates(subset=['indicateur'])) as new_calcs:
                    new_calcs_to_add = new_calcs[~new_calcs['indicateur'].isin(existing_calcs['label'])]
                    if not new_calcs_to_add.empty:
                        self._insert_new_calcs(new_calcs_to_add, existing_calcs)
                    else:
                        logger_info.info("Aucun nouveau calcul à ajouter")
        except Exception as e:
            error_msg = f"Erreur lors de la mise à jour des calculs : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            

    def _insert_new_calcs(self, new_calcs: pd.DataFrame, existing_calcs: pd.DataFrame) -> None:
        """
        Insertion des nouveaux calculs dans la base de données.
        
        :param new_calcs: DataFrame des nouveaux calculs à insérer.
        :param existing_calcs: DataFrame des calculs existants.
        :raises: HistorisationError si une erreur intervient de l'insertion.
        """
        try:
            maille_data = self.maille_controller.get_data()
            new_calcs_to_insert = self.model.prepare_new_calcs(new_calcs, existing_calcs, maille_data, self.rapport)
            if new_calcs_to_insert:
                df_to_insert = pd.DataFrame(new_calcs_to_insert)
                self.db_connection.insert_dataframe(df_to_insert, self.table_name, self.schema)
                logger_info.info(f"Ajout de {len(new_calcs_to_insert)} nouveaux calculs")
        except Exception as e:
            error_msg = f"Erreur lors de l'insertion du calc : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            
    
# Contrôleur : Gère les opérations de base de données et orchestre le processus
class JourController(BaseTable):
    def __init__(self, db_connection: DatabaseModel, rapport: List[str], date: date, historization_error: HistorisationError, maille_controller: MailleController, calc_controller: CalcController):
        """
        Initialise le contrôleur des jours.
        
        :param db_connection: Objet de connection à la base de données.
        :param rapport: Liste des rapports.
        :param date: Date de l'historisation.
        :param historization_error: Erreur de l'historisation.
        :param maille_controller: Contrôleur des mailles.
        :param calc_controller: Contrôleur des calculs.
        """
        super().__init__(Tables.JOURS, db_connection, historization_error)
        self.rapport = rapport
        self.maille_controller = maille_controller
        self.calc_controller = calc_controller
        self.date = date
        self.model = JourModel()

    def update(self, new_data: pd.DataFrame) -> None:
        
        """
        Mettez à jour les données quotidiennes avec de nouvelles informations.

        Cette méthode vérifie si les nouvelles données fournies sont valides et non vides,
        le compare avec les données de calcul et de messagerie existantes, et insère le
        de nouvelles données dans la base de données si nécessaire. Il gère les erreurs liées à
        préparation et insertion des données.

        :param new_data: DataFrame contenant les nouvelles données à mettre à jour.
        :raises: HistorisationError si une erreur se produit lors de la préparation des données ou l’insertion.
        """
        if not isinstance(new_data, pd.DataFrame) or new_data.empty:
            logger_info.warning("Aucune nouvelle donnée à mettre à jour pour les jours")
            return
        try:
            logger_info.info("Mise à jour des données journalières")       
            with manage_dataframe(self.calc_controller.get_data()) as calc_data:
                with manage_dataframe(self.maille_controller.get_data()) as maille_data:
                    with manage_dataframe(new_data) as new_data:
                        try:                            
                            to_insert = self.model.prepare_data_for_insertion(new_data, calc_data, maille_data, self.date)                           
                            if not to_insert.empty:
                                self.db_connection.insert_dataframe(to_insert, self.table_name, self.schema)
                                logger_info.info(f"Insertion de {len(to_insert)} nouvelles lignes dans la table des jours")
                            else:
                                logger_info.warning("Aucune nouvelle donnée valide à insérer dans la table des jours")
                        except KeyError as e:
                            error_msg = f"Erreur de clé lors de la préparation des données : {str(e)} Colonnes disponibles : {new_data.columns.tolist()}"
                            self.historization_error.add_error(error_msg)
                             
                        except Exception as e:
                            error_msg = f"Erreur de clé lors de la préparation des données : {str(e)} Colonnes disponibles : {new_data.columns.tolist()}"
                            self.historization_error.add_error(error_msg)
                             
        except Exception as e:
            error_msg = f"Erreur lors de la mise à jour des données journalières : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
        

class MoisController(BaseTable):
    def __init__(self, db_connection: DatabaseModel, rapport_dir: Path, historization_error: HistorisationError):
        """
        Initialisation du contrôleur des mois.

        :param db_connection: La connexion à la base de données.
        :param rapport_dir: Le répertoire contenant les rapports.
        :param historization_error: L'objet qui gère les erreurs de l'historisation.
        :raises: HistorisationError si une erreur se produit lors de l'initialisation.
        """
        super().__init__(Tables.MOIS, db_connection, historization_error)
        self.rapport_dir = rapport_dir
        self.model = MoisModel()

    def update(self) -> None:
        """
        Met à jour les données mensuelles.

        Cette méthode lit le fichier SQL du répertoire du rapport et exécute la requête pour récupérer les données mensuelles.
        Si le fichier SQL n'existe pas, la méthode crée une requête SQL pour extraire les données mensuelles.
        Les données sont ensuite insérées dans la base de données si elle ne sont pas déjà présentes.

        :raises: HistorisationError si une erreur se produit lors de l'exécution de la requête ou de l'insertion des données.
        """
        try:
            sql_file = os.path.join(self.rapport_dir, SQL_MOIS)
            if os.path.exists(sql_file):
                df_mois = self.db_connection.read_sql_query_file(sql_file)
            else:
                rapport = [os.path.basename(self.rapport_dir).upper()]
                query = self.model.prepare_monthly_query(self.schema, rapport)
                df_mois = self.db_connection.read_sql_query(query)

            if not df_mois.empty:
                self.db_connection.insert_dataframe(df_mois, self.table_name, self.schema, if_exists='append')
                logger_info.info(f"Added {len(df_mois)} monthly records")
            else:
                logger_info.warning("Pas de données mensuelles à historiser")
        except Exception as e:
            error_msg = f"Erreur lors de l'histiorisation des données mensuelles: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
               
            
# Controller
class RapportController:
    def __init__(self, rapport_dir: Path, db_connection: DatabaseModel, historization_error: HistorisationError, flag_manager: Optional[FlagManager] = None, date: Optional[date] = None):
        """
        Initialise le contrôleur de rapport.

        :param rapport_dir: chemin du répertoire du rapport
        :param db_connection: objet de connexion à la base de données
        :param historization_error: objet en charge de la gestion des erreurs de l'historisation
        :param flag_manager: objet en charge de la gestion des flags (facultatif)
        :param date: date de l'historisation (facultatif, par défaut la date du jour)
        """
        self.historization_error = historization_error
        self.db_connection = db_connection
        self.date = date or datetime.now().date()
        self.rapport_dir = Path(rapport_dir)
        self.rapport = [self.rapport_dir.name.upper()]
        self.maille_controller = MailleController(self.db_connection, self.historization_error)
        self.calc_controller = CalcController(self.db_connection, self.maille_controller, self.rapport, self.historization_error)
        self.jour_controller = JourController(self.db_connection, self.rapport, self.date, self.historization_error, self.maille_controller, self.calc_controller)
        self.mois_controller = MoisController(self.db_connection, rapport_dir, self.historization_error)
        self.flag_manager = flag_manager
        self.exitcode = 0
        self.sql_processor = SQLFileProcessor(self.db_connection, self.historization_error)
        self.model = RapportModel()

    def process_sql_files(self) -> int:
        """
        Exécute les fichiers SQL journaliers et mensuels liés au rapport.
        
        :return: code de retour de la méthode
        """
        try:
            self._process_daily_files()
            self._process_monthly_file()
            return self.exitcode
        except Exception as e:
            self._handle_error(f"Erreur lors du traitement processus des fichiers SQL : {str(e)}")
            return self.exitcode

    def _process_daily_files(self):
        """
        Exécute les fichiers SQL journaliers liés au rapport.
        
        :return: None
        """
        for sql_file in self.rapport_dir.glob('*.sql'):
            if SQL_MOIS not in sql_file.name:
                self._process_single_file(sql_file)

    def _process_monthly_file(self):
        """
        Traitement du fichier mensuel en vérifiant s’il existe et en mettant à jour les données mensuelles si nécessaire.
        
        :return: None
        """
        monthly_file = self.rapport_dir / SQL_MOIS
        if monthly_file.exists():
            self._process_single_file(monthly_file, is_monthly=True)
        elif not self.flag_manager.manage_flag(FLAG_FILE_MONTHLY, FlagAction.CHECK, FlagType.MONTHLY):
            self._update_monthly_data()

    def _process_single_file(self, sql_file: Path, is_monthly: bool = False):
        """
        Exécute un fichier SQL journalier ou mensuel lié au rapport.
        
        :param sql_file: Chemin du fichier SQL
        :param is_monthly: Si le fichier est mensuel, par défaut False
        :return: None
        """
        if not self._should_process_file(sql_file, is_monthly):
            return
        try:
            # Créez l'instance de SQLFileProcessor ici, en passant les deux arguments requis
            sql_processor = SQLFileProcessor(self.db_connection, self.historization_error)
            data = sql_processor.process_file(sql_file)            
            if data is not None and not data.empty:
                prepared_data = self.model.prepare_data(data, is_monthly)
                self._update_data(prepared_data, is_monthly)                
                self.flag_manager.manage_flag(sql_file, FlagAction.CREATE, FlagType.MONTHLY if is_monthly else FlagType.DAILY)            
        except Exception as e:
            self._handle_error(f"Erreur lors du traitement du fichier {sql_file}: {str(e)}")

    def _should_process_file(self, sql_file: Path, is_monthly: bool) -> bool:
        """
        Vérifie si un fichier SQL journalier ou mensuel a déjà été traité.
        
        :param sql_file: Chemin du fichier SQL
        :param is_monthly: Si le fichier est mensuel, par défaut False
        :return: True si le fichier n'a pas encore été traité, False sinon
        """       
        flag_type = FlagType.MONTHLY if is_monthly else FlagType.DAILY
        if self.flag_manager.manage_flag(sql_file, FlagAction.CHECK, flag_type):
            logger_info.warning(f"Le fichier {sql_file.name} a déjà été traité.")
            return False
        return True        

    def _update_data(self, data: pd.DataFrame, is_monthly: bool):
        """
        Met à jour les données journalières ou mensuelles.
        
        :param data: DataFrame contenant les données à mettre à jour
        :param is_monthly: Si les données sont mensuelles, par défaut False
        :return: None
        """
        if is_monthly:
            self.mois_controller.update()
        else:
            self.maille_controller.update(data[['maille', 'maille_parent']])
            self.calc_controller.update(data[['indicateur', 'indicateur_parent', 'maille_parent']])
            self.jour_controller.update(data)
            
    def _update_monthly_data(self):
        """
        Met à jour les données mensuelles en appelant la méthode update du controlleur des données mensuelles
        et en créant un flag pour indiquer que les données mensuelles ont été mises à jour.
        
        :return: None
        """
        self.mois_controller.update()
        self.flag_manager.manage_flag(FLAG_FILE_MONTHLY, FlagAction.CREATE, FlagType.MONTHLY)

    def _handle_error(self, error_msg: str):
        """
        Gère une erreur qui se produit lors du traitement des fichiers SQL journaliers/mensuels.
        
        :param error_msg: Message d'erreur à logger et à stocker dans l'objet d'erreur
        :return: Rien, mais relève une exception
        """
        logger_info.error(error_msg)
        self.historization_error.add_error(error_msg)
        
        self.exitcode += 1

# Controller Traitement : Gère le processus de traitement global
class TraitementController:
    def __init__(self, db_connection: DatabaseModel, historization_error: HistorisationError, flag_manager: FlagManager):
        """
        Initialise le contrôleur de traitement.
        
        :param db_connection: objet de connexion à la base de données
        :param historization_error: objet en charge de la gestion des erreurs de l'historisation
        :param flag_manager: objet en charge de la gestion des flags
        """
        self.db_connection = db_connection
        self.historization_error = historization_error
        self.flag_manager = flag_manager
        self.model = TraitementModel()

    def process_action(self, action: str, date: Optional[date] = None, rapport_dir: Optional[Path] = None) -> int:
        """
        Effectue le traitement demandé.
        
        :param action: Action demandée, peut prendre les valeurs 'default', 'jour', 'mois' ou 'rapport'
        :param date: Date du traitement, optionnel mais obligatoire pour les traitements 'jour' et 'default'
        :param rapport_dir: Chemin du répertoire du rapport, optionnel mais obligatoire pour le traitement 'rapport'
        :return: Code de retour de la méthode
        """
       
        try:
            logger_info.info(f"Process_data, action souhaitée : {action}")
            
            is_monthly = action == 'mois'
            self._clean_old_data(is_monthly)
            
            if action == 'default':
                return self._process_default(date)
            elif action == 'jour':
                return self._process_jour(date)
            elif action == 'mois':
                return self._process_mois()
            elif action == 'rapport' and rapport_dir:
                return self._process_rapport(rapport_dir)
            else:
                error_msg = f"Action non reconnue : {action}"
                raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Erreur inattendue lors du traitement {action} : {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            return 1

    def _clean_old_data(self, is_monthly: bool):
        """
        Supprime les données anciennes de la table JOURS ou MOIS en fonction de la fréquence (journalière ou mensuelle)
        en fonction de la valeur de retention_months.
        
        :param is_monthly: Booléen indiquant si les données sont mensuelles ou non
        :return: Rien
        """
        retention_months = config['data_cleanup']['monthly_retention_months' if is_monthly else 'daily_retention_months']
        self.model.clean_old_data(self.db_connection, is_monthly, retention_months)

    def _process_default(self, date: Optional[date] = None) -> int:
        """
        Effectue le traitement 'default', qui consiste à lancer la méthode process_sql_files() pour chaque répertoire
        de rapports SQL trouvés dans le dossier SQLPATH.

        :param date: Date du traitement, optionnel mais obligatoire si la date du jour n est pas
                     dans le fichier de flag JOUR.flag
        :return: Code de retour de la méthode
        """
        logger_info.info("traitement par default")
        if not self.flag_manager.manage_flag(FLAG_FILE_DAILY, FlagAction.CHECK) or date:
            exitcode = 0
            for rapport_dir in SQLPATH.glob('*'):
                if rapport_dir.is_dir():
                    rapport_controller = RapportController(Path(rapport_dir), self.db_connection, self.historization_error, self.flag_manager, date)
                    exitcode += rapport_controller.process_sql_files()
            if not date:
                self.flag_manager.manage_flag(FLAG_FILE_DAILY, FlagAction.CREATE)
            return exitcode
        return 0

    def _process_jour(self, date: Optional[date] = None) -> int:
        """
        Supprime les données de la journée passée en paramètre (ou du jour actuel si date=None)
        dans la table JOURS, puis relance la méthode _process_default pour recalculer les données
        journalières. Supprime le fichier JOUR.flag.
        
        :param date: Date du traitement, optionnel mais obligatoire si la date du jour n est pas
                     dans le fichier de flag JOUR.flag
        :return: Code de retour de la méthode
        """
        date = date if date else datetime.now().date()
        self.db_connection.delete_day_data(SCHEMA, Tables.JOURS, date)
        self.flag_manager.manage_flag(FLAG_FILE_DAILY, FlagAction.REMOVE)
        return self._process_default(date)

    def _process_mois(self) -> int:
        """
        Effectue le traitement des données mensuelles.
        
        Supprime les données du mois précédent, prépare les données mensuelles, et gère les flags associés.
        
        :return: Le code de retour de l'opération
        """
        last_month = self.model.get_last_month()
        self.db_connection.delete_month_data(SCHEMA, Tables.MOIS, last_month.year, last_month.month)
        self.flag_manager.manage_flag(FLAG_FILE_MONTHLY, FlagAction.REMOVE)
        self._clean_old_data(True)
        exitcode = self.model.prepare_monthly_data(self.db_connection, last_month, list(SQLPATH.glob('*')))
        self.flag_manager.manage_flag(FLAG_FILE_MONTHLY, FlagAction.CREATE, FlagType.MONTHLY)
        return exitcode

    def _process_rapport(self, rapport_dir: Path) -> int:
        """
        Exécute le traitement pour un répertoire de rapports passé en paramètre.
        
        :param rapport_dir: Chemin du répertoire de rapports
        :return: Code de retour de l'opération
        """
        rapport_controller = RapportController(rapport_dir, self.db_connection, self.historization_error, self.flag_manager)
        return rapport_controller.process_sql_files()
        
        