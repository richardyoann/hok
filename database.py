#!/usr/bin/env python
# coding: utf-8
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Union
import sqlalchemy
import pandas as pd
from model import DatabaseModel
from errors import HistorisationError
from utils import log_execution_time
from logger import get_logger
logger_info= get_logger()

class DatabaseController:
    def __init__(self, user: str, password: str, host: str, port: int, bdd: str, historization_error: HistorisationError):
        """
        Initialise l'objet DatabaseController .

        Cette object permet d'interagir avec la base de données postgres.

        Parameters
        ----------
        user : str 
            Username de la base de données.
        password : str
            Password du compte de la base de données.
        host : str
            Hostname ou address IP du serveur de la base de données.
        port : int
            Port de la base de données.
        bdd : str
            Nom de la de la base de données 
        historization_error : HistorisationError
            Objet qui contient des méthodes pour gérer les erreurs qui peuvent se produire pendant le processus d’historisation.

        Returns
        -------
        None
        """
        self.historization_error = historization_error
        self.connection_string = DatabaseModel.prepare_connection_string(user, password, host, port, bdd)
        self.engine = None
        self.connection = None
        self._initialize_engine()

    def _initialize_engine(self):
        """
        Initialise l'engine de connection à la base de données postgres.
        
        Essaie de créer l'engine de connection avec la chaine de connection enregistrée.
        Si cela échoue, enregistre l'erreur dans l'objet historization_error.
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
        Etablit la connexion de la base de données de données PostgreSQL. 

        Si la connexion n’est pas établie ou est fermée, cette méthode tente d’ouvrir une nouvelle connexion.
        En cas d’échec, l’erreur est consignée et ajoutée à l’objet historization_error.

        Returns
        -------
        sqlalchemy.engine.base.Connection Connexion à la base de données.
        
        Raises
        ------
        Exception
            En cas d’erreur lors de la tentative de connexion.
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
        Ferme la connexion à la base de données PostgreSQL si elle est active.

        Essaie de fermer la connexion si elle est active, puis de fermer l'engine.
        Si cela échoue, l'erreur est consignée et ajoutée à l'objet historization_error.

        Raises
        ------
        Exception
            En cas d'erreur lors de la tentative de fermeture de la connexion.
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
        Lit une requête SQL et la stocke dans un DataFrame.
        
        Lit une requête SQL en base de données et la stocke dans un DataFrame.
        Si cela échoue, l'erreur est consignée et ajoutée à l'objet historization_error.
        
        Parameters
        ----------
        sql : str
            requête SQL à exécuter.
        
        Returns
        -------
        pd.DataFrame
            DataFrame contenant les données issues de la requête.
        
        Raises
        ------
        Exception
            En cas d'erreur lors de la tentative d'exécution de la requête.
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
        Exécute une requête SQL en écriture.
        
        Exécute une requête SQL en base de données. Si cela échoue, l'erreur est consignée et ajoutée à l'objet historization_error.
        
        Parameters
        ----------
        sql : str
            requête SQL à exécuter.
        
        Returns
        -------
        object
            Résultat de l'exécution de la requête.
        
        Raises
        ------
        Exception
            En cas d'erreur lors de la tentative d'exécution de la requête.
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
        Lit un fichier .sql et execute la requête SQL en base de données.
        
        Lit un fichier .sql, execute la requête SQL en base de données et stocke le résultat
        dans un DataFrame. Si cela échoue, l'erreur est consignée et ajoutée à l'objet historization_error.
        
        Parameters
        ----------
        sqlfilename : Path
            Chemin du fichier .sql à exécuter.
        
        Returns
        -------
        pd.DataFrame
            DataFrame contenant les données issues de la requête.
        
        Raises
        ------
        Exception
            En cas d'erreur lors de la tentative d'exécution de la requête.
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
        Insert un DataFrame dans une table de la base de données.
        
        Insert les données du DataFrame df_out dans la table table du schéma schema.
        Si la table n'existe pas, elle est créée. Si elle existe, les données sont ajoutées.
        
        Parameters
        ----------
        df_out : pd.DataFrame
            DataFrame contenant les données à insérer.
        table : str
            Nom de la table cible.
        schema : str
            Schéma dans lequel se trouve la table cible.
        if_exists : str, optional
            Comportement si la table existe déjà. Par défaut 'append', les données sont ajoutées.
            Les autres valeurs possibles sont 'replace' (supprime la table et la recrée) et 'fail'
            (lève une erreur).

        Raises
        ------
        Exception
            En cas d'erreur lors de la tentative d'insertion.
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
        Supprime les données d'une journée spécifique dans la base de données.

        Parameters
        ----------
        schema : str
            Le nom du schéma où se trouve la table.
        table : str
            Le nom de la table à partir de laquelle les données doivent être supprimées.
        date : Union[str, datetime, date]
            La date spécifique des données à supprimer.

        Returns
        -------
        None

        Raises
        ------
        Exception
            En cas d'erreur lors de la suppression des données.
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
        Supprime les données d'un mois spécifique dans la base de données.

        Parameters
        ----------
        schema : str
            Le nom du schéma où se trouve la table.
        table : str
            Le nom de la table à partir de laquelle les données doivent être supprimées.
        year : int
            L'année du mois à supprimer.
        month : int
            Le mois à supprimer.

        Returns
        -------
        None

        Raises
        ------
        Exception
            En cas d'erreur lors de la suppression des données.
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
        Supprime les données de la veille dans la base de données.

        Parameters
        ----------
        schema : str
            Le nom du schéma où se trouve la table.
        table : str
            Le nom de la table à partir de laquelle les données doivent être supprimées.

        Returns
        -------
        None

        Raises
        ------
        Exception
            En cas d'erreur lors de la suppression des données.
        """
        yesterday = (datetime.now() - timedelta(days=1)).date()
        self.delete_day_data(schema, table, yesterday) 