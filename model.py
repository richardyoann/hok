#!/usr/bin/env python
# coding: utf-8
from pathlib import Path
import pandas as pd
from typing import List, Dict, Union, Optional
from datetime import date, datetime, timedelta
from config import config,SQL_FORMAT_LABELS,SCHEMA
from errors import HistorisationError
from logger import get_logger
logger_info= get_logger()

# Model bdd :  Contient la logique métier de la gestion des bdd
class DatabaseModel:
    @staticmethod
    def prepare_connection_string(user: str, password: str, host: str, port: int, bdd: str) -> str:
        """
        Prepare une chaine de connection PostgreSQL
        :param user: nom utilisateur
        :param password: mot de passe
        :param host: nom de l'hôte
        :param port: port
        :param bdd: nom de la base de données
        :return: chaine de connection
        """
        return f'postgresql://{user}:{password}@{host}:{port}/{bdd}'

    @staticmethod
    def prepare_delete_day_query(schema: str, table: str, date: Union[str, datetime, date]) -> str:
        """
        Prepare une requête DELETE PostgreSQL pour supprimer les données d'une journée
        :param schema: nom du schéma
        :param table: nom de la table
        :param date: date de la journée à supprimer
        :return: requête DELETE
        """
        return f"DELETE FROM {schema}.{table} WHERE date = '{date}'"

    @staticmethod
    def prepare_delete_month_query(schema: str, table: str, year: int, month: int) -> str:
        """
        Prepare la requête permettant de supprimer les données mensuelles historisées en fonction du schéma et la table et de la temporalité renseignée.
        :param schema: nom du schema
        :param table: nom de la table
        :param year: L'année de suppression
        :param month: le mois de suppression
        :return: Requête DELETE         
        """   
        return f"DELETE FROM {schema}.{table} WHERE EXTRACT(YEAR FROM date) = {year} AND EXTRACT(MONTH FROM date) = {month}"
    
# Centralisation des noms de tables
class Tables:
    CALC : str = config['connexion']['tables']['calc']
    JOURS : str = config['connexion']['tables']['jours']
    MOIS : str = config['connexion']['tables']['mois']
    MAILLES : str = config['connexion']['tables']['mailles']        

# Model Maille : Contient la logique métier de la gestion des Mailles
class MailleModel:
    @staticmethod
    def prepare_new_mailles(new_data: pd.DataFrame, existing_mailles: pd.DataFrame) -> List[Dict[str, Union[int, str]]]:
        """
        Prepare une liste de dictionnaires représentant de nouveaux mailles à insérer dans la base de données à partir du DataFrame passé en parametre new_data, du DataFrame existing_mailles passé en parametre dans la base de données
        
        :param new_data: DataFrame  new data
        :param existing_mailles: DataFrame  existing_mailles 
        :return: Liste de DataFrame representant les nouvelles mailles à inserer dans la bdd
        """
            # Validation des colonnes attendues
        required_columns = ['maille', 'maille_parent']
        if not all(col in new_data.columns for col in required_columns):
            raise ValueError(f"Le DataFrame new_data doit contenir les colonnes {required_columns}")

        required_existing_columns = ['id_maille', 'label']
        if not all(col in existing_mailles.columns for col in required_existing_columns):
            raise ValueError(f"Le DataFrame existing_mailles doit contenir les colonnes {required_existing_columns}")
        
        max_id = existing_mailles['id_maille'].max() if not existing_mailles.empty else 0
        label_to_id = dict(zip(existing_mailles['label'], existing_mailles['id_maille']))
        new_mailles_to_insert = []

        def prepare_maille(label: str, parent_label: Optional[str]) -> None:       
            """
            Prepare la maille pour insertion en bdd.

            :param label: Label de la maille
            :param parent_label: Label parent de la maille, optional
            :return: None
            """
            nonlocal max_id
            if label in label_to_id:
                return None  # La maille existe déjà
            max_id += 1
            parent_id = label_to_id.get(parent_label, 0)
            new_maille = {
                'id_maille': max_id,
                'label': label,
                'id_parent': parent_id
            }            
            label_to_id[label] = max_id
            return new_maille

        # Préparation des mailles parentes
        for _, row in new_data.iterrows():
            parent_label = row['maille_parent']
            if parent_label and (parent_label not in label_to_id):
                new_maille = prepare_maille(parent_label, None)   
                if new_maille:
                    new_mailles_to_insert.append(new_maille)             

        # Préparation des mailles enfants
        for _, row in new_data.iterrows():
            label = row['maille']
            parent_label = row['maille_parent']
            new_maille = prepare_maille(label, parent_label)
            if new_maille is not None:
                new_mailles_to_insert.append(new_maille)

        return new_mailles_to_insert


# Modèle Calc: Contient la logique métier de la gestion des calc
class CalcModel:
    @staticmethod
    def prepare_new_calcs(new_calcs: pd.DataFrame, existing_calcs: pd.DataFrame, maille_data: pd.DataFrame, rapport: List[str]) -> List[Dict[str, Union[int, str, List[str]]]]:
        """
        Préparer une liste de DataFrame représentant les nouveaux calculs à insérer dans la base de données
        à partir d’un DataFrame donné de nouvelles données, d’un DataFrame existant de calcs dans la base de données,
        un DataFrame de mailles et une liste de rapports.

        :param new_calcs: DataFrame new data
        :param existing_calcs: DataFrame permettant de lister les indicateurs existants dans la base de données
        :param maille_data: DataFrame des mailles de la base de données
        :param rapport: Reference du rapport
        :return: DataFram des nouveaux calc à inserer dans la bdd
        """
        max_id = existing_calcs['id_calc'].max() if not existing_calcs.empty else 0
        calc_label_to_id = dict(zip(existing_calcs['label'], existing_calcs['id_calc']))
        maille_label_to_id = dict(zip(maille_data['label'], maille_data['id_maille']))
        new_calcs_to_insert = []

        def prepare_calc(indicateur: str, parent: Optional[str], maille: str) -> Dict[str, Union[int, str, List[str]]]:
            """
            Prepare un DataFrame representant les nouvelles calc à inserer dans la bdd 
            à partir du label de l'indicateur, l'indicateur parent et le label de la maille             

            :param indicateur: label du calc
            :param parent: label du parent calc
            :param maille: label du maille
            :return: DataFrame des nouveaux calc à inserer dans la bdd
            """
            nonlocal max_id
            if indicateur in calc_label_to_id:
                return None
            max_id += 1
            parent_id = calc_label_to_id.get(parent, 0)
            id_maille_groupe = maille_label_to_id.get(maille, 0)
            new_calc = {
                'id_calc': max_id,
                'label': indicateur,
                'id_parent': parent_id,
                'id_maille_groupe': id_maille_groupe,
                'rapports': rapport
            }
            calc_label_to_id[indicateur] = max_id
            return new_calc

        # D'abord, préparer tous les calculs parents
        for _, row in new_calcs.iterrows():
            parent_label = row['indicateur_parent']
            if parent_label and parent_label not in calc_label_to_id:
                new_calc = prepare_calc(parent_label, None, row['maille_parent'])
                if new_calc:
                    new_calcs_to_insert.append(new_calc)

        # Ensuite, préparer les calculs enfants
        for _, row in new_calcs.iterrows():
            label = row['indicateur']
            parent_label = row['indicateur_parent']
            maille_label = row['maille_parent']
            new_calc = prepare_calc(label, parent_label, maille_label)
        if new_calc:
            new_calcs_to_insert.append(new_calc)

        return new_calcs_to_insert
    
# Modèle Jours: Contient la logique métier de l'historisation journaliere
class JourModel:
    @staticmethod
    def prepare_data_for_insertion(new_data: pd.DataFrame, calc_data: pd.DataFrame, maille_data: pd.DataFrame, date: date) -> pd.DataFrame:
        """
        Préparez un DataFrame de nouvelles données à insérer dans la base de données à partir new_data, du DataFrame des calcs provenant de la base de données, du DataFrame des mailles provenant de la base de données et de la date.

        :param new_data: DataFrame of new data
        :param calc_data: DataFrame of calcs de la bdd
        :param maille_data: DataFrame of mailles de la bdd
        :param date: date de l'historisation demandée
        :return: DataFrame pert à être inserer dans la bdd
        """
        calc_data_unique = calc_data.drop_duplicates(subset='label', keep='first')
        maille_data_unique = maille_data.drop_duplicates(subset='label', keep='first')
        
        new_data = new_data.merge(calc_data_unique[['label', 'id_calc']], 
                                  left_on='indicateur', 
                                  right_on='label', 
                                  how='left')
        new_data = new_data.merge(maille_data_unique[['label', 'id_maille']], 
                                  left_on='maille', 
                                  right_on='label', 
                                  how='left')
        new_data['date'] = date
        to_insert = new_data[['id_calc', 'id_maille', 'date', 'valeur']].dropna()
        return to_insert
    
# Modèle Mois : Contient la logique métier de l'historisation mensuelle
class MoisModel:
    @staticmethod
    def prepare_monthly_query(schema: str, rapport: List[str]) -> str:
        """
        Préparation d’une requête SQL pour l’extraction des données mensuelles de la base de données

        :param schema: Le schema de la table
        :param rapport: La référence du rapport
        :return: Script SQL pour l'historisation des données mensuelles
        """
        return f"""
        SELECT id_calc, id_maille, 
               date_trunc('month', (current_date - interval '1 month'))::date as "date", 
               avg(valeur) as valeur 
        FROM {schema}.{Tables.JOURS} j 
        JOIN {schema}.{Tables.CALC} c USING(id_calc) 
        WHERE extract(month from "date") = extract(month from current_date - interval '1 month') 
          AND c.rapports @> array[{rapport}]
        GROUP BY id_calc, id_maille, 
                 date_trunc('month', (current_date - interval '1 month'))::date
        """
    
# Model Rapport : Contient la logique métier de l'historisation mensuelle
class RapportModel:
    @staticmethod
    def prepare_data(data: pd.DataFrame, is_monthly: bool) -> pd.DataFrame:       
        """
        Prepare les data pour l'historisation

        :param data: Le DataFrame avec les données à historiser
        :param is_monthly: A boolean permettant d'indiquer la temporalité de l'historisation mensuelle ou non
        :return: Le DataFrame a historiser
        """
       
        if is_monthly:
            # Préparation des données mensuelles
            return data
        else:
            # Préparation des données journalières
            missing_columns = set(SQL_FORMAT_LABELS) - set(data.columns)
            if missing_columns:
                error_msg = f"Colonnes manquantes dans le fichier sql: {', '.join(missing_columns)}"
                logger_info.error(error_msg)      
            else :     
                return data[['maille', 'maille_parent', 'indicateur', 'indicateur_parent', 'maille_parent','valeur']]
        
# Model Traitement : Contient la logique métier du traitement
class TraitementModel:
    @staticmethod
    def clean_old_data(db_connection: DatabaseModel, is_monthly: bool, retention_months: int):
        """
        Supprime les données anciennes de la table JOURS ou MOIS en fonction de la fréquence (journalière ou mensuelle)
        en fonction de la valeur de retention_months.
        :param db_connection: Objet de connection à la base de données
        :param is_monthly: Booléen indiquant si les données sont mensuelles ou non
        :param retention_months: Nombre de mois pour lequel les données doivent être conservées
        :return: Rien, mais renvoie l'objet de connexion à la base de données
        """
        schema = SCHEMA
        table = Tables.MOIS if is_monthly else Tables.JOURS
        interval = f"{retention_months} months"
        query = f"""
        DELETE FROM {schema}.{table}
        WHERE date < CURRENT_DATE - INTERVAL '{interval}'
        """
        return db_connection.execute_query(query)

    @staticmethod
    def get_last_month():
        """
        Renvoie le premier jour du mois précédent.

        :return: Un objet date du premier jour du mois précédent
        """
        return (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)

    @staticmethod
    def prepare_monthly_data(db_connection: DatabaseModel, last_month: date, rapport_dirs: List[Path]):
        """
        Prépare les données mensuelles pour l'historisation.

        Cette méthode prend en charge la préparation des données mensuelles en parcourant les répertoires de rapports fournis.
        Pour chaque répertoire de rapport, elle crée un contrôleur Mois et exécute la mise à jour, accumulant les codes de sortie.
        
        :param db_connection: Objet de connexion à la base de données.
        :param last_month: Date du mois précédent.
        :param rapport_dirs: Liste des chemins des répertoires de rapports.
        :return: Code de sortie cumulé après la mise à jour des données mensuelles.
        """
        from controllers import MoisController  # Import local pour éviter les imports circulaires
        exitcode = 0
        for rapport_dir in rapport_dirs:
            mois_controller = MoisController(db_connection, str(rapport_dir), HistorisationError())
            exitcode += mois_controller.update()
        return exitcode