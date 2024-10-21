#!/usr/bin/env python
# coding: utf-8

# In[3]:


"""
@desciption: historisation des indicateurs mensuels
@author: Filiere Data ITSUP
@doc   : https://si-confluence.edf.fr/pages/viewpage.action?pageId=1402635113
""" ; None

import pandas as pd
import time
from datetime import datetime, timedelta, date
import sqlalchemy
import logging.handlers
import re
import os
import sys
import pathlib
import argparse
from typing import List, Optional, Dict, Union, Tuple
from IPython import get_ipython
from pathlib import Path

########################################
# Constantes d'environnement à adapter
########################################

LOGLEVEL: int = logging.INFO

# Chemins des fichiers et logs
SQLPATH: Path = Path('./sql')
LOGPATH: Path = Path('./log')
LOGFILE: Path = Path('hok.log')
    
# Chemin odbc.ini
ODBC_INI: Path = Path('../.odbc.ini')  

# Chaînes de connexion ODBC
ODBC_STRING_R: str = "DTA_lecture"
ODBC_STRING_W: str = "DTA_lecture"

# Structure des tables de la BDD prod
SCHEMA: str = 'use_case_dev'

SQL_MOIS: str = 'indic_mois_kpi.sql'

# Colonnes attendues dans le résultat des requêtes SQL
SQL_FORMAT_LABELS: List[str] = ['indicateur', 'indicateur_parent', 'maille', 'maille_parent', 'valeur']

# Fichiers flag pour ne pas recalculer les données journalières et mensuelles
FLAG_FILE_DAILY: Path = Path('./JOUR.flag')
FLAG_FILE_MONTHLY: Path = Path('./MOIS.flag')

########################################
# Configuration du logger
########################################

logging.basicConfig()
formatter_info = logging.Formatter("%(asctime)s -- %(name)s -- %(process)s -- %(processName)s -- %(levelname)s -- %(module)s -- %(funcName)s -- %(message)s")

logger_info = logging.getLogger("hok")
fh = logging.handlers.TimedRotatingFileHandler(LOGPATH / LOGFILE, 'D', 1, 7)

logger_info.setLevel(LOGLEVEL)
fh.setFormatter(formatter_info)
logger_info.addHandler(fh)

########################################
# Décorateur 
########################################
def log_execution_time(func):
    """Récupère le temps d'execution d'un traitement."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger_info.info(f"Exécution de {func.__name__} terminée en {execution_time:.2f} secondes")
        return result
    return wrapper

########################################
# Classes
########################################
class Connection_DB:
    def __init__(self, user: Optional[str] = None, password: Optional[str] = None, 
                 host: Optional[str] = None, port: Optional[int] = None, bdd: Optional[str] = None):
        try:           
            # Utilisez les arguments s'ils sont fournis, sinon utilisez les valeurs par défaut           
            self.user =  user 
            self.password =  password
            self.host =  host 
            self.port =  port 
            self.bdd =  bdd   
        
            self.connection_string = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.bdd}'    
            self.engine = sqlalchemy.create_engine(self.connection_string)
            self.connection : Optional[sqlalchemy.engine.base.Connection] = None
            logger_info.info(f"Initialisation réussie de Connection_DB")
        except Exception as e:
            logger_info.error(f"Erreur lors de la connexion : {str(e)}")           
            raise   

    def retrieve_credentials(self, odbc_string: str) -> Dict[str, str]:
        """Récupère les identifiants de connexion depuis le fichier ODBC."""
        try:
            with open(ODBC_INI, 'r') as f:
                content = f.read()
            
            section = f'[{odbc_string}]' if not odbc_string.startswith('[') else odbc_string
            pattern = fr'{section}(.*?)(?=\[|$)'
            match = re.search(pattern, content, re.DOTALL)
            
            if not match:
                raise ValueError(f"Section {odbc_string} non trouvée dans {ODBC_INI}")
            
            section_content = match.group(1)
            credentials: Dict[str, str] = {}
            for line in section_content.splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    credentials[key.strip().lower()] = value.strip()
            
            return credentials
        
        except Exception as e:
            logger_info.error(f"Erreur lors de la récupération des identifiants : {str(e)}")
            raise

    def connect(self)-> sqlalchemy.engine.base.Connection:
        """Établit une connexion à la base de données."""
        try:
            if not self.connection or self.connection.closed:
                self.connection = self.engine.connect()
            return self.connection
        except Exception as e:
            logger_info.error(f"Erreur lors de la connexion avec la bdd : {str(e)}")
            raise
    
    def disconnect(self)-> None:
        """Ferme la connexion à la base de données."""
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            self.engine.dispose()
            logger_info.info(f"Connexion fermée avec succès")
        except Exception as e:
            logger_info.error(f"Erreur lors de la connexion avec la bdd : {str(e)}")
            raise   

class DatabaseAccess:
    """Classe pour accéder et manipuler la base de données."""
    def __init__(self, user: str, password: str, host: str, port: int, bdd: str):  
        self.read = Connection_DB( user, password, host, port, bdd )
        self.write = Connection_DB( user, password, host, port, bdd )       

    def disconnect(self) -> None:
        """Ferme toutes les connexions à la base de données."""
        self.read.disconnect()
        self.write.disconnect()
        
    @log_execution_time
    def read_sql_query(self, sql) -> pd.DataFrame:
        """Exécute une requête SQL en lecture."""
        try:            
            query = sqlalchemy.text(sql)
            with self.read.connect() as conn:                
                return pd.read_sql(query, conn)
        except Exception as e:
            logger_info.error(f"Erreur lors de l'exécution de la requête SQL en lecture : {str(e)}")
            raise
            
    @log_execution_time
    def execute_query(self, sql: str):# -> Result:
        """Exécute une requête SQL en écriture."""
        try:   
            with self.write.connect() as conn:
                return conn.execute(sql)
        except Exception as e:
            logger_info.error(f"Erreur lors de l'exécution de la requête SQL en écriture : {str(e)}")
            raise   
            
    @log_execution_time        
    def read_sql_query_file(self, sqlfilename : Path) -> pd.DataFrame:
        """Lit et exécute une requête SQL depuis un fichier."""
        try: 
            with sqlfilename.open('r', encoding='utf-8') as fd:
                requete = fd.read()
            if not isinstance(requete, str):
                raise ValueError(f"Le contenu du fichier {sqlfilename} n'est pas une chaîne de caractères valide.")
            #logger_info.info(f"Contenu du fichier SQL : {requete[:100]}...")  # Log les 100 premiers caractères
            return self.read_sql_query(requete)   
        except Exception as e:
            logger_info.error(f"Erreur lors de la lecture et exécution du fichier SQL {sqlfilename}: {str(e)}")
            raise

    @log_execution_time        
    def insert_dataframe(self, df_out: pd.DataFrame, table: str, if_exists: str = 'append') -> None:
        """Insère un DataFrame dans une table de la base de données."""
        try:
            df_out.to_sql(table, self.write.engine, schema=SCHEMA, index=False, if_exists=if_exists)
            logger_info.info(f"Insertion réussie de {len(df_out)} lignes dans {SCHEMA}.{table}")
        except Exception as e:
            logger_info.error(f"Erreur lors de l'insertion d'un un DataFrame dans {SCHEMA}.{table} : {str(e)}")
            raise

    @log_execution_time        
    def delete_day_data(self, date: Union[str, datetime, date]) -> None:
        """Supprime les données d'un jour spécifique."""
        try:
            query = f"DELETE FROM {SCHEMA}.{Tables.JOURS} WHERE date = '{date}'"
            self.execute_query(query)
            logger_info.info(f"Données du {date} supprimées de la table {Tables.JOURS}")
        except Exception as e:
            logger_info.error(f"Erreur lors de la suppression des données du jour : {str(e)}")
            raise

    @log_execution_time        
    def delete_month_data(self, year: int, month: int) -> None:
        """Supprime les données d'un mois spécifique."""
        try:
            query = f"DELETE FROM {SCHEMA}.{Tables.MOIS} WHERE EXTRACT(YEAR FROM date) = {year} AND EXTRACT(MONTH FROM date) = {month}"
            self.execute_query(query)
            logger_info.info(f"Données du mois {month}/{year} supprimées de la table {Tables.MOIS}")
        except Exception as e:
            logger_info.error(f"Erreur lors de la suppression des données du mois : {str(e)}")
            raise
            
    @log_execution_time
    def delete_yesterday_data(self) -> None:
        """Supprime les données de la veille."""
        yesterday = (datetime.now() - timedelta(days=1)).date()
        self.delete_day_data(yesterday)


#Fonction utilitaires pour les flags
def create_flag(flag_name: Path) -> None:
    try:
        with open(flag_name, 'w') as f:
            f.write(date.today().isoformat())
        logger_info.info(f"Flag créé : {flag_name}")
    except Exception as e:
        logger_info.error(f"Erreur lors de la création du flag : {flag_name} : {str(e)}")
        raise

def create_file_flag(file_path: Path) -> None:
    try:
        flag_file = file_path.with_suffix('.flag')
        with open(flag_file, 'w') as f:
            f.write(date.today().isoformat())
        logger_info.info(f"Flag créé : {flag_file}")
    except Exception as e:
        logger_info.error(f"Erreur lors de la création du flag : {flag_name} : {str(e)}")
        raise
        
def check_flag(flag_name: Path) -> bool:
    try:
        flag_name_flag = flag_name.with_suffix('.flag')
        logger_info.info(f"Vérification de l'existence du flag : {flag_name_flag}")
        if flag_name_flag.exists() and flag_name_flag.suffix == '.flag':
            with open(flag_name_flag, 'r', encoding='utf-8') as f:
                flag_date = date.fromisoformat(f.read().strip())
            
            today = date.today()
            
            if flag_name_flag == FLAG_FILE_MONTHLY:
                # Vérifier si le flag est du mois courant
                return flag_date.replace(day=1) == today.replace(day=1)
            elif flag_name_flag == FLAG_FILE_DAILY:
                return flag_date == today
        return False
    except Exception as e:
        logger_info.error(f"Erreur lors de la vérification de l'existence du flag : {flag_name} : {str(e)}")
        raise
        
def remove_flag(flag_name: Path) -> None:
    logger_info.info(f"Flag supprimé : {flag_name}")
    try:
        if flag_name.exists() and flag_name.suffix == '.flag':        
            flag_name.unlink()
            logger_info.info(f"Flag supprimé : {flag_name}")
    except Exception as e:
        logger_info.error(f"Erreur lors du suppression du flag : {flag_name} : {str(e)}")
        raise   
        
        
def remove_flag_path(flag_name: Path)  -> None:
    # Suppression de tous les flags des scripts SQL
    for sql_file in flag_name.glob('**/*.flag'):        
        remove_flag(sql_file)
    logger_info.info("Tous les flags des scripts SQL ont été supprimés.")
        
class BaseTable:
    """Classe de base pour les opérations sur les tables."""
    def __init__(self, table_name: str, db: DatabaseAccess):        
        self.schema: str = SCHEMA
        self.table_name: str = table_name
        self.db: DatabaseAccess = db
        
    def get_data(self) -> pd.DataFrame:
        """Récupère toutes les données de la table."""
        query = f"SELECT * FROM {self.schema}.{self.table_name}"
        return self.db.read_sql_query(query)     
    
    def update(self, new_data: pd.DataFrame) -> None:
        """Méthode abstraite pour mettre à jour les données."""
        raise NotImplementedError("La sous-classe doit implémenter la méthode abstraite")


# Centralisation des noms de tables
class Tables:
    CALC : str = 'hcal_dfa_hok_calc'
    JOURS : str = 'hcal_dfa_hok_jours'
    MOIS : str = 'hcal_dfa_hok_mois'
    MAILLES : str = 'hcal_dfa_hok_mailles'

class Maille(BaseTable):
    """Classe pour gérer les mailles (structures hiérarchiques)."""
    def __init__(self, db: DatabaseAccess):
        super().__init__(Tables.MAILLES, db)     
        
    def update(self, new_data: pd.DataFrame) -> None:
        """Met à jour les mailles avec de nouvelles données."""
        if not isinstance(new_data, pd.DataFrame) or new_data.empty:
            logger_info.warning("Aucune nouvelle donnée à mettre à jour pour les mailles")
            return
        
        try:
            logger_info.info("Mise à jour des mailles")
            df_mailles_existing = self.get_data()
            
            new_data = new_data.drop_duplicates(subset=['maille'])            
            new_mailles = new_data[~new_data['maille'].isin(df_mailles_existing['label'])]
            
            if not new_mailles.empty:
                self._insert_new_mailles(new_mailles, df_mailles_existing)
            else:
                logger_info.info("Aucune nouvelle maille à ajouter")

        except Exception as e:
            logger_info.error(f"Erreur lors de la mise à jour des mailles : {str(e)}")
            raise

    def _insert_new_mailles(self, new_mailles: pd.DataFrame, existing_mailles: pd.DataFrame) -> None:
        max_id = existing_mailles['id_maille'].max() if not existing_mailles.empty else 0
        label_to_id = dict(zip(existing_mailles['label'], existing_mailles['id_maille']))
        new_mailles_to_insert = []

        def insert_maille(label: str, parent_label: Optional[str]) -> Dict[str, Union[int, str]]:
            nonlocal max_id
            if label in label_to_id:
                return  # La maille existe déjà
            
            max_id += 1
            parent_id = label_to_id.get(parent_label, 0)
            new_maille = {
                'id_maille': max_id,
                'label': label,
                'id_parent': parent_id
            }
            new_mailles_to_insert.append(new_maille)
            label_to_id[label] = max_id
            return new_maille

        # D'abord, insérer toutes les mailles parents
        for _, row in new_mailles.iterrows():            
            parent_label = row['maille_parent']
            if parent_label and parent_label not in label_to_id:
                insert_maille(parent_label, None)
                logger_info.info(f"Maille parent créée : {parent_label}")

        # Ensuite, insérer les mailles enfants
        for _, row in new_mailles.iterrows():
            label = row['maille']
            parent_label = row['maille_parent']
            insert_maille(label, parent_label)

        if new_mailles_to_insert:
            df_to_insert = pd.DataFrame(new_mailles_to_insert)
            self.db.insert_dataframe(df_to_insert, self.table_name)
            logger_info.info(f"Ajout de {len(new_mailles_to_insert)} nouvelles mailles")

class Calc(BaseTable):
    """Classe pour gérer les calculs."""
    def __init__(self, db: DatabaseAccess, maille: Maille, rapport: List[str]):
        super().__init__(Tables.CALC, db)        
        self.rapport = rapport
        self.maille = maille

    def update(self, new_calcs: pd.DataFrame) -> None:
        """Met à jour les calculs avec de nouvelles données."""
        if not isinstance(new_calcs, pd.DataFrame) or new_calcs.empty:
            logger_info.warning("Aucune nouvelle donnée à mettre à jour pour les calculs")
            return

        try:
            logger_info.info("Mise à jour des calculs")
            df_calcs_existing = self.get_data()
            
            new_calcs = new_calcs.drop_duplicates(subset=['indicateur'])
            new_calcs_to_add = new_calcs[~new_calcs['indicateur'].isin(df_calcs_existing['label'])]

            if not new_calcs_to_add.empty:
                self._insert_new_calcs(new_calcs_to_add, df_calcs_existing)
            else:
                logger_info.info("Aucun nouveau calcul à ajouter")

        except Exception as e:
            logger_info.error(f"Erreur lors de la mise à jour des calculs : {str(e)}")
            raise

    def _insert_new_calcs(self, new_calcs: pd.DataFrame, existing_calcs: pd.DataFrame) -> None:
        max_id = existing_calcs['id_calc'].max() if not existing_calcs.empty else 0
        calc_label_to_id = dict(zip(existing_calcs['label'], existing_calcs['id_calc']))
        maille_label_to_id = dict(zip(self.maille.get_data()['label'], self.maille.get_data()['id_maille']))
        new_calcs_to_insert = []

        def insert_calc(indicateur: str, parent: Optional[str], maille: str) -> Dict[str, Union[int, str, List[str]]]:
            nonlocal max_id
            if indicateur in calc_label_to_id:
                return  # Le calcul existe déjà
            
            max_id += 1
            parent_id = calc_label_to_id.get(parent, 0)
            id_maille_groupe = maille_label_to_id.get(maille, 0)
            new_calc = {
                'id_calc': max_id,
                'label': indicateur,
                'id_parent': parent_id,
                'id_maille_groupe': id_maille_groupe,
                'rapports': self.rapport
            }
            new_calcs_to_insert.append(new_calc)
            calc_label_to_id[indicateur] = max_id
            return new_calc

        # D'abord, insérer tous les calculs parents
        for _, row in new_calcs.iterrows():            
            parent_label = row['indicateur_parent']
            if parent_label and parent_label not in calc_label_to_id:
                insert_calc(parent_label, None, row['maille_parent'])
                logger_info.info(f"Calcul parent créé : {parent_label}")

        # Ensuite, insérer les calculs enfants
        for _, row in new_calcs.iterrows():
            label = row['indicateur']
            parent_label = row['indicateur_parent']
            maille_label = row['maille_parent']
            insert_calc(label, parent_label, maille_label)

        if new_calcs_to_insert:
            df_to_insert = pd.DataFrame(new_calcs_to_insert)
            self.db.insert_dataframe(df_to_insert, self.table_name)
            logger_info.info(f"Ajout de {len(new_calcs_to_insert)} nouveaux calculs")

class Jour(BaseTable):
    """Classe pour gérer les données journalières."""
    def __init__(self, db: DatabaseAccess, rapport: List[str], date:date):
        super().__init__(Tables.JOURS, db)
        self.maille: Maille = Maille(db)        
        self.rapport: List[str] = rapport
        self.date: date = date

    def update(self, new_data: pd.DataFrame) -> None:
        """Met à jour les calculs avec de nouvelles données."""
        if not isinstance(new_data, pd.DataFrame) or new_data.empty:
            logger_info.warning("Aucune nouvelle donnée à mettre à jour pour les jours")
            return
        
        try:
            logger_info.info("Mise à jour des données journalières")
            
            calc_data = Calc(self.db, self.maille, self.rapport).get_data()
            maille_data = self.maille.get_data()
            
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

            new_data['date'] = self.date

            # Sélectionnez uniquement les colonnes nécessaires
            to_insert = new_data[['id_calc', 'id_maille', 'date', 'valeur']]

            # Supprimez les lignes avec des valeurs manquantes
            to_insert = to_insert.dropna()

            if not to_insert.empty:
                self.db.insert_dataframe(to_insert, self.table_name)
                logger_info.info(f"Insertion de {len(to_insert)} nouvelles lignes dans la table des jours")
            else:
                logger_info.warning("Aucune nouvelle donnée valide à insérer dans la table des jours")
            
        except Exception as e:
            logger_info.error(f"Erreur lors de la mise à jour des données journalières : {str(e)}")
            raise   

class Mois(BaseTable):
    """Classe pour gérer les données mensuelles."""
    def __init__(self, db: DatabaseAccess, rapport_dir: Path):
        super().__init__(Tables.MOIS, db)
        self.rapport_dir: Path = rapport_dir
        
    def update(self) -> None:   
        """Met à jour les calculs avec de nouvelles données."""
        try:
            # Vérifier si le fichier indic_mois_kpi.sql existe
            sql_file = os.path.join(self.rapport_dir, SQL_MOIS)
            
            if os.path.exists(sql_file):
                # Si le fichier existe, exécuter le script SQL
                df_mois = self.db.read_sql_query_file(sql_file)
                
            else:
                rapport = [os.path.basename(self.rapport_dir).upper()]
                # Sinon, utiliser la requête SQL fournie
                query = f"""
                SELECT id_calc, id_maille, 
                       date_trunc('month', (current_date - interval '1 month'))::date as "date", 
                       avg(valeur) as valeur 
                FROM {self.schema}.{Tables.JOURS} j 
                JOIN {self.schema}.{Tables.CALC} c USING(id_calc) 
                WHERE extract(month from "date") = extract(month from current_date - interval '1 month') 
                  AND c.rapports @> array[{rapport}]
                GROUP BY id_calc, id_maille, 
                         date_trunc('month', (current_date - interval '1 month'))::date
                """
                df_mois = self.db.read_sql_query(query)

            if not df_mois.empty:
                self.db.insert_dataframe(df_mois, self.table_name, if_exists='append')
                logger_info.info(f"Ajout de {len(df_mois)} historisations mensuelles")
                
            else:
                logger_info.warning("No monthly data to update")
            
        except Exception as e:
            logger_info.error(f"Erreur de mise à jour des données mensuelles : {str(e)}")
            raise    
            
# Classe Rapport pour le traitement des fichiers SQL
# @log_execution_time
class Rapport:
    def __init__(self, rapport_dir: Path, db: DatabaseAccess, date: Optional[date] = None):
        try:            
            logger_info.info("Initialisation du rapport")  
            self.db: DatabaseAccess = db
            self.date = date = date if date else datetime.now().date()  
            self.rapport_dir: Path =  Path(rapport_dir)
            self.rapport: List[str] = [self.rapport_dir.name.upper()]
            self.maille: Maille = Maille(db)
            self.calc: Calc = Calc(db, self.maille, self.rapport)
            self.jour: Jour = Jour(db, self.rapport, self.date)
            self.mois: Mois = Mois(db, rapport_dir)
            self.exitcode = 0    
            
        except Exception as e:
            logger_info.error(f"Erreur lors de l'initialisation du rapport : {str(e)}")
            self.exitcode += 1
            raise     
        
    def process_sql_files(self) -> int:
        sql_files = list(self.rapport_dir.glob('*.sql'))   
        trouve = False
        try:
            # Traitement de tous les fichiers SQL
            for sql_file in sql_files:
                if sql_file.name != SQL_MOIS:
                    if not check_flag(sql_file):
                        logger_info.info(f"Traitement du fichier SQL : {sql_file}")
                        try:                    
                            try:                    
                                logger_info.info(f"Lecture du fichier SQL : {sql_file}")
                                data = self.db.read_sql_query_file(sql_file)
                                logger_info.info(f"Données lues : {data.shape}")
                            except Exception as e:
                                logger_info.error(f"Erreur lors du traitement du fichier {sql_file}: {str(e)}")
                                logger_info.error(f"Type de l'erreur : {type(e)}")
                                logger_info.error(f"Traceback : ", exc_info=True)
                                self.exitcode = 1

                            if isinstance(data, pd.DataFrame):
                                if set(SQL_FORMAT_LABELS).issubset(data.columns):
                                    self.maille.update(data[['maille', 'maille_parent']])
                                    self.calc.update(data[['indicateur', 'indicateur_parent', 'maille_parent']])
                                    self.jour.update(data)
                                    create_file_flag(sql_file)
                                else:
                                    logger_info.error(f"Colonnes manquantes dans le fichier {sql_file}")
                            else:
                                logger_info.error(f"Le fichier {sql_file} n'a pas retourné de DataFrame valide.")
                        except Exception as e:
                            logger_info.error(f"Erreur lors du traitement du fichier {sql_file}: {str(e)}")
                            self.exitcode = 1
                    else:
                        logger_info.info(f"Le fichier {sql_file.name} a déjà été traité aujourd'hui.")
                        self.exitcode = 1
                else:
                    # Traitement de indic_mois_kpi.sql
                    trouve = True
                    if not check_flag(FLAG_FILE_MONTHLY):               
                        
                        logger_info.info(f"Traitement du fichier mensuel : {sql_file}")
                        self.mois.update()
                        create_flag(FLAG_FILE_MONTHLY)
                        create_file_flag(sql_file)                        
                    else:
                        logger_info.info("Le traitement mensuel a déjà été effectué ce mois-ci.")
                        self.exitcode = 1
            
            if not trouve and not check_flag(FLAG_FILE_MONTHLY):
                logger_info.info(f"Traitement du fichier mensuel : {sql_file}")
                self.mois.update()
                create_flag(FLAG_FILE_MONTHLY)
                create_file_flag(sql_file)  

        except Exception as e:
            logger_info.error(f"Erreur lors du traitement processus des fichiers SQL : {str(e)}")
            self.exitcode += 1
            raise
        finally : 
            return self.exitcode
            
#Fonction permettant de modifier le comportement du script en fonction d'arguments        
def process_default(db_connection: DatabaseAccess, path: Path = SQLPATH, date: Optional[date] = None) -> int:
    """Traitement par défaut sans arguments spécifiques."""
    exitcode = 0    
    try :       
        date = date if date else datetime.now().date() 
        if not check_flag(FLAG_FILE_DAILY) or date : 
            
            logger_info.info(f"Exécution du traitement journalier par défaut pour le rapport {Path}.")
            for rapport_dir in pathlib.Path(path).glob('*'):
                if rapport_dir.is_dir():                   
                    logger_info.info(f"Traitement du répertoire de rapport : {Path(rapport_dir)}")                    
                    logger_info.info(f"Type de date : {date}")
                    try :
                        rapport = Rapport(Path(rapport_dir), db_connection, date)
                        rapport.process_sql_files()
                    except Exception as e:
                        logger_info.error(f"Erreur lors appel Rapport : {str(e)}")
                        raise

            # Création des flags après l'exécution réussie
            if not date :
                create_flag(FLAG_FILE_DAILY)     

        else:
            logger_info.warning("Le traitement journalier a déjà été effectué aujourd'hui.")       
            exitcode = 1 

        logger_info.info("Traitement terminé avec succès.")
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement par défauts des fichiers SQL : {str(e)}")
        exitcode += 1 
        raise
    finally:  
        return exitcode
    
def process_suppression_journaliere(db_connection: DatabaseAccess, date: Optional[date] = None, path: Path = SQLPATH) -> int:
    """Traitement forcé des données journalières."""
    exitcode = 0
    try :        
        date = date if date else datetime.now().date() 
        
        #Suppression des données du jour
        db_connection.delete_day_data(date)
        logger_info.info(f"Données du {date} supprimées.")
        
        #Suppression du flag du jour
        remove_flag(FLAG_FILE_DAILY)
        logger_info.info("Flag journalier supprimé.")
        
        # Suppression de tous les flags des scripts SQL
        remove_flag_path(path)        
        
        logger_info.info("Données du jour supprimées. Recalcul forcé.")
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement forcé du jour : {str(e)}")
        exitcode += 1 
        raise
    finally:  
        return exitcode
    
def process_jour(db_connection: DatabaseAccess, date: Optional[date] = None, path: Path = SQLPATH) -> int:
    """Traitement forcé des données journalières."""
    exitcode = 0
    try :                 
        
        #Supppression des données journalière
        exitcodej = process_suppression_journaliere(db_connection, date)
        
        #Relance du calcul pour tous les rapports en utilisant process_default
        exitcoded = process_default(db_connection,path, date)
        
        logger_info.info("Données du jour supprimées. Recalcul forcé.")
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement forcé du jour : {str(e)}")
        exitcode += 1 
        raise
    finally:  
        return max(exitcodej,exitcoded)

def process_mois(db_connection: DatabaseAccess) -> int:
    """Traitement forcé des données mensuelles."""
    exitcode = 0
    try :           
        
        #Suppression des données du mois
        last_month = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)
        db_connection.delete_month_data(last_month.year, last_month.month)
        logger_info.info(f"Données du mois {last_month.month}/{last_month.year} supprimées.")
        
        
        #Suppression du flag du mois
        remove_flag(FLAG_FILE_MONTHLY)
        logger_info.info("Flag mensuel supprimé.")
        
        # Relance du calcul mensuel pour tous les rapports
        for rapport_dir in SQLPATH.glob('*'):
            if rapport_dir.is_dir():
                mois = Mois(db_connection, str(rapport_dir))
                mois.update()  
        
        logger_info.info("Données du mois précédent supprimées. Calcul des KPI mensuels relancé pour tous les rapports.")
        
        # Créer le flag après la mise à jour réussie
        create_flag(FLAG_FILE_MONTHLY)
        logger_info.info("Flag mensuel créé.")
        
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement forcé des données mensuelles : {str(e)}")
        exitcode += 1 
        raise
        
    finally:  
        return exitcode

def process_veille(db_connection: DatabaseAccess) -> int:
    """Traitement forcé des données de la veille."""
    exitcode = 0
    try :
        yesterday = (datetime.now() - timedelta(days=1)).date()
        
        # Suppression des données de la veille        
        process_jour(db_connection,yesterday)        
        
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement forcé de la vieille : {str(e)}")
        exitcode += 1 
        raise
    finally:  
        return exitcode
    
def process_rapport(db_connection: DatabaseAccess, rapport_dir: Path) -> int:
    """Calcul des données d'un rapport"""
    exitcode = 0
    try :
        logger_info.info(f"Calcul des données du rapport : {rapport_dir}.")
       
        rapport = Rapport(rapport_dir, db_connection)
        rapport.process_sql_files()
        
    except Exception as e:
        logger_info.error(f"Erreur lors du traitement forcé d'un rapport' : {str(e)}")
        exitcode += 1 
        raise
    finally:  
        return exitcode

def parse_arguments() -> Optional[argparse.Namespace]:
    """Parse les arguments de ligne de commande ou retourne None si exécuté dans Jupyter."""
    if 'ipykernel' in sys.modules:
        # Exécution dans Jupyter, pas de parsing d'arguments
        return None
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

# Fonction principale
@log_execution_time
def main() -> int:
    # Configuration des arguments de ligne de commande
    args = parse_arguments()    
    exitcode = 0
    try:   
        
        # Si aucun argument spécifique n'est fourni, exécuter le traitement par défaut
        if args is None:
            # Exécution dans Jupyter Notebook
            logger_info.info("Définision des arguments car None")
            user = "YR45209N"
            password = "0jvA28Q=r7h4NozhZT4I"
            host = "db_prd_usr_dfa.edf.fr"
            port = "5452"
            bdd = "dfacto"
            path_rapport = Path("sql/r035")
        else:
            logger_info.info("Définision des arguments via args")            
            user = args.user
            password = args.password
            host = args.host
            port = args.port
            bdd = args.bdd
            path_rapport = Path(args.rapport)

        db_connection = DatabaseAccess(
            user=user,
            password=password,
            host=host,
            port=port,
            bdd=bdd)
                
        try: 
            if args is None or (not args.jour and not args.mois and not args.veille and not args.rapport):
                exitcode = process_default(db_connection)
                #exitcode = process_suppression_journaliere(db_connection)
                #exitcode = process_rapport(db_connection,Path("sql/r026"))     
                #process_mois(db_connection)
                #exitcode = process_veille(db_connection)
            else:
                if args.jour:
                    exitcode = process_jour(db_connection)
                if args.mois:
                    exitcode = process_mois(db_connection)
                if args.veille:
                    exitcode = process_veille(db_connection)
                if args.suppression:
                    exitcode = process_suppression_journaliere(db_connection)
                if args.rapport:
                    r = Rapport(Path(args.rapport), db_connection)
                    r.process_sql_files()
                    exitcode = rapport.exitcode
                
                
        except Exception as e:
            logger_info.error(f"Erreur générale traitement rapport : {str(e)}")
            exitcode = 1
            raise   

                
    except Exception as e:
        logger_info.error(f"Erreur générale : {str(e)}")
        exitcode = 1
        raise
        
    finally:        
        if 'db_connection' in locals():
            db_connection.disconnect()            
        return exitcode
        
if __name__ == "__main__":
    sys.exit(main())

