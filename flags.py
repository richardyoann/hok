#!/usr/bin/env python
# coding: utf-8
from datetime import date
from enum import Enum
from pathlib import Path
from errors import HistorisationError
from logger import get_logger
logger_info= get_logger()

class FlagType(Enum):
    DAILY = "day"
    MONTHLY = "month"

class FlagAction(Enum):
    CHECK = "check"
    CREATE = "create"
    REMOVE = "remove"   
    
class FlagManager:
    """Gère les flags pour l'historisation des KPI"""
    def __init__(self,historization_error: HistorisationError):  
        """
        Initialise le gestionnaire de flags.

        :param historization_error: Objet en charge de la gestion des erreurs de l'historisation.
        """      
        self.historization_error = historization_error        

    def manage_flag(self, flag_path: Path, action: FlagAction, flag_type: FlagType = FlagType.DAILY) -> bool:
        """
        Gère les opérations sur les flags (vérification, création, suppression)

        :param flag_path: Chemin du flag
        :param action: Action à effectuer sur le flag
        :param flag_type: Type du flag (par défaut : FlagType.DAILY)
        :return: Booléen indiquant si l'action a réussi
        """
        try:       
            flag_file = flag_path.with_suffix('.flag')
            
            if action == FlagAction.CHECK:
                return self._check_flag(flag_file, flag_type)
            elif action == FlagAction.CREATE:
                return self._create_flag(flag_file)
            elif action == FlagAction.REMOVE:
                return self._remove_flag(flag_file)            
        except Exception as e:
            error_msg = f"Erreur lors de l'action {action.value} sur le flag {flag_path}: {str(e)}"
            logger_info.error(error_msg)
            self.historization_error.add_error(error_msg)
            return False
        
    def _check_flag(self, flag_file: Path, flag_type: FlagType) -> bool:
        """Vérifie l'existence et la validité du flag"""
        logger_info.info(f"Vérification de l'existence du flag : {flag_file}")
        
        if not flag_file.exists():
            logger_info.info(f"Le flag : {flag_file} n'existe pas")            
            return False

        with open(flag_file, 'r', encoding='utf-8') as f:
            flag_date = date.fromisoformat(f.read().strip())
        
        today = date.today()
        
        if flag_type == FlagType.MONTHLY:
            return flag_date.replace(day=1) == today.replace(day=1)
        return flag_date == today

    def _create_flag(self, flag_file: Path) -> bool:
        """Crée le flag avec la date du jour"""
        with open(flag_file, 'w') as f:
            f.write(date.today().isoformat())
        logger_info.info(f"Flag créé : {flag_file}")
        return True

    def _remove_flag(self, flag_file: Path) -> bool:
        """Supprime le flag s'il existe"""
        if flag_file.exists():
            flag_file.unlink()
            logger_info.info(f"Flag supprimé : {flag_file}")
        return True