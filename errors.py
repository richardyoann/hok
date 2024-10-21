#!/usr/bin/env python
# coding: utf-8
from typing import List, Tuple

class HistorisationError(Exception):
    def __init__(self):
        """
        Constructeur de la classe.

        Initialise les attributs :
        - error_messages : liste des messages d'erreur
        - total_exitcode : code de sortie total (somme des codes de sortie des erreurs)
        """
        self.error_messages: List[str] = []
        self.total_exitcode: int = 0

    def add_error(self, message: str) -> None:
        """
        Ajoute un message d'erreur dans la liste des erreurs et incrémente le total des codes de sortie.
        :param message: message d'erreur à ajouter
        :return: None
        """
        self.error_messages.append(message)
        self.total_exitcode += 1
        return self

    def get_summary(self) -> Tuple[List[str], int]:        
        """
        Renvoie un tuple contenant la liste des messages d'erreur et le total des codes de sortie.
        :return: un tuple (liste des messages d'erreur, total des codes de sortie)
        """
        return self.error_messages, self.total_exitcode

    def get_str(self) -> str:        
        """
        Return un message listant les messages d'erreur ainsi que le nombre d'erreur.
        """
        return f"Nombre total d'erreurs : {self.total_exitcode}\nMessages d'erreur :\n" + \
               "\n".join(f"- {msg}" for msg in self.error_messages)