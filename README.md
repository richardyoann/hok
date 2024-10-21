# HoK

Historisation des indicateurs ou History of KPI

## Version
v1.0.0 - 11/05/2023 - Version initiale

## Documentation
https://si-confluence.edf.fr/display/DWW8DATAIT/T012+HOK

# Système de Traitement et d'Historisation des Données

## Vue d'ensemble

Ce projet est un système de traitement et d'historisation de données, conçu pour gérer efficacement des opérations sur des bases de données, des calculs et des rapports. Il est structuré de manière modulaire pour faciliter la maintenance et l'extensibilité.

## Structure du Projet

Le projet est divisé en plusieurs modules Python :

1. `models.py`: Contient les classes de modèle pour la gestion des données.
2. `controllers.py`: Implémente les contrôleurs pour gérer la logique métier.
3. `errors.py`: Gère la logique de traitement et d'enregistrement des erreurs.
4. `flags.py`: Gère les flags pour contrôler l'exécution des tâches.
5. `utils.py`: Contient des utilitaires et des décorateurs communs.
6. `config.py`: Gère le chargement et l'accès à la configuration.
7. `database.py`: Gère les opérations de base de données.
8. `application.py`: Contient la classe principale de l'application.
9. `main.py`: Point d'entrée principal du script.
10. `logger.py`: Configure et gère le système de logging.

## Configuration

La configuration de l'application se fait via un fichier `config.json`. Les paramètres importants incluent :

- Informations de connexion à la base de données
- Chemins des fichiers SQL
- Configuration des flags
- Paramètres de rétention des données

## Utilisation

Le script peut être exécuté avec différentes options :

bash
python main.py [--jour] [--mois] [--veille] [--sup] [--rapport CHEMIN] [--user USER] [--password PASSWORD] [--host HOST] [--port PORT] [--bdd BDD]

Options :

--jour: Force le recalcul des données journalières
--mois: Force le recalcul des données mensuelles
--veille: Force le recalcul des données de la veille
--sup: Force la suppression des données journalières
--rapport: Spécifie le chemin vers le répertoire du rapport
Autres options pour la configuration de la base de données

## Logging
Le système utilise un logger centralisé configuré dans logger.py. Les logs sont écrits à la fois dans un fichier (avec rotation) et sur la console. Le niveau de log peut être configuré dans le fichier de configuration.
Gestion des Erreurs
Les erreurs sont gérées de manière centralisée via la classe HistorisationError. Cette approche permet un suivi cohérent des erreurs à travers l'application.

## Extensibilité
Le système est conçu pour être facilement extensible :

Nouveaux modèles peuvent être ajoutés dans models.py
Nouveaux contrôleurs peuvent être implémentés dans controllers.py
Des fonctionnalités supplémentaires peuvent être ajoutées en étendant les classes existantes ou en créant de nouvelles classes

## code de sortie

- [ ] 0		succès
- [ ] >0	erreur, plus liste des erreurs remontées part les exceptions

## log
voir ./log

## requetes sql
voir ./sql

## code
Le script .ipynb fait fois et est la référence à tenir à jour.
Le sript est développé sous Jupyter, prendre le .ipynb et travailler dessus.
Une fois les modifs réalisées, exporter au format script pour générer le .py.
Les deux formats sont stockés ici.

conversion du fichier .ipynb en .py:
```jupyter nbconvert histok.ipynb --no-prompt --to python```

supprimer les commentaires du fichier .py
```grep -Eo '^[^#]*' histok.py  | sed s/[[:space:]]*$//g | cat -s > histok2.py```

supprimer les commentaires du fichier .py
```mv histok2.py histok.py```

