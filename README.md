# Decaux
Extrait du prototype repris lors du projet intellicités

L'objectif du projet repose sur une récupération en 10 tables FNBC des données relatives aux vélos en libre service détenus par la société DECAUX en temps réel.

L'extrait ici présent peut être sorti de boucle while pour un usage en crontab

pour fonctionner, l'algorithme nécessite la création d'un fichier private.py en racine du projet avec :

mdp= mot de passe de la bdd phpmyadmin
adress= adresse de la base de donnée
user= utilisateur de la bdd
apim= api key openweather
apiKey = api key Decaux

code en python 3.5

librairies :
- pandas
- time
- pymysql
- requests
