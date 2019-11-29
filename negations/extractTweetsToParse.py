# coding: utf-8
import argparse
import logging
import inspect
import ujson
import glob
import csv
import os


# Args
parser = argparse.ArgumentParser(description = "Récupère les tweets d'utilisateurs précis")
parser.add_argument("--years", "-y", default=[""], nargs='+', type=str, help="Années pour lesquelles on veut extraire les tweets (les mettre à la suite, en les séparant par un espace)")
parser.add_argument("--readPath", "-r", required=True, help="Chemin vers les fichiers desquels on veut extraire les tweets appartenant aux utilisateurs ciblés")
parser.add_argument("--writePath", "-w", required=True, help="Répertoire dans lequel on écrira les tweets extraits")
args = parser.parse_args()

years = args.years
readPath = os.path.abspath(args.readPath)+"/"
writePath = os.path.abspath(args.writePath)+"/"

if readPath == writePath :
    raise ValueError("les répertoires des données entrantes et sortantes doivent être différents")

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+("_").join(str(y) for y in years)+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("Lecture des fichiers à partir du répertoire %s", readPath)
logger.info("Écriture des tweets récupérés dans le répertoire %s", writePath)

# Récupération des utilisateurs
usersToRecover = {}

users_file = open("location_incomes.tsv","r")
reader = csv.reader(users_file, delimiter=" ")

for line in reader :
    if line[0]!="TwitterUID" :
        usersToRecover[str(line[0])]=""

users_file.close()

logger.info("%d utilisateurs ciblés.",len(usersToRecover))


# Récupération de la liste des fichiers à vérifier
files = []

for year in years :
    files+=sorted(glob.glob(readPath+year+"*data"))

logger.info("Années prises en compte : %s (une liste vide signifie qu'elles sont toutes prises en compte)", str(years))
logger.info("%d fichiers à traiter", len(files))


# Récupération des tweets des utilisateurs prioritaires
logger.info("Récupération des tweets...")

for i,file in enumerate(files) :
    fileName = os.path.basename(file)
    logger.info("Traitement du fichier %s (%d/%d)", fileName,i+1,len(files))
    file = open(file, "r")
    nbTweets = 0
    for line in file :
        tweet = ujson.loads(line)
        if str(tweet["user"]["id"]) in usersToRecover :
            if (not os.path.exists(writePath+fileName)) :
                fileOut = open(writePath+fileName, "w")
            nbTweets+=1
            fileOut.write(ujson.dumps(tweet)+"\n")
    logger.info("%d tweets récupérés dans le fichier %s", nbTweets,fileName)
    if (os.path.exists(writePath+fileName)) :
        fileOut.close()
    file.close()

logger.info("Tous les tweets des utilisateurs ciblés ont été récupérés")
