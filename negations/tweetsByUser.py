# coding: utf-8

import datetime
import logging
import sqlite3
import inspect
import ujson
import glob
import os

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("Récupération des tweets par utilisateur...")

files = sorted(glob.glob("/warehouse/COMPLEXNET/ltarrade/parseInPriority/tweets_toParse/*data"))

logger.info("dossier d'entrée : /warehouse/COMPLEXNET/ltarrade/parseInPriority/tweets_toParse/")
logger.info("%d fichiers à traiter"%(len(files)))

tweetsByUser = {}

for i,file in enumerate(files) :

    logger.info("Traitement du fichier %s (%d/%d)"%(os.path.basename(file), i+1, len(files)))
    t0=datetime.datetime.now()

    f = open(file)

    nbTweets=0

    for line in f :
        tweet = ujson.loads(line)
        idTweet = tweet["id"]
        idUser = tweet["user"]["id"]

        if idUser not in tweetsByUser :
            tweetsByUser[idUser]=[]
        tweetsByUser[idUser].append(tweet["id"])

        nbTweets+=1

    t1=datetime.datetime.now()
    logger.info("Fichier %s traité en %f secondes (%d tweets)"%(os.path.basename(file), (t1-t0).total_seconds(), nbTweets))

logger.info("Récupération terminée.")

logger.info("Inscription dans la base de données...")

conn = sqlite3.connect('tweetsByUser.db')
cursor = conn.cursor()
cursor.execute("CREATE TABLE tweetsByUser (user text, tweets text, nbTweets integer)")

i=0
for user in tweetsByUser :
    i+=1
    logger.info("Inscription de l'utilisateur %s (%d/%d)"%(user, i, len(tweetsByUser)))
    cursor.execute("INSERT INTO tweetsByUser VALUES (?, ?, ?)", (user, str(tweetsByUser[user]), len(tweetsByUser[user])))
    conn.commit()
conn.close()

logger.info("Inscription terminée")
