# coding: utf-8

import logging
import sqlite3
import inspect
import glob
import re
import os

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("Récupération du nombre de négations standard et non standard par tweet")

path = "/warehouse/COMPLEXNET/ltarrade/recuperation_negation/all_tweetsPrio/"

logger.info("Répertoire d'entrée : %s"%path)

filesNeg = sorted(glob.glob(path+"*neg*tsv"))
filesStd = sorted(glob.glob(path+"*std*tsv"))

negParTweet = {}

for i,file in enumerate(filesNeg) :
    fileName = os.path.basename(file)
    logger.info("Récupération du nombre de négations dans le fichier %s (%d/%d)"%(fileName, i+1, len(filesNeg)))
    file = open(file)
    for j,line in enumerate(file) :
        if j!=0 :
            infos = re.match(r"^(\d+)\t(\d+)$", line)
            idTweet = infos.group(1)
            nbNeg = infos.group(2)
            negParTweet[idTweet]={"neg":0, "std":0}
            negParTweet[idTweet]["neg"]=nbNeg

for i,file in enumerate(filesStd) :
    fileName = os.path.basename(file)
    logger.info("Récupération du nombre de négations standard dans le fichier %s (%d/%d)"%(fileName, i+1, len(filesNeg)))
    file = open(file)
    for j,line in enumerate(file) :
        if j!=0 :
            infos = re.match(r"^(\d+)\t(\d+)$", line)
            idTweet = infos.group(1)
            nbStd = infos.group(2)
            negParTweet[idTweet]["std"]=nbStd

conn = sqlite3.connect('infosNeg.db')
cursor = conn.cursor()
cursor.execute("CREATE TABLE negByTweets (tweet text, neg integer, std integer)")

toRegister = []
for tweet in negParTweet :
    toRegister.append((tweet, int(negParTweet[tweet]["neg"]), int(negParTweet[tweet]["std"])))

logger.info("Début inscription dans base de données")

cursor.executemany("INSERT INTO negByTweets VALUES (?, ?, ?)", toRegister)
conn.commit()

conn.close()

logger.info("Inscription terminée")
