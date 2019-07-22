# coding: utf-8

import sqlite3
import logging
import inspect
import ast

#log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

conn = sqlite3.connect("./infosNeg.db")
cursor = conn.cursor()

tweetsByUser = {}
negByTweet = {}
negByUser = {}

logger.info("récupération des tweets par utilisateur...")
cursor.execute("SELECT * from tweetsByUser")
result = cursor.fetchall()

for r in result :
    user = r[0]
    tweets = ast.literal_eval(r[1])
    tweetsByUser[user]=tweets

logger.info("Récupération du nombre de négations par tweet...")
cursor.execute("SELECT * from negByTweet")
result = cursor.fetchall()

for r in result :
    tweet = r[0]
    neg = r[1]
    std = r[2]
    negByTweet[tweet]={}
    negByTweet[tweet]["neg"]=neg
    negByTweet[tweet]["std"]=std

logger.info("Récupération du nombre de négations par utilisateur")
for i,user in enumerate(tweetsByUser) :
    logger.info("Récupération des négations pour l'utilisateur %s (%d/%d)", user, i+1, len(tweetsByUser))
    negByUser[user]={"neg":0, "std":0}
    for tweet in tweetsByUser[user] :
        negByUser[user]["neg"]+=negByTweet[tweet]["neg"]
        negByUser[user]["std"]+=negByTweet[tweet]["std"]

cursor.execute("CREATE TABLE negByUser (user text, neg integer, std integer, std_rate real)")
toRegister = []
for user in negByUser :
    neg = int(negByUser[user]["neg"])
    std = int(negByUser[user]["std"])
    if neg!=0 :
        std_rate = std/neg
    else :
        std = 0.0
    toRegister.append((user, neg, std, std_rate))

logger.info("Inscription dans la base de données")

cursor.executemany("INSERT INTO negByUser VALUES (?, ?, ?, ?)", toRegister)
conn.commit()
conn.close()

logger.info("Inscription terminée")
