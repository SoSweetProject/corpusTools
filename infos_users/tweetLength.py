# coding: utf-8

from conllu import parse_incr
import pandas as pd
import sqlite3
import logging
import inspect
import glob
import sys
import re
import os

path = "/warehouse/COMPLEXNET/ltarrade/parsed_tweets/"
files = glob.glob(path+"*conllu")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("Dossier d'entrée : %s"%path)
logger.info("%i fichier à passer en revue"%len(files))

# Récupération d'un dico "length" avec la longueur de chaque tweet

length = {}

logger.info("...Récupération de la longueur de chaque tweet...")
print("Récupération de la longueur de chaque tweet\n")

for i,file in enumerate(files) : 
    
    fileName = os.path.basename(file)
    
    logger.info("Traitement du fichier %s (%i/%i)"%(fileName, i+1, len(files)))

    sys.stdout.write("\rTraitement du fichier %s (%i/%i)"%(fileName, i+1, len(files)))
    
    file = open(file, "r")
    sentences = parse_incr(file)

    for tweet in sentences :

        # Récupération de l'id du tweet
        try :
            idTweet = tweet.metadata["tweet_id"]
            idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
            length[idTweet] = 0
        except :
            pass

        # pour compter les articles contractés comme un seul token
        notConsider = [] 

        for token in tweet : 
            if type(token["id"])==tuple : 
                notConsider.append(token["id"][0])
                notConsider.append(token["id"][2])
            if token["id"] not in notConsider : 
                length[idTweet]+=1
                
logger.info("Récupération de la longueur de chaque tweet terminée")

# sauvegarde pour vérifications
out = open("./lengthAllTweets", "w")
out.write(str(length))
out.close()

logger.info("%i tweets au total"%len(length))

logger.info("...Récupération des tweets par utilisateur...")

# récupération des tweets par utilisateur
conn = sqlite3.connect("/warehouse/COMPLEXNET/ltarrade/negations/infosNeg.db")
cursor = conn.cursor()
cursor.execute("select * from tweetsByUser")
result = cursor.fetchall()
conn.close()

tweetsByUser = {}
nbTweets = 0
print("Récupération des tweets par utilisateur \n")
for i,u in enumerate(result) :
    if (i)%1000 == 0 : 
        sys.stdout.write("\r"+str(i))
    user = u[0]
    tweets = u[1]
    tweets = tweets.replace("'", "")
    tweets = tweets.replace(" ", "")
    tweets = tweets.replace("[", "")
    tweets = tweets.replace("]", "")
    tweets = tweets.split(",")
    nbTweets+=len(tweets)
    tweetsByUser[user]=tweets

logger.info("Récupération des tweets par utilisateur terminée")
logger.info("%i tweets pour %i utilisateurs"%(nbTweets,len(tweetsByUser)))

logger.info("...Ajout de la longueur moyenne des tweets par utilisateur dans le dataframe...")

df = pd.read_csv("./infosUser_1.csv", sep=";", index_col=0)

new_df = df.copy()

new_df.index = new_df.index.astype("str")
df.index = df.index.astype("str")

new_df["average_length_tweets"]=0.0
i=0 

print("Ajout de la longueur moyenne des tweets dans le dataframe\n")
for user,row in df.iterrows():
    i+=1
    if i%100 == 0 :
        sys.stdout.write("\r"+str(i))
    if user in tweetsByUser : 
        nbTotalToken = 0
        tweets = tweetsByUser[user] 
        for tweet in tweets : 
            nbTotalToken+=length[tweet]
        moyenne = nbTotalToken/len(tweets)
        new_df.loc[new_df.index==user,'average_length_tweets']=moyenne

logger.info("Ajout de la longueur moyenne des tweets par utilisateur dans le dataframe terminé.")

logger.info("Sauvegarde dans un nouveau dataframe 'infosUser_2.csv'")

new_df.to_csv("./infosUser_2.csv", sep=";")

logger.info("Terminé")

