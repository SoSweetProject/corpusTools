# coding: utf-8

from multiprocessing import Pool
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

logger.info("...Récupération de la longueur de chaque tweet...")
print("Récupération de la longueur de chaque tweet\n")

def recup_length(file) : 
    
    length={}

    fileName = os.path.basename(file)
    
    logger.info("Traitement du fichier %s"%fileName)
    
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
                form = token["form"].lower()
                # length[idTweet]+=1 ici et suppression des lignes ci-dessous si récupération de l'ensemble des formes
                if not re.match(r"^\W+$", form) and not re.match(r"^[#@].+?$", form) and not re.match("^https?://.+?", form) : 
                    length[idTweet]+=1
                
    return length

try :
    pool = Pool(processes=None)
    allLengthByTweet = pool.map(recup_length, files)
finally:
    pool.close()
    pool.join()

allLength = {}
for dic in allLengthByTweet : 
    allLength.update(dic)

logger.info("Récupération de la longueur de chaque tweet terminée")

# sauvegarde pour vérifications
out = open("./lengthAllTweets_triForm", "w")
out.write(str(allLength))
out.close()

logger.info("%i tweets au total"%len(allLength))

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

df = pd.read_csv("./infosUser_2.csv", sep=";", index_col=0)

new_df = df.copy()

new_df.index = new_df.index.astype("str")
df.index = df.index.astype("str")

# ou new_df["average_length_tweets"]=0.0 si c'est sur l'ensemble des formes
new_df["average_length_tweets_triForm"]=0.0
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
            nbTotalToken+=allLength[tweet]
        moyenne = nbTotalToken/len(tweets)
        # ou new_df.loc[new_df.index==user,'average_length_tweets']=moyenne si c'est sur l'ensemble des formes
        new_df.loc[new_df.index==user,'average_length_tweets_triForm']=moyenne

logger.info("Ajout de la longueur moyenne des tweets par utilisateur dans le dataframe terminé.")

logger.info("Sauvegarde dans un nouveau dataframe 'infosUser_2_triForm.csv'")

new_df.to_csv("./infosUser_2_triForm.csv", sep=";")

logger.info("Terminé")

