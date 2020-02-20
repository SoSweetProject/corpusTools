# coding: utf-8

from multiprocessing import Pool 
from conllu import parse_incr
import pandas as pd
import logging
import inspect
import sqlite3
import glob
import sys
import re
import os


# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

path = "/warehouse/COMPLEXNET/ltarrade/parsed_tweets/"
files = glob.glob(path+"*conllu")

logger.info("Dossier d'entrée : %s"%path)
logger.info("%i fichier à passer en revue"%len(files))

dic_emoj_emot = {}
emoj_emot = []

fileEmo = open("emoticonsRegex_2.txt", encoding="utf-8")
for line in fileEmo : 
     dic_emoj_emot[line.rstrip()]=len(line.rstrip())
        
# on trie par longueur pour avoir les plus longs motifs au début de la regex, pour que les emojis composées de plusieurs éléments ne soient pas considérées comme plusieurs emojis
for emo in sorted(dic_emoj_emot, key = dic_emoj_emot.get, reverse=True) :
    emoj_emot.append(emo)
    
re_emojEmot=re.compile('|'.join(emoj_emot))


def recup_nbEmoParTweets(file) : 

    nbEmo = {}
    
    fileName = os.path.basename(file)
    logger.info("Traitement du fichier %s"%fileName)
    
    for i,tweet in enumerate(parse_incr(open(file))):
        
        # On récupère l'id du tweet
        try :
            idTweet = tweet.metadata["tweet_id"]
            idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
            nbEmo[idTweet]=0
        except :
            pass

        nbEmojt = 0
        
        tweetContent = (tweet.metadata["text"])
        
        # On récupère le nombre d'emoticônes et d'emojis dans le tweet
        emojt = re.findall(re_emojEmot,tweetContent)
        nbEmojt = len(emojt)
        
        nbEmo[idTweet]+=nbEmojt
                                    
    return nbEmo

try :
    pool = Pool(processes=None)
    allEmotjByTweet = pool.map(recup_nbEmoParTweets, files)
finally:
    pool.close()
    pool.join()

allEmotj = {}
for dic in allEmotjByTweet : 
    allEmotj.update(dic)


logger.info("Récupération des émoticônes/emojis terminée")
logger.info("%i tweets au total"%len(allEmotj))


logger.info("...Récupération des tweets par utilisateur...")

# récupération des tweets par utilisateur
conn = sqlite3.connect("/warehouse/COMPLEXNET/ltarrade/negations/infosNeg.db")
cursor = conn.cursor()
cursor.execute("select * from tweetsByUser")
result = cursor.fetchall()
conn.close()

tweetsByUser = {}
nbTweets = 0
print("\nRécupération des tweets par utilisateur \n")
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


logger.info("...Ajout du nombre moyen d'émoticônes/emojis par tweet pour chaque utilisateur dans le dataframe...")

df = pd.read_csv("./infosUsers_complet.csv", sep=";", index_col=0)

new_df = df.copy()

new_df.index = new_df.index.astype("str")
df.index = df.index.astype("str")

new_df["nbMoyen_emojt_byTweet"] = 0
i=0 

for user,row in df.iterrows():
    
    nbMoyen_emojt_byTweet=0.0
    nbEmojt = 0

    i+=1

    if i%100 == 0 :
        sys.stdout.write("\r"+str(i))
    if user in tweetsByUser : 
        tweets = tweetsByUser[user] 
        nbTweets = len(tweets)
        for tweet in tweets : 
            nbEmojt+=allEmotj[tweet]
                    
    if nbTweets==0 : 
        new_df.loc[new_df.index==user,'nbMoyen_emojt_byTweet']=None
    else :
        nbMoyen_emojt_byTweet = nbEmojt/nbTweets
        new_df.loc[new_df.index==user,'nbMoyen_emojt_byTweet']=nbMoyen_emojt_byTweet
    
logger.info("Ajout du nombre moyen d'émoticônes/emojis par tweet pour chaque utilisateur dans le dataframe terminé.")

logger.info("Sauvegarde dans un nouveau dataframe 'infosUser_nbEmojt.csv'")

new_df.to_csv("./infosUser_nbEmojt.csv", sep=";")

logger.info("Terminé")





