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

# Récupération pour chaque tweet de ses pluriels
logger.info("...Récupération des pluriels de chaque tweet...")

# unités de mesures abrégées
abrUnit = {"eme", "ème", "°c", "°f", "k", "°ré", "°n", "°ra", "m³", "dm³", "cm³", "l", "dl", "cl", "ml", "fl", "in³", "ft³", "yd³", "gal", "bbl", "pt", "km", "m", "dm", "cm", "mm", "mi", "in", "ft", "yd", "t", "kg", "hg", "g", "dg", "cg", "mg", "µg", "carat", "oz", "lb", "cwt", "st", "km²", "m²", "dm²", "cm²", "mm²", "ha", "a", "ca", "mille²", "in²", "yd²", "ft²", "ro", "acro", "kmph", "mps", "mph", "knot", "ma", "b", "o", "ko", "mo", "go", "to", "po", "eo", "zo", "yo", "min", "s", "ms", "µs", "hz", "khz", "mhz", "ghz", "atm", "bar", "mbar", "pa", "hpa", "psi", "j", "kj", "cal", "kcal", "wh", "kwh", "btu", "thm", "ft-lb", "grad", "mil", "rad", "h"}

def recup_pluriels(file) : 

    plurals = {}
    
    fileName = os.path.basename(file)
    logger.info("Traitement du fichier %s"%fileName)

    for i,tweet in enumerate(parse_incr(open(file))):
        
        # On récupère l'id du tweet
        try :
            idTweet = tweet.metadata["tweet_id"]
            idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
            plurals[idTweet]={'plural':0, 'nonstandard':0}
        except :
            pass

        for j,token in enumerate(tweet):
            
            nDet=""
            
            # On recherche les déterminants et on récupère leur nombre s'il est indiqué dans ses traits
            if token['xpostag']=='DET': 
                nDet = token['feats']['n'] if token['feats'] and 'n' in token['feats'] else '?'
                form = token["form"].lower()
                    
                # On vérifie que le déterminant est un déterminant pluriel, on exclut quelques exceptions, on vérifie qu'il n'y a que des caractères alphanumériques, exception faite des nombres (pour les déterminants numéraux cardinaux) 
                if nDet=="p" and form not in {'de','votre', 'leur', 'notre'} and (form.isalpha() or re.match("^\d+$", form)) :
                    noun = ""
                    nounForm = ""
                    nNoun = ""
                    
                    # On recherche ensuite le mot auquel le déterminant est rattaché (sa tête)
                    for token2 in tweet[j+1:] : 
                        if token2['id']==token['head']:
                            noun = token2
                            nounForm = noun["form"].lower()
                            break
                    
                    # On vérifie que le déterminant est bien relié à un mot, et que ce mot n'est ni une suite de caractères exclusivement non alpha-numériques (%,$,€,%,etc.), ni une mention, ni une url, ni une unité de mesure abrégée, et qu'il ne s'agit pas d'un nom propre
                    if noun!= "" and not re.match(r"^\W+$", nounForm) and not re.match(r"^@.+?$", nounForm) and not re.match("^https?://.+?", nounForm) and nounForm not in abrUnit and noun["xpostag"]!="NPP" : 
                        
                        card = False
                        
                        # On exclut les cardinaux 
                        if noun['feats'] and 's' in noun['feats'] and noun["feats"]["s"]=="card" : 
                            card = True 
                                                   
                        if not card : 
                            
                            # On récupère le nombre du mot si c'est indiqué
                            nNoun=noun['feats']['n'] if noun['feats'] and 'n' in noun['feats'] else '?'
                            
                            # On comptabilise un pluriel ici, après l'ensemble des filtres
                            plurals[idTweet]["plural"]+=1
                            
                            # Si le mot n'est pas au pluriel, on vérifie si il se termine par un s ou un x, et si ce n'est pas le cas, on considère qu'il s'agit d'un pluriel non standard
                            if nNoun!="p" : 
                                if nounForm[-1] not in {'s','x'} : 
                                    plurals[idTweet]["nonstandard"]+=1 
                                    
    return plurals

try :
    pool = Pool(processes=None)
    allPluralsByTweet = pool.map(recup_pluriels, files)
finally:
    pool.close()
    pool.join()

allPlurals = {}
for dic in allPluralsByTweet : 
    allPlurals.update(dic)

logger.info("Récupération des pluriels terminée")
logger.info("%i tweets au total"%len(allPlurals))

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

logger.info("...Ajout du taux de pluriel par utilisateur dans le dataframe...")

df = pd.read_csv("./infosUsers_complet.csv", sep=";", index_col=0)

new_df = df.copy()

new_df.index = new_df.index.astype("str")
df.index = df.index.astype("str")

new_df["incorrectPlurals_rate"] = None
i=0 

for user,row in df.iterrows():
    
    incorrectPlurals_rate=0.0

    i+=1

    if i%100 == 0 :
        sys.stdout.write("\r"+str(i))
    if user in tweetsByUser : 
        tweets = tweetsByUser[user] 
        plurals = {"nbPlurals" : 0, "nbIncorrectPlurals" : 0}
        for tweet in tweets : 
            plurals["nbPlurals"]+=allPlurals[tweet]["plural"]
            plurals["nbIncorrectPlurals"]+=allPlurals[tweet]["nonstandard"]
        if plurals["nbPlurals"]!=0 : 
            incorrectPlurals_rate=plurals["nbIncorrectPlurals"]/plurals["nbPlurals"]
            new_df.loc[new_df.index==user,'incorrectPlurals_rate']=incorrectPlurals_rate
        else : 
            new_df.loc[new_df.index==user,'incorrectPlurals_rate']=None

logger.info("Ajout du taux de pluriels incorrects pour chaque utilisateur dans le dataframe terminé.")

logger.info("Sauvegarde dans un nouveau dataframe 'infosUsers_complet_incorretPluralsRate.csv'")

new_df.to_csv("./infosUsers_complet_incorretPluralsRate.csv", sep=";")

logger.info("Terminé")


# In[107]:


tweetsByUser


# In[ ]:




