# coding: utf-8

from multiprocessing import Pool
from conllu import parse_incr
import pandas as pd
import sqlite3
import logging
import inspect
import glob
import sys
import os
import re

path = "/warehouse/COMPLEXNET/ltarrade/parsed_tweets/"
files = glob.glob(path+"*conllu")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("Dossier d'entrée : %s"%path)
logger.info("%i fichier à passer en revue"%len(files))

# Récupération du lexique
logger.info("...Récupération du lexique...")

file = open("./lefff-3.4.mlex")

# dans dico pour rapidité ensuite
lexique = {}

for i,line in enumerate(file) : 
    if i%1000 == 0 :
        sys.stdout.write("\r"+str(i+1))
    form = re.match(r"^(.+?)\t.+?$", line.rstrip()).group(1)
    form = form.lower()
    lexique[form]=""
  
file.close()

# Récupération pour chaque tweet de sa liste de formes 
logger.info("...Récupération des formes de chaque tweet...")

def recup_forms(file) : 
    
    forms = {}

    fileName = os.path.basename(file)

    logger.info("Traitement du fichier %s"%fileName)
    
    file = open(file, "r")
    sentences = parse_incr(file)

    for tweet in sentences :

        # Récupération de l'id du tweet
        try :
            idTweet = tweet.metadata["tweet_id"]
            idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
            forms[idTweet] = []
        except :
            pass

        # pour compter les articles contractés comme un seul token
        notConsider = [] 
        
        for token in tweet : 
            if type(token["id"])==tuple : 
                notConsider.append(token["id"][0])
                notConsider.append(token["id"][2])
            if token["id"] not in notConsider :
                # on ne conserve que les formes qui ne sont ni des hashtags, ni des mentions, ni des urls, et qui ne contiennent pas uniquement des caractères non alpha-numériques (permet de ne pas inclure les emojis et émoticônes entre autres (exclut aussi la ponctuation du coup))
                form = token["form"].lower()
                if not re.match(r"^\W+$", form) and not re.match(r"^[#@].+?$", form) and not re.match("^https?://.+?", form) : 
                    forms[idTweet].append(form)
                
    return forms

try :
    pool = Pool(processes=None)
    allFormsByTweet = pool.map(recup_forms, files)
finally:
    pool.close()
    pool.join()
    
vocabulary = {}
for dic in allFormsByTweet : 
    vocabulary.update(dic)

logger.info("Récupération des formes terminée")

logger.info("%i tweets au total"%len(vocabulary))

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

# pour vérif 
try : 
    out = open("verif.txt", "w") 
    for tweet in tweetsByUser["2447106622"] : 
        out.write(vocabulary[tweet]+"\n")
    out.close()
except Exception as e : 
    print("pas possible de sauvegarder dans un fichier --> "+str(e))

logger.info("...Ajout de la richesse du vocabulaire par utilisateur dans le dataframe...")

df = pd.read_csv("./infosUser_2.csv", sep=";", index_col=0)

new_df = df.copy()

new_df.index = new_df.index.astype("str")
df.index = df.index.astype("str")

new_df["vocabulary_diversity"] = None
new_df["stdForm_rate"] = None
i=0 

print("\nAjout de la diversité du vocabulaire et du taux de formes standard pour chaque utilisateur dans le dataframe\n")

for user,row in df.iterrows():

    standardForm_rate=0.0
    nbStandardForm=0
    score=0.0

    i+=1

    if i%100 == 0 :
        sys.stdout.write("\r"+str(i))
    if user in tweetsByUser : 
        tweets = tweetsByUser[user] 
        forms = []
        for tweet in tweets : 
            forms+=vocabulary[tweet]
        if len(forms)!=0 : 
            nbForms = len(forms)
            uniqForms = set(forms)
            nbUniqForms = len(uniqForms)
            
            # calcul et ajout du "score" de diversité du vocabulaire dans le dataframe            
            forms_subLists = []
            #for j in range(0,len(forms),500) : 
            for j in range(0,len(forms),100) :
                if j+500<=len(forms) :
                    new_list = forms[j:j+500]
                    forms_subLists.append(new_list) 
                    
            scores_subLists = []
            
            for subList in forms_subLists : 
                nbForms_subList = len(subList)
                nbUniqForms_subList = len(set(subList))
                score_subList = nbUniqForms_subList/nbForms_subList
                scores_subLists.append(score_subList)
                
            score = np.mean(scores_subLists)
            new_df.loc[new_df.index==user,'vocabulary_diversity']=score

            # calcul et ajout du taux de formes standards sur l'ensemble des formes uniques (excepté hashtags, mentions, et suites de caractères exclusivement non alpha-numériques)
            for form in uniqForms : 
                if form in lexique : 
                    nbStandardForm += 1
            
            standardForm_rate = nbStandardForm/len(uniqForms)
            new_df.loc[new_df.index==user,'stdForm_rate']=standardForm_rate

logger.info("Ajout de diversité du vocabulaire pour chaque utilisateur dans le dataframe terminé.")

logger.info("Sauvegarde dans un nouveau dataframe 'infosUser_3.csv'")

new_df.to_csv("./infosUser_3.csv", sep=";")

logger.info("Terminé")

