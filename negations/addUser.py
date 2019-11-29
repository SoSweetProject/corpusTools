# coding: utf-8

import pandas as pd 
import sqlite3
import ast
import sys

# récupération des tweets par utilisateur
conn = sqlite3.connect("/Users/ltarrade/Desktop/Louise/SoSweet/négations/bdd/old_2019-07-22/infosNeg.db")
cursor = conn.cursor()
cursor.execute("select * from tweetsByUser")
result = cursor.fetchall()
conn.close()

# récupération du fichier csv contenant les détails de chaque négation 
df = pd.read_csv("./negByTweet_details.csv", sep="\t")
df["user"]=""

# transformation de la liste des tweets en liste (et plus en str) -> + rapide comme ça
dic = {}
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
    dic[user]=tweets

# dicTweets = pour chaque tweet son utilisateur associé
dicTweets={}
for i,user in enumerate(dic) : 
    if i%1000 == 0 :
        sys.stdout.write("\r"+str(i))
    for tweet in dic[user] : 
        dicTweets[tweet]=user


# on transforme le dataframe en dicitonnaire (manip + plus rapide)
dicComplet = df.to_dict()


# on associe l'utilisateur correspondant à chaque négation 
for i,e in enumerate(dicComplet["tweet"]):
    if i%1000 == 0 : 
        sys.stdout.write("\r"+str(i))
    dicComplet["user"][e]=dicTweets[str(dicComplet["tweet"][e])]

newDf = pd.DataFrame.from_dict(dicComplet)

newDf.to_csv("negByTweet_detailsAndUser_2.csv", sep="\t", index=False)
