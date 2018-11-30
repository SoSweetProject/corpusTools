# -*- coding: utf-8 -*-

from collections import defaultdict
import geopandas as gpd
import pandas as pd
import numpy as np
import datetime
import argparse
import shapely
import ujson
import time
import sys
import re
import os

parser = argparse.ArgumentParser(description="convert the JSON corpus to VRT format, by department")
parser.add_argument('--corpus', '-c', required=True, help='path to the corpus file in the JSON format')
parser.add_argument('--jsonCorpusByDep', '-j', required=True, help='path to the corpus directory in JSON format divided by department')
parser.add_argument('--vrtCorpusByDep', '-v', required=True, help='path to the corpus directory in VRT format divided by department')

args = parser.parse_args()

jsonCorpusByDep=args.jsonCorpusByDep
vrtCorpusByDep=args.vrtCorpusByDep
corpus=args.corpus

data=[]
departements=gpd.read_file("./departments.geojson")

# création d'un fichier csv contenant les tweets et leur département associé
with open(corpus) as f:
    for i,l in enumerate(f):
        t=ujson.loads(l)
        data.append({'geometry':shapely.geometry.Point(t['geo']['longitude'],t['geo']['latitude']), 'tweet':ujson.dumps(t)})
allTweets = gpd.GeoDataFrame(data)
allTweets = allTweets.where(pd.notnull(allTweets), None)
allTweets.crs=departements.crs
allTweets_with_departments = gpd.sjoin(allTweets,departements, how="inner", op='intersects')
allTweets_with_departments.to_csv('tweets_with_departments_newCorpus.csv')


# répartition des tweets par département (un fichier par département) au format json
if (jsonCorpusByDep[-1]!="/") :
    jsonCorpusByDep+="/"

tweets_df = pd.read_csv("./tweets_with_departments_newCorpus.csv")

dep = 1
while (dep<=19) :
    file = open(jsonCorpusByDep+"dep_"+str(dep)+".json","w")
    tweets = tweets_df.loc[(tweets_df["code_insee"]==dep) | (tweets_df["code_insee"]==str(dep)) | (tweets_df["code_insee"]=="0"+str(dep)),"tweet"]
    for tweet in tweets :
        file.write(tweet+"\n")
    file.close()
    dep+=1

file = open(jsonCorpusByDep+"dep_2a.json","w")
tweets = tweets_df.loc[tweets_df["code_insee"]=="2A","tweet"]
for tweet in tweets :
    file.write(tweet+"\n")
file.close()

file = open(jsonCorpusByDep+"dep_2b.json","w")
tweets = tweets_df.loc[tweets_df["code_insee"]=="2B","tweet"]
for tweet in tweets :
    file.write(tweet+"\n")
file.close()

dep = 21
while (dep<=95) :
    file = open(jsonCorpusByDep+"dep_"+str(dep)+".json","w")
    tweets = tweets_df.loc[(tweets_df["code_insee"]==dep) | (tweets_df["code_insee"]==str(dep)) | (tweets_df["code_insee"]=="0"+str(dep)),"tweet"]
    for tweet in tweets :
        file.write(tweet+"\n")
    file.close()
    dep+=1

# répartition des tweets par département (un fichier par département) au format vrt
if (vrtCorpusByDep[-1]!="/") :
    vrtCorpusByDep+="/"

for file in os.listdir(jsonCorpusByDep) :
    if (os.path.splitext(file)[1] == ".json") :
        i = 0
        tweets = []
        nameFile = os.path.splitext(file)[0]
        jsonFile = open(jsonCorpusByDep+file,"r")
        vrtFile = open(vrtCorpusByDep+nameFile+".vrt","w")
        sys.stdout.write("Chargement du fichier \""+os.path.basename(file)+"\"\n")
        for line in jsonFile :
            tweets.append(ujson.loads(line))
        for tweet in tweets :
            i+=1
            percentage = int((i*100)/len(tweets))
            sys.stdout.write("\rÉcriture du fichier \""+nameFile+".vrt\" - "+str(percentage)+"%")
            geo = str(tweet["geo"]["latitude"])+", "+str(tweet["geo"]["longitude"])
            vrtFile.write("<text id=\""+str(tweet["id"])+"\" user=\""+str(tweet["user"]["id"])+"\" date=\""+str(tweet["date"])+"\" geo=\""+geo+"\">\n")
            for annotation in tweet["annotations"] :
                if (annotation["xpostag"] is None) :
                    vrtFile.write(annotation["form"]+"\t"+annotation["upostag"]+"\t"+annotation["lemma"]+"\n")
                else :
                    vrtFile.write(annotation["form"]+"\t"+annotation["xpostag"]+"\t"+annotation["lemma"]+"\n")
            vrtFile.write("</text>\n")
        sys.stdout.write("\rÉcriture du fichier \""+nameFile+".vrt\" - 100%\n")
        jsonFile.close()
        vrtFile.close()

# post-traitement : remplacer les chevrons hors métadonnées par leur code html
for element in os.listdir(vrtCorpusByDep) :
    if (os.path.splitext(element)[1]==".vrt") :
        print("traitement de \""+os.path.basename(element)+"\"")
        file = open(vrtCorpusByDep+os.path.basename(element),"r")
        lines = file.readlines()
        for index, line in enumerate(lines) :
            if (not re.search(r'^(<text)|(</text>)',line)) :
                lines[index] = lines[index].replace("<","&lt;")
                lines[index] = lines[index].replace(">","&gt;")
        file.close()
        file = open(vrtCorpusByDep+os.path.basename(element),"w")
        file.writelines(lines)
        file.close()
