# coding: utf-8
from multiprocessing import Pool
from conllu import parse_incr
import pandas as pd
import unicodedata
import datetime
import argparse
import logging
import inspect
import glob
import sys
import os
import re

def noDiacCase(f):
    f = f.lower()
    f = unicodedata.normalize('NFD', f).encode('ASCII', 'ignore').decode("utf-8")
    return f

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

ne = ["ne", "n'", "n’"]
negWord = ["pas", "pa", "aps", "jamais", "jms", "personne", "rien", "ri1", "r1", "aucun", "aucune", "aucuns", "aucunes"]

files = sorted(glob.glob("/warehouse/COMPLEXNET/ltarrade/parsed_tweets/*conllu"))

logger.info("Nombre de fichiers à traiter : "+str(len(files)))

t0 = datetime.datetime.now()

def neg_recup(file) : 

    neg = {}
    
    fileName = os.path.basename(file)
    logger.info("Traitement du fichier "+fileName)
    file = open(file, "r")
    sentences = parse_incr(file)
    for tweet in sentences :

        # Récupération de l'id du tweet
        try :
            idTweet = tweet.metadata["tweet_id"]
            idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
            neg[idTweet] = []
        except :
            pass

        soustrairePourIndice = -1

        standardNotConsidered = 0
        negStandard = 0

        for token in tweet :

            toExlude = False

            form = noDiacCase(token["form"])
            formDetails = token

            # Pour ajuster l'indice si il y a eu des articles contractés avant
            if not isinstance(token["id"], int) :
                soustrairePourIndice+=1

            else :

                # Récupération de l'id du mot de négation
                indiceInTweet = token["id"]+soustrairePourIndice

                # ajustement des indices pour ne pas qu'ils "dépassent" ceux du tweet
                indiceBegin = indiceInTweet-4
                while indiceBegin<0 :
                    indiceBegin+=1
                indiceEnd = indiceInTweet+4
                while indiceEnd>=len(tweet) :
                    indiceEnd = indiceEnd-1

            # Si mot de négation
            if form in negWord :

                negInfos = {"motNeg":"", "std":False, "sujet":None}

                exception = False
                ellipse = False

                # Si le mot de négation est le premier mot de la "phrase"
                if indiceInTweet==0 :
                    if indiceEnd>=1 and tweet[1]["xpostag"]=="VINF" :
                        ellipse = False
                    elif form not in ["pas", "pa", "aps"] :
                        ellipse = True
                        for j,t in enumerate(tweet[indiceInTweet+1:indiceEnd+1]) :
                            if indiceEnd==4 and tweet[4]["upostag"]=="V" :
                                if tweet[1]["upostag"]=="CL" and tweet[2]["form"].lower() in ne and tweet[3]["upostag"]=="CL" :
                                    if form not in ["jamais", "jms"] and tweet[1]["xpostag"]=="CLS" :
                                        ellipse = True
                                    else :
                                        ellipse = False
                                elif tweet[1]["form"].lower() in ne and tweet[2]["upostag"]=="V" and tweet[3]["upostag"]=="CL" :
                                    ellipse = False
                            if indiceEnd>=3 and tweet[3]["upostag"]=="V" :
                                if tweet[1]["form"].lower() in ne and tweet[2]["xpostag"]=="V" and noDiacCase(tweet[2]["form"])!="a" and tweet[3]["xpostag"] in ["VINF", "VPP"] :
                                    ellipse = False
                                elif tweet[1]["form"].lower() in ne and tweet[2]["upostag"]=="CL" :
                                    if form not in ["jamais", "jms"] and tweet[1]["xpostag"]=="CLS" :
                                        ellipse = True
                                    else :
                                        ellipse = False
                                elif tweet[1]["upostag"]=="PRO" and tweet[2]["form"].lower() in ne :
                                    ellipse = False
                                elif tweet[1]["upostag"]=="V" and tweet[2]["upostag"]=="CL" :
                                    ellipse = False
                                elif tweet[1]["upostag"]=="CL" and tweet[2]["upostag"]=="CL" :
                                    if form not in ["jamais", "jms"] and tweet[1]["xpostag"]=="CLS" :
                                        ellipse = True
                                    else :
                                        ellipse = False
                            if indiceEnd>=2 and tweet[2]["upostag"]=="V" :
                                if tweet[1]["xpostag"]=="V" and noDiacCase(tweet[1]["form"])!="a" and tweet[2]["xpostag"] in ["VINF", "VPP"] :
                                    ellipse = False
                                elif tweet[1]["upostag"]=="CL" :
                                    if form not in ["jamais", "jms"] and tweet[1]["xpostag"]=="CLS" :
                                        ellipse = True
                                    else :
                                        ellipse = False
                                elif tweet[1]["xpostag"]=="PRO" :
                                    ellipse = False
                    else :
                        ellipse = True

                # Si le mot de négation n'est pas le premier mot de la "phrase"
                if indiceInTweet-1>=0 :

                    formPrec = noDiacCase(tweet[indiceInTweet-1]["form"])
                    posPrec = tweet[indiceInTweet-1]["xpostag"]

                    if formPrec in negWord :
                        exception = True

                    elif form=="personne" :

                        if posPrec=="DET" or formPrec in ["de", "en", "chaque", "ni", "sans"] :
                            exception = True

                        elif posPrec=="P" and formPrec=="a" :
                            for t in tweet[indiceBegin+1:indiceInTweet] :
                                if noDiacCase(t["form"]) in negWord :
                                    exception = True

                    elif form in ["rien", "ri1", "r1"] :

                        if posPrec in ["DET", "CC"] or formPrec in ["de", "pour", "ni", "sans"] :
                            exception = True

                        if indiceInTweet-2>=0 :
                            formPrecPrec = noDiacCase(tweet[indiceInTweet-2]["form"])
                            if formPrecPrec in ["moins", "mieux"] and formPrec=="que" :
                                exception = True

                    elif form in ["pas", "pa", "aps"] :

                        if formPrec in ["un", "1", "le", "les", "des", "pourquoi", "pk"] or posPrec=="CC" :
                            exception = True

                    elif form in ["jamais", "jms"] :

                        if formPrec in ["comme", "a", "du", "tout", "tt", "sans"] :
                            exception = True

                        if posPrec in ["CC", "CS"] :
                            if indiceInTweet+1<=len(tweet)-1 :
                                posSuiv = tweet[indiceInTweet+1]["xpostag"]
                                if posSuiv=="CLS" and formPrec!="si" :
                                    exception = False
                                else :
                                    exception = True
                            else :
                                exception = True

                    elif form in ["aucun", "aucune", "aucuns", "aucunes"] :
                        if formPrec in ["d'", "d’", "sans"] :
                            exception = True


                # Si le mot de négation n'est pas le dernier mot de la "phrase"
                if indiceInTweet+1<=len(tweet)-1 :
                    formSuiv = noDiacCase(tweet[indiceInTweet+1]["form"])
                    posPrec = tweet[indiceInTweet-1]["xpostag"]
                    if (form in ["pas", "pa", "aps"] and formSuiv in ["cher", "chers", "chere", "cheres", "longtemps", "lgt"]) :
                        exception = True
                        if indiceInTweet-1>=0 and posPrec in ["V", "CLS+CLO+V"] :
                            exception = False

                # S'il ne correspond à aucune exception et s'il ne s"agit pas en fait d'une ellipse, le mot de négation est considéré comme tel
                if not exception and not ellipse :
                    negInfos["motNeg"] = form
                    # Ici on regarde quel est la nature du sujet du verbe auquel la négation est rattachée
                    i = 0
                    candidats = {}
                    headIsAVerb = False
                    for tok in tweet : 
                        if tok["id"]==formDetails["head"] and tok["upostag"]=="V" : 
                            headIsAVerb = True
                        if tok["head"]==formDetails["head"] and tok["id"]!=formDetails["id"] and tok["deprel"]=="suj" and not re.match(r"^@.+?", tok["form"]): 
                            if tok["xpostag"]=="NC" :
                                dist = abs(tok["id"]-formDetails["head"])
                                candidats[dist]="GN"
                                i+=1
                            elif tok["xpostag"]=="PRO" or tok["xpostag"]=="CLS" : 
                                dist = abs(tok["id"]-formDetails["head"])
                                candidats[dist]="PRO|CLS"
                                i+=1
                    candidats_sorted = sorted(candidats.keys())
                    if not headIsAVerb : 
                        negInfos["sujet"]=None
                    elif len(candidats_sorted)==0 : 
                        negInfos["sujet"]="autre"
                    else : 
                        negInfos["sujet"]=candidats[candidats_sorted[0]]

                    standard = False

                    # on regarde s'il y a un "ne" dans une fenêtre de +4,+3 autour du mot de négation
                    if form not in ["pas", "pa", "aps"] :
                        for t in tweet[indiceInTweet+1:indiceEnd] :
                            if t["form"].lower() in ne :
                                negInfos["std"] = True
                                standard = True
                                # s'il s'agit de n'importe ou n'imp, la négation est décomptée des négations standard
                                if t["id"]+soustrairePourIndice<indiceEnd and tweet[t["id"]+soustrairePourIndice+1]["form"].lower() in ["importe", "imp"] : 
                                    negInfos["std"] = False
                                    standard = False
                                break
                    if not standard :
                        for t in tweet[indiceBegin:indiceInTweet] :
                            if t["form"].lower() in ne :
                                negInfos["std"] = True
                                standard = True
                                # s'il s'agit de n'importe ou n'imp, la négation est décomptée des négations standard
                                if t["id"]+soustrairePourIndice<indiceEnd and tweet[t["id"]+soustrairePourIndice+1]["form"].lower() in ["importe", "imp"] : 
                                    negInfos["std"] = False
                                    standard = False
                                break

                # On ajoute les informations récoltées sur la négation dans neg, à l'id du tweet dans lequel elle se trouve                
                if not exception and not ellipse :                 
                    neg[idTweet].append(negInfos)

    nbTotalNeg=0
    nbTotalDoubleNeg=0

    for tweet in neg :
        for n in neg[tweet] : 
            nbTotalNeg+=1
            if n["std"] :
                nbTotalDoubleNeg += 1

    logger.info(fileName+" traité : "+str(nbTotalNeg)+" négations, dont "+str(nbTotalDoubleNeg)+" négations standard")

    return neg

try :
    pool = Pool(processes=None)
    results = pool.map(neg_recup, files)
finally:
    pool.close()
    pool.join()

df = pd.DataFrame(columns=["tweet", "motNeg", "std", "sujet"])

for i,neg in enumerate(results) :  
    logger.info("Récupération des résulats ("+str(i+1)+"/"+str(len(results))+")")
    for tweet in neg :
        if len(neg[tweet])!=0 :  
            for dic in neg[tweet] : 
                df.loc[len(df)] = [tweet, dic["motNeg"], dic["std"], dic["sujet"]]

df.to_csv("./negByTweet_details.csv")

logger.info(str(len(df))+" négations au total")
logger.info(str(len(df[df["std"]==True]))+" négations standard au total")

logger.info("récupération des négations sur l'ensemble des fichiers terminée en "+str(datetime.datetime.now()-t0))
