# coding: utf-8
from conllu import parse_incr
import unicodedata
import datetime
import argparse
import logging
import inspect
import glob
import sys
import os
import re

# Suppression des signes diacrtiques et de la casse des caractères
def noDiacCase(f):
    f = f.lower()
    f = unicodedata.normalize('NFD', f).encode('ASCII', 'ignore').decode("utf-8")
    return f

# args
parser = argparse.ArgumentParser(description = "Récupération des négations")
parser.add_argument("--file", "-f", required=True, help="fichier à traiter")
args = parser.parse_args()

path = "/warehouse/COMPLEXNET/ltarrade/parsed_tweets/"
file = path+args.file
fileName = os.path.basename(file)

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./all_tweetsPrio/log/'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'_'+fileName+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

logger.info("récupération des négations dans le fichier %s...", fileName)
t0=datetime.datetime.now()

file = open(file, "r")
sentences = parse_incr(file)

ne = ["ne", "n'", "n’"]
negWord = ["pas", "pa", "aps", "jamais", "jms", "personne", "rien", "ri1", "r1", "aucun", "aucune", "aucuns", "aucunes"]
neg = {}

for tweet in sentences :

    # Récupération de l'id du tweet
    try :
        idTweet = tweet.metadata["tweet_id"]
        idTweet = re.match(r'"id": "(.+?)$', idTweet).group(1)
        neg[idTweet] = {"nbNeg":0, "nbDoubleNeg":0}
    except :
        pass

    soustrairePourIndice = -1

    standardNotConsidered = 0
    negStandard = 0

    for token in tweet :

        toExlude = False

        form = noDiacCase(token["form"])

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

                if form=="personne" :

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
                neg[idTweet]["nbNeg"]+=1
                standard = False
                # on regarde s'il y a un "ne" dans une fenêtre de +4,+3 autour du mot de négation
                if form != ["pas", "pa", "aps"] :
                    for t in tweet[indiceInTweet+1:indiceEnd] :
                        if t["form"].lower() in ne :
                            neg[idTweet]["nbDoubleNeg"]+=1
                            standard = True
                            break
                if not standard :
                    for t in tweet[indiceBegin:indiceInTweet] :
                        if t["form"].lower() in ne :
                            neg[idTweet]["nbDoubleNeg"]+=1
                            break


fileOut = open("all_tweetsPrio/résultats_neg_"+fileName+".tsv","w")
fileOut.write("ID\tnbNeg\n")
for tweet in neg :
    fileOut.write(tweet+"\t"+str(neg[tweet]["nbNeg"])+"\n")
fileOut.close()

fileOut = open("all_tweetsPrio/résultats_std_"+fileName+".tsv","w")
fileOut.write("ID\tnbStd\n")
for tweet in neg :
    fileOut.write(tweet+"\t"+str(neg[tweet]["nbDoubleNeg"])+"\n")
fileOut.close()

nbNeg=0
nbNegSTD=0
for tweet in neg :
    nbNeg = nbNeg+neg[tweet]["nbNeg"]
    nbNegSTD = nbNegSTD+neg[tweet]["nbDoubleNeg"]

t1=datetime.datetime.now()

logger.info("Récupération des négations terminée pour le fichier %s en %f : %d négations, dont %d négations standard", fileName, (t1-t0).total_seconds(), nbNeg, nbNegSTD)
