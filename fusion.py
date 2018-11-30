# -*- coding: utf-8 -*-

from conllu import parse_incr
import argparse
import logging
import ujson
import sys
import re
import os

parser = argparse.ArgumentParser(description="merges CONLLU anntotations with the JSON corpus")
parser.add_argument('--jsonCorpus', '-j', required=True, help='path to the corpus file in the JSON format')
parser.add_argument('--conlluCorpus', '-c', required=True, help='path to the corpus directory in the CONLLU format')
args = parser.parse_args()

conlluCorpus = args.conlluCorpus
jsonCorpus = args.jsonCorpus

files = os.listdir(conlluCorpus)

fileJSON = open(jsonCorpus,"r")
newFile = open("newCorpus.json","a")

logging.basicConfig(filename='fusion.log',level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s")

tweets={}
for line in fileJSON :
    tweet = ujson.loads(line)
    tweets.update({tweet["id"]:tweet})

fileJSON.close()

i=0
for file in files :
    if (re.match(r".+?conllu$",file)) :
        i+=1
        sys.stdout.write("\rTraitement du fichier "+file)
        if (conlluCorpus[-1]!="/") :
            conlluCorpus+="/"
        fileCONLL = open(conlluCorpus+file,"r")
        sentences=parse_incr(fileCONLL)
        try :
            for tweet in sentences :
                m = re.search(r'.*(\d{18}).*', tweet.metadata["tweet_id"])
                id = m.group(1)
                if (id in tweets and "melt" in tweets[id]) :
                    tweet = ujson.dumps(tweet)
                    del tweets[id]["melt"]
                    tweets[id]["annotations"] = ujson.loads(tweet)
                    newFile.write(ujson.dumps(tweets[id])+"\n")
            logging.info("%s traité" % file)
        except Exception as e :
            logging.debug("erreur dans le fichier %s, id du tweet précédent : %s" % (file,str(id)))
            logging.debug(str(e))
            break
        fileCONLL.close()

newFile.close()
