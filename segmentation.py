# coding: utf-8
from multiprocessing import Pool
import argparse
import datetime
import logging
import inspect
import glob
import re
import os

# args
parser = argparse.ArgumentParser(description = "Segmentation des tweets")
parser.add_argument("--years", "-y", default=[""], nargs='+', help="Années à traiter (les mettre à la suite, en les séparant par un espace)")
args = parser.parse_args()

years = args.years

# log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+("_").join(str(y) for y in years)+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

# regex correspondant à chaque motif au niveau duquel on veut segmenter la phrase
s=open("emoticons.txt").read().strip()
regexEmoticons=re.compile('(?<!  "tweet": )(\S\s+)((' + '|'.join(s.split('\n')) + ')+)(\s+\w)')

regexPunct=re.compile('(?<!  "tweet": )(\S\s*)((\?|\.|\!)+)(\s+\w)')

s=open("emojis.txt").read().strip()
regexEmoji=re.compile('(?<!  "tweet": )(\S\s*)((' + '|'.join([re.escape(e) for e in s.split('\n')]) +'){1,10})(\s+\w)')

regexInterj=re.compile('(\w\s+)((\\bm+d+r+\\b|\\bp+t+d+r+\\b|\\bl+o+l+\\b)+)(\s+\w)')

regexMentions=re.compile('^(  "tweet": "(@.+?\s+)+)')
regexHashtags=re.compile('((#\S+\s*)+(https?://\S*\s*)*)$')

tweet=re.compile('^  "tweet": .*$')

def segmenting(regex, line) :
    newLine = line
    for e in regex.finditer(line) :
        pattern = re.escape(e.group(0))
        before = e.group(1)
        element = e.group(2)
        after = e.group(4)
        newLine = re.sub(pattern, before+element+"\\n"+after, newLine)
    return newLine

def segmentingMentions(regex, line) :
    newLine = line
    for e in regex.finditer(line) :
        pattern = re.escape(e.group(0))
        mentions = e.group(1)
        newLine = re.sub(pattern, mentions+"\\n", newLine)
    return(newLine)

def segmentingHashtags(regex, line) :
    newLine = line
    for e in regex.finditer(line) :
        pattern = re.escape(e.group(0))
        hashtag = e.group(1)
        newLine = re.sub(pattern, "\\n"+hashtag, newLine)
    return(newLine)

def treatFile(file) :
    name = os.path.basename(file)
    fileIn = open(file, "r")
    t0=datetime.datetime.now()
    logger.info("Traitement du fichier %s", name)
    out = open(output+name+".segmented", "w")
    for line in fileIn :
        if tweet.match(line) :
            # segmentation au niveau des émoticônes
            line = segmenting(regexEmoticons, line)
            # segmentation au niveau des émojis
            line = segmenting(regexEmoji, line)
            #segmentation au niveau des ponctuations
            line = segmenting(regexPunct, line)
            # segmentation au niveau des interjections (seulement lol, mdr et ptdr)
            line = segmenting(regexInterj, line)
            # segmentation au niveau des mentions de début de tweet
            line = segmentingMentions(regexMentions, line)
            # segmentation au niveau des hashtags de fin de tweet
            line = segmentingHashtags(regexHashtags, line)
            line = re.sub("\n", "\\\\n", line)
            line = line[:-2]
        out.write(line.rstrip()+"\n")
    t1=datetime.datetime.now()
    logger.info("Fichier %s traité en %f", name,(t1-t0).total_seconds())
    fileIn.close()
    out.close()

input = "/warehouse/COMPLEXNET/ltarrade/preprocessing/output/"
output = "./output/"

files = []
# Récupération des fichiers à traiter
for year in years :
    files += glob.glob(input+str(year)+"*clean")

files = sorted(files)

logger.info("input : %s", os.path.abspath(input))
logger.info("output : %s", os.path.abspath(output))
logger.info("%d fichiers à segmenter", len(files))

try :
    pool = Pool(4)
    pool.map(treatFile, files)
finally:
    pool.close()
    pool.join()
