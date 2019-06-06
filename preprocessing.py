# -*- coding: utf-8 -*-
import argparse
import tarfile
import logging
import inspect
import glob
import sys
import os
import re

logger = logging.getLogger(__name__)

# Fonction qui récupère les arguments
def parseArgs():
    parser = argparse.ArgumentParser(description='From the compressed or uncompressed (to be specified in args) tweets, concatenates the tweets into files by year, month or day, limiting to 100000 tweets per file.\nFiles names must be in "YY-MM-DDTHH.data" format.')
    parser.add_argument('--path_to_data', '-i', required=True, help='path to input compressed data files (Json format)')
    parser.add_argument('--path_to_output_data', '-o', required=True, help='path to output data files')
    parser.add_argument('--compressed', required=False, help='if the data are compressed', action='store_true', default=None)
    parser.add_argument('--no-compressed', required=False, help='if the data are not compressed', action="store_true", default=None)
    parser.add_argument('--yearsToConcatenate', '-Y', required=True, default=[""],  nargs='+', help='years that will be concatenated, Format : -y 2006 2007 2008')
    parser.add_argument('--concatenateByYear', '-y', required=False, default=[""],  nargs='+', help='years to be concatenated per year (default->by day). Format : -y 2006 2007 2008')
    parser.add_argument('--concatenateByMonth', '-m', required=False, default=[""],  nargs='+', help='years to be concatenated per month (default->by day). Format : -m 2006 2007 2008')
    parser.add_argument("--formatting", action="store_true", help="to formate the data", default=None)
    parser.add_argument("--clean", action="store_true", help="to replace next-line and no-break space characters by space in the data")
    args = parser.parse_args()
    if args.path_to_data == args.path_to_output_data :
        raise ValueError(
            "paths to input data files and output data files must be different")
    if args.clean and args.formatting is None:
        raise ValueError(
            "--formatting is required if --clean")
    if args.compressed is not None and args.no_compressed is not None :
        raise ValueError(
            "only one of both options is possible")
    if args.compressed is None and args.no_compressed is None :
        raise ValueError(
            "--compressed or --no-compressed is an obligatory arg")
    return args

# Fonction qui récupère les fichiers à traiter
def getFilesToTreat(path):
    files=[]
    if args.no_compressed :
        sys.stdout.write("looking for uncompressed files to treat\n")
        logger.info("...looking for uncompressed files to treat...")
        files = [f for f in glob.glob(path + '*.data') if (f.split('/')[-1].split('T')[0].split("-")[0] in args.yearsToConcatenate)]
        files.sort()
    else :
        sys.stdout.write("looking for compressed files to treat\n")
        logger.info("...looking for compressed files to treat...")
        files=[]
        for fileName in [f for f in glob.glob(path + '*.tgz') if (f.split('/')[-1].split('T')[0].split("-")[0] in args.yearsToConcatenate)]:
            logger.info("checking %s"%str(fileName))
            tf=tarfile.open(fileName)
            filesOK=[(n,tf) for n in tf.getnames() if (n.split('.')[-1] == "data" and n.split('.')[-2] != "retweets")]
            files+=filesOK
        files.sort()
    logger.info("%d files to treat"%len(files))
    return files

# Fonction qui concatène les fichiers par année, mois ou jour selon ce qui a été précisé dans les arguments (par défaut : par jour)
def concatenation(files) :

    concatenateByPrec = ""

    for i,file in enumerate(files):

        if (i+1)%10 == 0 :
            sys.stdout.write("\rConcatenation in progress - "+str(round(((i+1)*100)/len(files)))+"%")

        if args.no_compressed :
            fileIn = open(file, "r")
            name = os.path.basename(file)
        else :
            fileIn = file[1].extractfile(file[0])
            name = os.path.basename(file[0])

        date = re.match(r"(((\d{4})-\d{2})-\d{2})T\d{2}\.data",name)
        year = date.group(3)
        yearMonth = date.group(2)
        yearMonthDay = date.group(1)

        if year not in concatenateByYear and year not in concatenateByMonth :
            concatenateBy = yearMonthDay
        elif year in concatenateByMonth :
            concatenateBy = yearMonth
        elif year in concatenateByYear :
            concatenateBy = year
        else :
            logger.info("Le fichier %s ne semble pas être un fichier de tweets et ne sera donc pas traité"%file)

        # On remet les compteurs à 0 et on ouvre un nouveau fichier en écriture lorsqu'on change d'année, de mois, ou de jour selons les cas
        if concatenateBy!=concatenateByPrec :
            if i!=0 :
                logger.info("Concaténation terminée pour %s : %d tweets regroupés en %d fichier(s)"%(concatenateByPrec, nbTweets, nbFile))
            out = open(output+concatenateBy+".data", "a")
            nbFile=1
            nbFile_ext=""
            nbTweets=0

        logger.info("Traitement du fichier %s (%d/%d)"%(name, i+1, len(files)))

        out = open(output+concatenateBy+nbFile_ext+".data", "a")
        for line in fileIn :
            if args.no_compressed :
                out.write(line)
            else :
                out.write(line.decode("utf-8"))
            nbTweets+=1

            # Pour ne pas voir des fichiers trop longs à parser, on les limite à 100 000 tweets
            if nbTweets%100000==0 :
                nbFile+=1
                nbFile_ext="_"+str(nbFile)
                out.close()
                out = open(output+concatenateBy+nbFile_ext+".data", "a")

        if (i+1==len(files)) :
            logger.info("Concaténation terminée pour %s : %d tweets regroupés en %d fichier(s)"%(concatenateBy, nbTweets, nbFile))

        out.close()
        concatenateByPrec = concatenateBy

# Fonction qui met en forme les tweets pour qu'il y ait un ensemble clé/valeur par ligne
def formatting(path) :
     files = glob.glob(path+"20*data")
     files.sort()
     logger.info("...Mise en forme des données...")
     logger.info("%d fichiers à mettre en forme"%len(files))
     for i,file in enumerate(files) :
         if (i+1)%10 == 0 :
             sys.stdout.write("\rFormatting in progress - "+str(round(((i+1)*100)/len(files)))+"%")
         fileName = os.path.basename(file)
         logger.info("Mise en forme du fichier %s (%d/%d)"%(fileName, i+1, len(files)))
         os.system("jq -S . "+file+" > "+file+".formatted")

# Fonction qui remplace les caractères \u0085 (next-line) et \u00a0 (no-break space) par des espaces dans les données formattées
def clean(path) :
     files = glob.glob(path+"20*formatted")
     files.sort()
     logger.info("...Remplacement des caractères \\u0085 (next-line) et \\u00a0...")
     logger.info("%d fichiers à traiter"%len(files))
     for i,file in enumerate(files) :
         if (i+1)%10 == 0 :
             sys.stdout.write("\rCleaning in progress - "+str(round(((i+1)*100)/len(files)))+"%")
         fileName = os.path.basename(file)
         logger.info("Remplacement des caractères \\u0085 (next-line) et \\u00a0 du fichier %s (%d/%d)"%(fileName, i+1, len(files)))
         readFile = open(file, "r")
         outFile = open(file+".clean", "w")
         for line in readFile :
             if re.match(r'\s+"tweet":.*', line) :
                 line = re.sub(r"(| +  +| + |  +)", " ", line)
             outFile.write(line)
         outFile.close()
         readFile.close()

#log
logger.setLevel(logging.INFO)
handler = logging.FileHandler('./'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

# Récupération des arguments
args = parseArgs()

if args.path_to_data[-1] != '/':
    args.path_to_data += '/'
if args.path_to_output_data[-1] != '/':
    args.path_to_output_data += '/'

input = args.path_to_data
output = args.path_to_output_data
concatenateByYear = args.concatenateByYear
concatenateByMonth = args.concatenateByMonth

logger.info("input directory: %s"%input)
logger.info("output directory: %s"%output)
logger.info("year(s) concatenated by year : %s"%", ".join(concatenateByYear))
logger.info("year(s) concatenated by month : %s"%", ".join(concatenateByMonth))

# récupération des fichiers à traiter
files = getFilesToTreat(input)

# Concaténation des fichiers par année, mois ou jour (100 000 tweets max par fichier)
concatenation(files)
sys.stdout.write("\nConcatenation finished !\n")

# Mise en forme 1 entrée par ligne, les entrées sont triées par ordre alphabétique
if args.formatting :
    formatting(output)
    sys.stdout.write("\nFormatting finished !\n")

# Traitement de \u0085 et \u00a0 qui posent problème lors du parsing
if args.clean :
    clean(output)
    sys.stdout.write("\nCleaning finished !\n")

sys.stdout.write("Preprocessing finished !\n")
