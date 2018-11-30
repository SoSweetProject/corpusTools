# -*- coding: utf-8 -*-

from os.path import basename, splitext
import subprocess
import argparse
import os

parser = argparse.ArgumentParser(description="imports corpus in vrt format into cqp")
parser.add_argument('--corpus', '-c', required=True, help='path to the corpus repertory in vrt format')
parser.add_argument('--data', '-d', required=True, help='path to data repertory of cqp')
parser.add_argument('--registry', '-r', required=True, help='path to registry repertory of cqp')
args = parser.parse_args()

pathToCorpus = args.corpus
pathToDataRep = args.data
pathToRegistryRep = args.registry
currentPath = os.getcwd()

for corpus in os.listdir(pathToCorpus) :
    name, ext = os.path.splitext(corpus)
    if (ext==".vrt") :

        if (pathToDataRep[-1]!="/") :
            pathToDataRep+="/"
        if (pathToRegistryRep[-1]!="/") :
            pathToRegistryRep+="/"

        os.chdir(pathToCorpus)

        outputFile = open(currentPath+"/output/"+name+".txt","a")

        subprocess.Popen("mkdir "+pathToDataRep+name,shell=True,stdout=subprocess.PIPE)

        #1
        proc = subprocess.Popen("cwb-encode -d "+pathToDataRep+name+" -c utf8 -f ./"+corpus+" -R "+pathToRegistryRep+name+" -P pos -P lemma -S text:0+id+user+date+geo",shell=True,stdout=subprocess.PIPE)
        out = proc.communicate()
        result = out[0].decode("utf8")
        outputFile.write(result+"\n")

        #2
        proc = subprocess.Popen("cwb-makeall -r "+pathToRegistryRep+" -V "+name,shell=True,stdout=subprocess.PIPE)
        out = proc.communicate()
        result = out[0].decode("utf8")
        outputFile.write(result+"\n")

        #3
        proc = subprocess.Popen("cwb-huffcode -r "+pathToRegistryRep+" -A "+name,shell=True,stdout=subprocess.PIPE)
        out = proc.communicate()
        result = out[0].decode("utf8")
        outputFile.write(result+"\n")

        #4
        os.chdir(pathToDataRep+name+"/")

        #5
        subprocess.Popen("rm *.corpus",shell=True,stdout=subprocess.PIPE)

        #6
        proc = subprocess.Popen("cwb-compress-rdx -r "+pathToRegistryRep+" -A "+name,shell=True,stdout=subprocess.PIPE)
        out = proc.communicate()
        result = out[0].decode("utf8")
        outputFile.write(result+"\n")

        #7
        subprocess.Popen("rm *.corpus.rev",shell=True,stdout=subprocess.PIPE)

        #8
        subprocess.Popen("rm *.corpus.rdx",shell=True,stdout=subprocess.PIPE)
