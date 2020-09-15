# coding: utf-8

import pandas as pd
import numpy as np
import random


# # On récupère d'abord l'ensemble des individus de notre population
population = pd.read_csv("./dfValidUsersMore10Tweets.csv", index_col=0)

# ici c'est juste pour vérifier qu'on a bien le bon nombre de colonnes et de lignes du dataframe
#print(population.shape)


# # Échantillon stratifié

# ici on définit les tranches que l'on va utiliser pour degree
ii_degree = pd.IntervalIndex.from_tuples([(0,10),(10,20),(20,50),(50,100),(100,200),(200,1000),(1000,20000)])

# #### À partir de la population, on crée un tableau "combinedCriteria" à n dimensions (1 dimension par critère)
# #### Chaque critère est découpé en n bins
combinedCriteria = population.groupby([pd.cut(population.lat,53), pd.cut(population.long,53), pd.cut(population.income,18), pd.cut(population.density,100), pd.cut(population.degree,ii_degree)]).groups

# #### On trie le tableau obtenu de la case contenant le moins d'individus à celle contenant le plus d'individus
combinedCriteria_sorted = sorted(combinedCriteria.items(), key=lambda x: len(x[1]))


# #### Pour chacune des cases du tableau, on pioche un taux d'individus équivalent à celui présent dans la population, selon le nombre d'individus voulu dans l'échantillon
# cf. echantillon_explications.pdf dans le mail contenant l'échantillon pour les explications sur la façon dont est fait le tirage
combinedCriteria_echantillon = []

taille_population = 0
excedent = 0
ech = 0

for e in combinedCriteria_sorted : 
    taille_population+=len(e[1])

print("Taille population : "+str(taille_population))
taille_ech = 1000

for i,cc in enumerate(combinedCriteria_sorted) : 
    liste = cc[1].tolist()
    e = len(liste)
    if i+1==len(combinedCriteria_sorted) :
        nb_tirage = taille_ech-ech
        excedent = 0
    else : 
        taux = e/taille_population
        nbAleatoire = random.random()
        nb_echant = taux*taille_ech
        nb_echantExc = nb_echant+excedent
        if nb_echantExc < 0 : 
            nb_tirage = 0 
            excedent = nb_echantExc
        else : 
            nb_entier = int(nb_echantExc)
            prob = nb_echantExc%1
            if nbAleatoire<=prob : 
                nb_tirage = nb_entier+1
                excedent = -(1-prob)
            else : 
                nb_tirage = nb_entier
                excedent = prob
    
    combinedCriteria_echantillon.append((cc[0], random.sample(liste,nb_tirage)))
    
    ech+=nb_tirage

print("Taille échantillon : "+str(ech))

echantillon = []
for cce in combinedCriteria_echantillon :
    echantillon.extend(cce[1])


# #### on récupère les informations relatives aux individus sélectionnés dans l'échantillon 
echantillon_df = population[population.index.isin(echantillon)]

print(echantillon_df)

# si on veut enregistrer le résultat dans un csv, décommenter ligne ci-dessous
#echantillon_df.to_csv("./échantillons_générés/echantillon_stratifie.csv")
