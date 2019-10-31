# coding: utf-8

import pandas as pd
import numpy as np
import random


# # Population

population = pd.read_csv("../data/dfValidUsersMore10Tweets.csv", index_col=0)
population.shape


# # Echantillon Aléatoire

# #### Division en n bins pour chacun des critères à considérer pour constituer l'échantillon, et calcul du taux d'individus dans chaque bin
# --> sert pour la constitution de l'échantillon aléatoire, et pour la comparaison des échantillons

hist_population_density = list(np.histogram(population.density, bins=100))
hist_population_density[0]=hist_population_density[0]/hist_population_density[0].sum()

hist_population_income = list(np.histogram(population.income, bins=18))
hist_population_income[0]=hist_population_income[0]/hist_population_income[0].sum()

hist_population_degree = list(np.histogram(population.degree, bins=350))
hist_population_degree[0]=hist_population_degree[0]/hist_population_degree[0].sum()

hist_population_long = list(np.histogram(population.long, bins=53))
hist_population_long[0]=hist_population_long[0]/hist_population_long[0].sum()

hist_population_lat = list(np.histogram(population.lat, bins=53))
hist_population_lat[0]=hist_population_lat[0]/hist_population_lat[0].sum()


# #### Constitution des échantillons aléatoires, et calcul du chi2 entre chaque échantillon et la population pour chacun des critères ci-dessus

samples = {}
for i in range(1000) : 
    
    chi2s = {}
    
    sample = population.sample(1000)
    
    hist_sample_density = list(np.histogram(sample.density, bins=hist_population_density[1]))
    hist_sample_density[0] = hist_sample_density[0]/hist_sample_density[0].sum()
    chi2_density = ((hist_sample_density[0]-hist_population_density[0])**2/hist_population_density[0])
    chi2_density = np.nan_to_num(chi2_density)
    chi2_density = chi2_density.sum()
    
    hist_sample_income = list(np.histogram(sample.income, bins=hist_population_income[1]))
    hist_sample_income[0] = hist_sample_income[0]/hist_sample_income[0].sum()
    chi2_income = ((hist_sample_income[0]-hist_population_income[0])**2/hist_population_income[0])
    chi2_income = np.nan_to_num(chi2_income)
    chi2_income = chi2_income.sum()
    
    hist_sample_degree = list(np.histogram(sample.degree, bins=hist_population_degree[1]))
    hist_sample_degree[0] = hist_sample_degree[0]/hist_sample_degree[0].sum()
    chi2_degree = ((hist_sample_degree[0]-hist_population_degree[0])**2/hist_population_degree[0])
    chi2_degree = np.nan_to_num(chi2_degree)
    chi2_degree = chi2_degree.sum()
    
    hist_sample_long = list(np.histogram(sample.long, bins=hist_population_long[1]))
    hist_sample_long[0] = hist_sample_long[0]/hist_sample_long[0].sum()
    chi2_long = ((hist_sample_long[0]-hist_population_long[0])**2/hist_population_long[0])
    chi2_long = np.nan_to_num(chi2_long)
    chi2_long = chi2_long.sum()
    
    hist_sample_lat = list(np.histogram(sample.lat, bins=hist_population_lat[1]))
    hist_sample_lat[0] = hist_sample_lat[0]/hist_sample_lat[0].sum()
    chi2_lat = ((hist_sample_lat[0]-hist_population_lat[0])**2/hist_population_lat[0])
    chi2_lat = np.nan_to_num(chi2_lat)
    chi2_lat = chi2_lat.sum()
    
    chi2s = {"chi2_density":chi2_density,
             "chi2_income":chi2_income,
             "chi2_degree":chi2_degree,
             "chi2_long":chi2_long,
             "chi2_lat":chi2_lat}
    
    samples[i+1]=(sample,chi2s)


# #### Pour chacun des critères, on classe les échantillons selon leur chi2 (+ il est proche de 0, + il est proche de la population)

dic_density = {}
for sample in samples :
    dic_density[sample]=samples[sample][1]["chi2_density"]
density_sorted = sorted(dic_density.items(), key=lambda t: t[1])

dic_income = {}
for sample in samples :
    dic_income[sample]=samples[sample][1]["chi2_income"]
income_sorted = sorted(dic_income.items(), key=lambda t: t[1])

dic_degree = {}
for sample in samples :
    dic_degree[sample]=samples[sample][1]["chi2_degree"]
degree_sorted = sorted(dic_degree.items(), key=lambda t: t[1])

dic_long = {}
for sample in samples :
    dic_long[sample]=samples[sample][1]["chi2_long"]
long_sorted = sorted(dic_long.items(), key=lambda t: t[1])

dic_lat = {}
for sample in samples :
    dic_lat[sample]=samples[sample][1]["chi2_lat"]
lat_sorted = sorted(dic_lat.items(), key=lambda t: t[1])


# #### On récupère les n premiers de chaque critère, et on les transforme en dico pour la rapidité

density = density_sorted[0:200]
density_dic = {}
for e in density :
    density_dic[e[0]]=e[1]
    
income = income_sorted[0:200] 
income_dic = {}
for e in income :
    income_dic[e[0]]=e[1]
    
degree = income_sorted[0:200]
degree_dic = {}
for e in degree :
    degree_dic[e[0]]=e[1]
    
long = long_sorted[0:200]
long_dic = {}
for e in long :
    long_dic[e[0]]=e[1]

lat = lat_sorted[0:200]
lat_dic = {}
for e in lat :
    lat_dic[e[0]]=e[1]


# #### On regarde si un des échantillons est classé dans les n premiers de chaque critère

for e in density_dic : 
    if (e in income_dic) and (e in degree_dic) and (e in long_dic) and (e in lat_dic) : 
        echant_ok = e
        print(e)

samples[echant_ok][0].to_csv("./fichiers_echantillons/3_aleatoire_bins_chosen.csv")


# # Échantillon stratifié

# #### À partir de la population, on crée un tableau à n dimensions (1 dimension par critère)
# #### Chaque critère est découpé en n bins

#ii_income = pd.IntervalIndex.from_tuples([(0,12000),(12000,18000),(18000,24000),(24000,30000)])
#ii_degree = pd.IntervalIndex.from_tuples([(0,20),(20,50),(50,100),(100,200),(200,1000),(1000,20000)])
#ii_density = pd.IntervalIndex.from_tuples([(-0.001,0.1),(0.1,0.25),(0.25,0.50),(0.50,0.75),(0.75,1)])

#combinedCriteria = population.groupby([pd.cut(population.lat,11), pd.cut(population.long,11), pd.cut(population.income,ii_income), pd.cut(population.density,ii_density), pd.cut(population.degree,ii_degree)]).groups

combinedCriteria = population.groupby([pd.cut(population.lat,53), pd.cut(population.long,53), pd.cut(population.income,18), pd.cut(population.density,100), pd.cut(population.degree,350)]).groups


# #### On trie le tableau obtenu de la case contenant le moins d'individus à celle contenant le plus d'individus

combinedCriteria_sorted = sorted(combinedCriteria.items(), key=lambda x: len(x[1]))


# #### Pour chacune des cases du tableau, on pioche un taux d'individus équivalent à celui présent dans la population, selon le nombre d'individus voulus dans l'échantillon

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

echantillon_df.to_csv("./fichiers_echantillons/3_stratifie_bins_chosen.csv")


# # COMPARAISON DES ECHANTILLONS

# #### échantillon que l'on veut comparer à la population

sample = pd.read_csv("./fichiers_echantillons/1_stratifie_bins_diff.csv", index_col=0)


# #### on divise l'échantillon en bins équivalents à ceux utilisés pour diviser la population (cf. début du script) et on calcule pour chacun des critères le chi2 entre la population et l'échantillon sélectionné

hist_sample_density = list(np.histogram(sample.density, bins=hist_population_density[1]))
hist_sample_density[0] = hist_sample_density[0]/hist_sample_density[0].sum()
chi2_density = ((hist_sample_density[0]-hist_population_density[0])**2/hist_population_density[0])
chi2_density = np.nan_to_num(chi2_density)
chi2_density = chi2_density.sum()
    
hist_sample_income = list(np.histogram(sample.income, bins=hist_population_income[1]))
hist_sample_income[0] = hist_sample_income[0]/hist_sample_income[0].sum()
chi2_income = ((hist_sample_income[0]-hist_population_income[0])**2/hist_population_income[0])
chi2_income = np.nan_to_num(chi2_income)
chi2_income = chi2_income.sum()
    
hist_sample_degree = list(np.histogram(sample.degree, bins=hist_population_degree[1]))
hist_sample_degree[0] = hist_sample_degree[0]/hist_sample_degree[0].sum()
chi2_degree = ((hist_sample_degree[0]-hist_population_degree[0])**2/hist_population_degree[0])
chi2_degree = np.nan_to_num(chi2_degree)
chi2_degree = chi2_degree.sum()
    
hist_sample_long = list(np.histogram(sample.long, bins=hist_population_long[1]))
hist_sample_long[0] = hist_sample_long[0]/hist_sample_long[0].sum()
chi2_long = ((hist_sample_long[0]-hist_population_long[0])**2/hist_population_long[0])
chi2_long = np.nan_to_num(chi2_long)
chi2_long = chi2_long.sum()
    
hist_sample_lat = list(np.histogram(sample.lat, bins=hist_population_lat[1]))
hist_sample_lat[0] = hist_sample_lat[0]/hist_sample_lat[0].sum()
chi2_lat = ((hist_sample_lat[0]-hist_population_lat[0])**2/hist_population_lat[0])
chi2_lat = np.nan_to_num(chi2_lat)
chi2_lat = chi2_lat.sum()

print("\nchi2_lat : "+str(chi2_lat)+"\nchi2_long : "+str(chi2_long)+"\nchi2_income : "+str(chi2_income)+"\nchi2_degree : "+str(chi2_degree)+"\nchi2_density : "+str(chi2_density))
print(str(chi2_lat).replace(".",","))
print(str(chi2_long).replace(".",","))
print(str(chi2_income).replace(".",","))
print(str(chi2_degree).replace(".",","))
print(str(chi2_density).replace(".",","))

