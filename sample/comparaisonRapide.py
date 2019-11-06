# coding: utf-8

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

population = pd.read_csv("/Users/ltarrade/Desktop/Louise/SoSweet/utilisateurs_prio/data/dfValidUsersMore10Tweets.csv", index_col=0)
echantillon = pd.read_csv("/Users/ltarrade/Desktop/Louise/SoSweet/utilisateurs_prio/échantillon_sélectionné/échantillon_1000.csv", index_col=0, sep=";")

#print(population.shape)
#print(population.head())
#print(echantillon.shape)
#print(echantillon.head())

#population.loc[population.index==101037588]
#echantillon.loc[echantillon.index==101037588]

def comp(population_critere,echantillon_critere,bins,critere) : 
    
    hist_population = np.histogram(population_critere, bins=bins)
    hist_population = list(hist_population)
    hist_population[0]=hist_population[0]/hist_population[0].sum()
    
    hist_echantillon = np.histogram(echantillon_critere, bins=hist_population[1])
    hist_echantillon = list(hist_echantillon)
    hist_echantillon[0]=hist_echantillon[0]/hist_echantillon[0].sum()
    
    #print(hist_population)
    #print(hist_echantillon)
    
    plt.plot(hist_population[1][1:], hist_population[0])
    plt.plot(hist_echantillon[1][1:], hist_echantillon[0])
    plt.title(critere+", "+str(bins)+" bins")

plt.figure(1, figsize=(20,20))
plt.subplot(3, 2, 1)
comp(population.lat,echantillon.lat,50,"lat")
plt.subplot(3, 2, 3)
comp(population.long,echantillon.long,50,"long")
plt.subplot(3, 2, 5)
comp(population.income,echantillon.income,50,"income")
plt.subplot(2, 2, 2)
comp(population.density,echantillon.density,50,"density")
plt.subplot(2, 2, 4)
comp(population.degree,echantillon.degree,50,"degree")

plt.show()
