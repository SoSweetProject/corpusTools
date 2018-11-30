- import.py -
  Importe des corpus au format vrt
  À lancer en sudo
  Si les attributs des tweets ou l'organisation des colonnes diffèrent, modifier " -P pos -P lemma -S text:0+id+user+date+geo" (ligne 36) en fonction

- toVrtByDep.py -
  à partir du corpus au format json, le convertit au format vrt
  + 1 fichier par département

- fusion.py -
  fusionne les annotations des tweets au format CONLLU avec les tweets au format json
