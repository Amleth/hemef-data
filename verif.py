import pandas
import json
from pprint import pprint

chemin = '1856_1861_modifiéV1.xlsx'

for id, row in pandas.read_excel(chemin, sheet_name="classe", encoding='utf-8').iterrows():
    if pandas.notna(row['eleve_observations']):
        chaine = row['eleve_observations']
        if chaine.count(';') > 1:
            print(id + 1 , ' - ', row['Identifiant_1'], ' : ', chaine)
    