from collections import defaultdict
import pandas
import unicodedata
import yaml

def check(xlsx_file, divergences_file):
    d = defaultdict(list)

    # Champs dont les valeurs ne doivent pas différer d'une ligne à l'autre
    cdlvndpddulal = [
        "Identifiant_1",
        "Identifiant_2",
        "eleve_nom",
        "eleve_complement_nom",
        "eleve_nom_epouse",
        "eleve_prenom_1",
        "eleve_prenom_2",
        "eleve_complement_prenom",
        "eleve_sexe",
        "eleve_date_naissance",
        "eleve_ville_naissance ",
        "eleve_ville_naissance_ancien_nom",
        "eleve_departement_naissance",
        "eleve_pays_naissance",
        "eleve_profession_pere",
        "eleve_profession_mere",
        "eleve_cote_AN_registre",
        "cursus_date_entree_conservatoire",
        "cursus_date_sortie_conservatoire",
        # "cursus_motif_sortie",
        "pre-cursus_nom_etablissement",
        "pre-cursus_type_etablissement_",
        "pre-cursus_ville_etablissement_",
    ]

    # Regroupement des lignes par identifiant (champ "Identifiant_1")
    for id, row in pandas.read_excel(xlsx_file, sheet_name="classe", encoding='utf-8').iterrows():
        d[row["Identifiant_1"]].append(dict(row))

    toutes_les_données_à_comparer = {}
    for id, lignes in d.items():
        # On ne considère que les identifiants qui ont au moins deux lignes
        if len(lignes) >= 2:
            toutes_les_données_à_comparer[id] = defaultdict(set)
            for c in range(0, len(lignes)): #c n'est jamais utilisé, a corriger eventuellement pour avoir un code plus propre
                for ligne in lignes:
                    for champ, valeur in ligne.items():
                        if champ in cdlvndpddulal:
                            valeur_normalisée = unicodedata.normalize("NFKD", str(valeur))
                            valeur_normalisée = valeur_normalisée.strip()
                            valeur_normalisée = valeur_normalisée.replace('\n', '')
                            if valeur_normalisée == 'nan':
                                valeur_normalisée = ''
                            toutes_les_données_à_comparer[id][champ].add(valeur_normalisée)  # toutes les valeurs sont converties en string car nan != nan

    divergences = {}
    for id, fields in toutes_les_données_à_comparer.items():
        for field, values in fields.items():
            if len(values) > 1:
                if id not in divergences:
                    divergences[id] = {}
                divergences[id][field] = list(values)

    with open(divergences_file, 'w') as file:
        yaml.dump(divergences, file, allow_unicode=True, line_break=None)
    
    return divergences.keys()