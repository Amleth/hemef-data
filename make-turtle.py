import argparse
import pandas
import json
from pprint import pprint
from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, FOAF, RDF, RDFS, SKOS, XSD, OWL
from rdflib.collection import Collection
import sys
import uuid
import yaml
from json.decoder import JSONDecodeError
import os
import datetime

from check import check

parser = argparse.ArgumentParser()
parser.add_argument("--xlsx")
parser.add_argument("--divergences")
parser.add_argument("--turtle")
args = parser.parse_args()

BASE_NS = Namespace("http://data-iremus.huma-num.fr/id/")
HEMEF = Namespace("http://data-iremus.huma-num.fr/ns/hemef#")
SCHEMA = Namespace("http://schema.org/")

is_a = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

g = Graph()
g.bind("skos", SKOS)
g.bind("hemef", HEMEF)

# NOTES SUR LES DONNÉES
#   - Chaque ligne a 67 colonnes

################################################################################
################################################################################
################################################################################

################################################################################
# Fonctions outil


def genURIIremus():
    return (BASE_NS[str(uuid.uuid4())])

################################################################################


registre = {}

if os.path.exists("registre.yaml"):
    with open("registre.yaml") as file:
        registre = yaml.load(file)


def générer_uuid(type_entité, clef_entité):
    if type_entité not in registre:
        registre[type_entité] = {}
    if clef_entité not in registre[type_entité]:
        registre[type_entité][clef_entité] = str(uuid.uuid4())
    return registre[type_entité][clef_entité]

################################################################################


lignes_pourries_à_ne_pas_traiter = check(args.xlsx, args.divergences)

ConceptSchemes = {}
villes = {}
departement = {}
pays = {}
eleves = {}
# cursus = {}
prix = {}
parcours_classe = {}
discipline = {}
classe = {} #un tuple discipline, professeur
professeur = {}

for id, row in pandas.read_excel(args.xlsx, sheet_name="Sheet1", encoding='utf-8').iterrows():
    if row["identifiant_1"] in lignes_pourries_à_ne_pas_traiter:
        continue
    else:
        eleves[row["identifiant_1"]] = générer_uuid("eleves", row["identifiant_1"])
        ConceptSchemes['Villes'] = générer_uuid("ConceptSchemes", 'Villes')
        ConceptSchemes['Departement'] = générer_uuid("ConceptSchemes", 'Departement')
        ConceptSchemes['Pays'] = générer_uuid("ConceptSchemes", 'Pays')

        if (pandas.notna(row["eleve_ville_naissance "])):
            villes[row["eleve_ville_naissance "]] = générer_uuid("villes", row["eleve_ville_naissance "])
        if (pandas.notna(row["eleve_departement_naissance"])):
            departement[row["eleve_departement_naissance"]] = générer_uuid("departements", row["eleve_departement_naissance"])
        if (pandas.notna(row["eleve_pays_naissance"])):
            pays[row["eleve_pays_naissance"]] = générer_uuid("pays", row["eleve_pays_naissance"])
        else:
            pays[row["eleve_pays_naissance"]] = générer_uuid("pays", "France")

        # cursus[row["identifiant_1"]] = générer_uuid("cursus", row["identifiant_1"])

        if (pandas.notna(row["prix_date"]) and pandas.notna(row["prix_nom"]) and pandas.notna(row["prix_discipline"])):
            id_prix = tuple((row["prix_date"], row["prix_nom"], row["prix_discipline"]))
            prix[id_prix] = générer_uuid('prix', id_prix)

        if ((pandas.notna(row["classe_nom_professeur"]) and pandas.notna(row["parcours_classe_date_entree"]) and pandas.notna(row["classe_discipline"]))):
            id_parcours_classe = tuple((row["classe_nom_professeur"], row["parcours_classe_date_entree"], row["classe_discipline"]))
            parcours_classe[id_parcours_classe] = générer_uuid('parcours_classe', id_parcours_classe)

        ConceptSchemes['Disciplines'] = générer_uuid("ConceptSchemes", 'Disciplines')
        ConceptSchemes['Classes'] = générer_uuid("ConceptSchemes", 'Classes')

        if ((pandas.notna(row['classe_discipline']))):
            discipline[row['classe_discipline']] = générer_uuid('Disciplines', row['classe_discipline'])

        if ((pandas.notna(row['classe_nom_professeur']))):
            professeur[row['classe_nom_professeur']] = générer_uuid('Professeurs', row['classe_nom_professeur'])

        if ((pandas.notna(row['classe_discipline']) and pandas.notna(row['classe_nom_professeur']) )):
            id_classe = tuple((row['classe_discipline'], row['classe_nom_professeur']))
            classe[id_classe]=générer_uuid('Classe', id_classe)

        # Voilà, on est sûr que la ligne est OK

###############################################################################
# Initialisation des CS
g.add(
    (
        URIRef(ConceptSchemes['Disciplines']),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)

g.add(
    (
        URIRef(ConceptSchemes['Disciplines']),
        URIRef(SKOS.prefLabel),
        Literal('Discipline_Classes')
    )
)

#Classes
g.add(
    (
        URIRef(ConceptSchemes['Classes']),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)

g.add(
    (
        URIRef(ConceptSchemes['Classes']),
        URIRef(SKOS.prefLabel),
        Literal('Classes')
    )
)

NoeudVilles = ConceptSchemes['Villes']
NoeudDepartement = ConceptSchemes['Departement']
NoeudPays = ConceptSchemes['Pays']

g.add(
    (
        URIRef(NoeudVilles),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudVilles),
        URIRef(SKOS.prefLabel),
        Literal('Villes')
    )
)

g.add(
    (
        URIRef(NoeudDepartement),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudDepartement),
        URIRef(SKOS.prefLabel),
        Literal('Departements')
    )
)
g.add(
    (
        URIRef(NoeudPays),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudPays),
        URIRef(SKOS.prefLabel),
        Literal('Pays')
    )
)

################################################################################
################################################################################

for id, row in pandas.read_excel(args.xlsx, sheet_name="Sheet1", encoding='utf-8').iterrows():
    if row["identifiant_1"] in lignes_pourries_à_ne_pas_traiter:
        continue
    else:
        # Creation des eleves
        uriEleve = eleves[row["identifiant_1"]]
        # On definit chaque elève comme une personne
        g.add(
            (
                URIRef(uriEleve),
                URIRef(is_a),
                URIRef(HEMEF["Eleve"])
            )
        )

        if (str(row['eleve_prenom_2']) != "nan"):
            prenom = row['eleve_prenom_1'] + " " + row['eleve_prenom_2']
        else:
            prenom = row['eleve_prenom_1']
        if (str(row['eleve_complement_prenom']) != "nan"):
            prenom = prenom + " " + row['eleve_complement_prenom']
        g.add(
            (
                URIRef(uriEleve),
                URIRef(HEMEF["prenom"]),
                Literal(prenom)
            )
        )
        g.add(
            (
                URIRef(uriEleve),
                URIRef(HEMEF["nom"]),
                Literal(row["eleve_nom"])
            )
        )
        if (str(row['eleve_complement_nom']) != "nan"):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["complement_nom"]),
                    Literal(row['eleve_complement_nom'])
                )
            )
        # nom_epouse vide, non gere pour le moment
        if (str(row['eleve_pseudonyme']) != "nan"):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["pseudonyme"]),
                    Literal(row['eleve_pseudonyme'])
                )
            )
        if (str(row['eleve_sexe']) == "H"):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["sexe"]),
                    Literal('Homme')
                )
            )
        else:
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF['sexe']),
                    Literal('Femme')
                )
            )
        if (isinstance(row["eleve_date_naissance"], datetime.datetime)):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["année_de_naissance"]),
                    Literal(row["eleve_date_naissance"], datatype=XSD.Date)
                )
            )
        else:
            print("Erreur de date : ", row["identifiant_1"], " ", row["eleve_date_naissance"])

        if str(row["eleve_cote_AN_registre"] != 'nan'):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["cote_AN_registre"]),
                    Literal(row["eleve_cote_AN_registre"])
                )
            )

        if ((str(row["eleve_observations"]) != 'nan') and (str(row["eleve_observations"]) != 'NaN')):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["observations"]),
                    Literal(row["eleve_observations"])
                )
            )

        if ((str(row["eleve_remarques de saisie"]) != 'nan') and (str(row["eleve_remarques de saisie"]) != 'NaN')):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["eleve_remarques_de_saisie"]),
                    Literal(row["eleve_remarques de saisie"])
                )
            )

        # Creation des métiers parents
        if (str(row["eleve_profession_pere"]) != 'nan' and (str(row["eleve_profession_pere"]) != 'NaN')):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["profession_pere"]),
                    Literal(row["eleve_profession_pere"])
                )
            )

        if (str(row["eleve_profession_mere"]) != 'nan' and (str(row["eleve_profession_mere"]) != 'NaN')):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["profession_mere"]),
                    Literal(row["eleve_profession_mere"])
                )
            )

        # Creation des villes
        uriVille = None
        if (pandas.notna(row["eleve_ville_naissance "])):
            uriVille = villes[row["eleve_ville_naissance "]]
            g.add(
                (
                    URIRef(uriVille),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )
            g.add(
                (
                    URIRef(uriVille),
                    URIRef(SKOS.prefLabel),
                    Literal(row["eleve_ville_naissance "])
                )
            )
            g.add(
                (
                    URIRef(uriVille),
                    URIRef(SKOS.inScheme),
                    URIRef(NoeudVilles)
                )
            )
            g.add(
                (
                    URIRef(NoeudVilles),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(uriVille)
                )
            )

            # Lien ville - eleve
            # On ne tient pas compte des villes 'NaN'
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["nait_a"]),
                    URIRef(uriVille)
                )
            )

        # creation des departements
        uriDep = None
        if (pandas.notna(row["eleve_departement_naissance"])):
            uriDep = departement[row["eleve_departement_naissance"]]
            g.add(
                (
                    URIRef(uriDep),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )
            g.add(
                (
                    URIRef(uriDep),
                    URIRef(SKOS.prefLabel),
                    Literal(row["eleve_departement_naissance"])
                )
            )
            g.add(
                (
                    URIRef(uriDep),
                    URIRef(SKOS.inScheme),
                    URIRef(NoeudDepartement)
                )
            )
            g.add(
                (
                    URIRef(NoeudDepartement),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(uriDep)
                )
            )

        # creation des Pays
        uriPays = pays[row["eleve_pays_naissance"]]
        g.add(
            (
                URIRef(uriPays),
                URIRef(is_a),
                URIRef(SKOS.Concept)
            )
        )

        g.add(
            (
                URIRef(uriPays),
                URIRef(SKOS.inScheme),
                URIRef(NoeudPays)
            )
        )
        g.add(
            (
                URIRef(NoeudPays),
                URIRef(SKOS.hasTopConcept),
                URIRef(uriPays)
            )
        )
        if (pandas.notna(row["eleve_pays_naissance"])):
            g.add(
                (
                    URIRef(uriPays),
                    URIRef(SKOS.prefLabel),
                    Literal(row["eleve_pays_naissance"])
                )
            )
        # Une case vide (NaN) correspond à la France
        else:
            g.add(
                (
                    URIRef(uriPays),
                    URIRef(SKOS.prefLabel),
                    Literal("France")
                )
            )

        if uriVille:
            if uriDep:
                g.add((
                    URIRef(uriVille),
                    URIRef(SKOS.broader),
                    URIRef(uriDep)
                ))

                g.add((
                    URIRef(uriDep),
                    URIRef(SKOS.narrower),
                    URIRef(uriVille)
                ))
                if uriPays:
                    g.add((
                        URIRef(uriDep),
                        URIRef(SKOS.broader),
                        URIRef(uriPays)
                    ))

                    g.add((
                        URIRef(uriPays),
                        URIRef(SKOS.narrower),
                        URIRef(uriDep)
                    ))
            elif uriPays:
                g.add((
                    URIRef(uriVille),
                    URIRef(SKOS.broader),
                    URIRef(uriPays)
                ))

                g.add((
                    URIRef(uriPays),
                    URIRef(SKOS.narrower),
                    URIRef(uriVille)
                ))

        if (pandas.notna(row["eleve_ville_naissance_ancien_nom"])):
            g.add(
                (
                    URIRef(uriVille),
                    URIRef(HEMEF["ancien_nom"]),
                    Literal(row["eleve_ville_naissance_ancien_nom"])
                )
            )

            # if (str(row["eleve_departement_naissance"]) != 'nan'):
            #     g.add(
            #         (
            #             URIRef(uriVille),
            #             URIRef(HEMEF["departement"]),
            #             Literal(row["eleve_departement_naissance"])
            #         )
            #     )
            # if (str(row["eleve_pays_naissance"]) != 'nan'):
            #     g.add(
            #         (
            #             URIRef(uriVille),
            #             URIRef(HEMEF["pays"]),
            #             Literal(row["eleve_pays_naissance"])
            #         )
            #     )

        #Gestion des précursus
        if (pandas.notna(row["pre-cursus_nom_etablissement"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["etablissement_pre-cursus"]),
                    Literal(row["pre-cursus_nom_etablissement"])
                )
            )
        
        if (pandas.notna(row["pre-cursus_type_etablissement_"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["type_etablissement_pre-cursus"]),
                    Literal(row["pre-cursus_type_etablissement_"])
                )
            )
        
        villePC = None
        if (pandas.notna(row["pre-cursus_ville_etablissement_"])):
            villePC = row["pre-cursus_ville_etablissement_"]
            if villePC in villes :
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["ville_pre-cursus"]),
                        URIRef(villes[villePC])
                    )
                )
            else :
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["ville_pre-cursus"]),
                        Literal(row["pre-cursus_ville_etablissement_"])
                    )
                )

        # Gestion des cursus ---> maintenant associés aux eleves
        # g.add(
        #     (
        #         URIRef(cursus[row['identifiant_1']]),
        #         URIRef(is_a),
        #         URIRef(HEMEF["Cursus"])
        #     )
        # )
        if (pandas.notna(row["cursus_motif_admission"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["cursus_motif_admission"]),
                    Literal(row["cursus_motif_admission"])
                )
            )

        # cursus_date_epreuve_admission non traité, vide
        if (pandas.notna(row["cursus_date_entree_conservatoire"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["cursus_date_entree_conservatoire"]),
                    Literal(row["cursus_date_entree_conservatoire"], datatype=XSD.Date)
                )
            )

        if (pandas.notna(row["cursus_date_sortie_conservatoire"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["cursus_date_sortie_conservatoire"]),
                    Literal(row["cursus_date_sortie_conservatoire"], datatype=XSD.Date)
                )
            )

        if (pandas.notna(row["cursus_motif_sortie"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["cursus_motif_sortie"]),
                    Literal(row["cursus_motif_sortie"])
                )
            )

        # Relation Eleve - Cursus

        # g.add(
        #     (
        #         URIRef(eleves[row['identifiant_1']]),
        #         URIRef(HEMEF["a_pour_cursus"]),
        #         URIRef(cursus[row['identifiant_1']])
        #     )
        # )

        # Gestion des prix
        uriPrix = None
        if (pandas.notna(row["prix_date"]) and pandas.notna(row["prix_nom"]) and pandas.notna(row["prix_discipline"])):
            id_prix = tuple((row["prix_date"], row["prix_nom"], row["prix_discipline"]))
            uriPrix = prix[id_prix]

            if (pandas.notna(row["prix_discipline"])):
                intitule_prix = str(row["prix_nom"]) + " " + str(row["prix_date"]) + " : " + str(row["prix_discipline"])
            else:
                intitule_prix = str(row["prix_nom"]) + " " + str(row["prix_date"])

            g.add(
                (
                    URIRef(uriPrix),
                    URIRef(HEMEF['intitule']),
                    Literal(intitule_prix)
                )
            )

            g.add(
                (
                    URIRef(uriPrix),
                    URIRef(is_a),
                    URIRef(HEMEF["Prix"])
                )
            )

            if (pandas.notna(row["prix_nom"])):
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['nom_prix']),
                        Literal(row["prix_nom"])
                    )
                )

            # gYeat represents a specific calendar year. The letter g signifies "Gregorian." The format of xsd:gYear is CCYY
            g.add(
                (
                    URIRef(uriPrix),
                    URIRef(HEMEF['année_prix']),
                    Literal(row["prix_date"], datatype=XSD.gYear)
                )
            )
            if (pandas.notna(row["prix_discipline"])):
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['discipline_prix']),
                        Literal(row["prix_discipline"])
                    )
                )

        # Gestion des classes
        Discipline = None
        if (pandas.notna(row['classe_discipline'])):
            Discipline = row['classe_discipline']
            g.add(
                (
                    URIRef(discipline[row['classe_discipline']]),
                    URIRef(SKOS.inScheme),
                    URIRef(ConceptSchemes['Disciplines'])
                )
            )

            g.add(
                (
                    URIRef(ConceptSchemes['Disciplines']),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(discipline[row['classe_discipline']])
                )
            )

            g.add(
                (
                    URIRef(discipline[row['classe_discipline']]),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )

            g.add(
                (
                    URIRef(discipline[row['classe_discipline']]),
                    URIRef(SKOS.prefLabel),
                    Literal(row['classe_discipline'])
                )
            )

        Prof = None
        if (pandas.notna(row['classe_nom_professeur'])):
            Prof = row['classe_nom_professeur']
            g.add(
                (
                    URIRef(professeur[row['classe_nom_professeur']]),
                    URIRef(is_a),
                    URIRef(HEMEF['Professeur'])
                )
            )

            g.add(
                (
                    URIRef(professeur[row['classe_nom_professeur']]),
                    URIRef(HEMEF['nom_professeur']),
                    Literal(row['classe_nom_professeur'])
                )
            )
        
        uriClasse = None
        if Discipline and Prof :
            id_classe = tuple((Discipline, Prof))
            uriClasse = classe[id_classe]
            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(SKOS.inScheme),
                    URIRef(ConceptSchemes['Disciplines'])
                )
            )

            g.add(
                (
                    URIRef(ConceptSchemes['Disciplines']),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(uriClasse)
                )
            )
            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )
            nom_classe = Discipline + ", " + Prof
            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(SKOS.prefLabel),
                    Literal(nom_classe)
                )
            )

            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(HEMEF['enseignant']),
                    URIRef(professeur[Prof]) 
                )
            )

            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(HEMEF['enseigne']),
                    URIRef(discipline[Discipline])
                )
            )

            if pandas.notna(row['classe_observations']):
                g.add(
                    (
                        URIRef(uriClasse),
                        URIRef(HEMEF['observations']),
                        Literal(row['classe_observations'])
                    )
                )

            if pandas.notna(row['classes_remarques_saisie']):
                g.add(
                    (
                        URIRef(uriClasse),
                        URIRef(HEMEF['remarques_saisie']),
                        Literal(row['classes_remarques_saisie'])
                    )
                )

        
        # Gestion Parcours_classe

        if (pandas.notna(row["classe_nom_professeur"]) and pandas.notna(row["parcours_classe_date_entree"]) and pandas.notna(row["classe_discipline"])):
            id_parcours_classe = tuple((row["classe_nom_professeur"], row["parcours_classe_date_entree"], row["classe_discipline"]))
            uri_parcours_classe = parcours_classe[id_parcours_classe]

            g.add(
                (
                    URIRef(uri_parcours_classe),
                    URIRef(is_a),
                    URIRef(HEMEF["Parcours_classe"])
                )
            )
            
            #modif : mtn lié a eleve et non plus a cursus
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["a_pour_parcours"]),
                    URIRef(uri_parcours_classe)
                )
            )

            g.add(
                (
                    URIRef(uri_parcours_classe),
                    URIRef(HEMEF["est_parcours_de"]),
                    URIRef(eleves[row['identifiant_1']])
                )
            )

            if (pandas.notna(row['parcours_classe_motif_entree'])):
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["motif_entree"]),
                        Literal(row['parcours_classe_motif_entree'])
                    )
                )
            if (pandas.notna(row['parcours_classe_observations_eleve'])):
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["observations_eleve"]),
                        Literal(row['parcours_classe_observations_eleve'])
                    )
                )

            if (uriClasse):
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF['classe_parcourue']),
                        URIRef(uriClasse)
                    )
                )

                if uriPrix != None:
                    g.add(
                        (
                            URIRef(uriPrix),
                            URIRef(HEMEF['recompense_parcours']),
                            URIRef(uri_parcours_classe)
                        )
                    )

                    g.add(
                        (
                            URIRef(uri_parcours_classe),
                            URIRef(HEMEF['prix_decerne']),
                            URIRef(uriPrix),
                        )
                    )

################################################################
################################################################################
################################################################################

with open(args.turtle, "w") as file:
    output = f"@base <{BASE_NS}> .\n" + g.serialize(
        format="turtle", base=BASE_NS
    ).decode("utf-8")
    file.write(output)

with open("registre.yaml", "w") as file:
    yaml.dump(registre, file)
