import argparse
import pandas, json
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
#Fonctions outil
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
cursus = {}
prix = {}
parcours_classe = {}
classe = {}
professeur = {}

for id, row in pandas.read_excel(args.xlsx, sheet_name="Sheet1", encoding='utf-8').iterrows():
    if row["identifiant_1"] in lignes_pourries_à_ne_pas_traiter:
        continue
    else :
        eleves[row["identifiant_1"]] = générer_uuid("eleves", row["identifiant_1"])
        ConceptSchemes['Villes'] = générer_uuid("ConceptSchemes",'Villes')
        ConceptSchemes['Departement'] = générer_uuid("ConceptSchemes",'Departement')
        ConceptSchemes['Pays'] = générer_uuid("ConceptSchemes",'Pays')
        
        if (str(row["eleve_ville_naissance "])!='NaN'):
            villes[row["eleve_ville_naissance "]] = générer_uuid("villes",row["eleve_ville_naissance "])
        if (str(row["eleve_departement_naissance"])!='NaN'):
            departement[row["eleve_departement_naissance"]] = générer_uuid("departements",row["eleve_departement_naissance"])
        if (str(row["eleve_pays_naissance"])!= 'NaN'):
            pays[row["eleve_pays_naissance"]] = générer_uuid("pays",row["eleve_pays_naissance"])
        
        cursus[row["identifiant_1"]] = générer_uuid("cursus",row["identifiant_1"])
        if (row["prix_date"] and row["prix_nom"] and row["prix_discipline"]):
            id_prix = tuple((row["prix_date"], row["prix_nom"], row["prix_discipline"]))
            prix[id_prix] = générer_uuid('prix', id_prix)

        if ((row["identifiant_1"] != 'nan' and ["parcours_classe_date_entree"] != 'nan' and row["parcours_classe_date_sortie"] != 'nan' and row["classe_discipline"] != 'nan')):
            id_parcours_classe = tuple((row["identifiant_1"], row["parcours_classe_date_entree"], row["parcours_classe_date_sortie"], row["classe_discipline"]))
            parcours_classe[id_parcours_classe] = générer_uuid('parcours_classe', id_parcours_classe)
        ConceptSchemes['Disciplines'] = générer_uuid("ConceptSchemes",'Disciplines')
        classe[row['classe_discipline']] = générer_uuid('discipline', row['classe_discipline'])

        professeur[row['classe_nom_professeur']] = générer_uuid('professeurs', row['classe_nom_professeur'])

        # Voilà, on est sûr que la ligne est OK

###############################################################################
#Initialisation des CS
g.add(
    (
        URIRef(ConceptSchemes['Disciplines']),
        URIRef(is_a),
        URIRef(SKOS.ConceptSchemes)
    )
)

g.add(
    (
        URIRef(ConceptSchemes['Disciplines']),
        URIRef(SKOS.prefLabel),
        Literal('Discipline_Classes')
    )
)

NoeudVilles = ConceptSchemes['Villes']
NoeudDepartement = ConceptSchemes['Departement']
NoeudPays = ConceptSchemes['Pays']

g.add(
    (
        URIRef(NoeudVilles),
        URIRef(is_a),
        URIRef(SKOS.ConceptSchemes)
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
        URIRef(SKOS.ConceptSchemes)
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
        URIRef(SKOS.ConceptSchemes)
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
    else :
        #Creation des eleves
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
        else :
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
        #nom_epouse vide, non gere pour le moment
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
        else :
            g.add(
                    (
                        URIRef(uriEleve),
                        URIRef(HEMEF['sexe']),
                        Literal('Femme')
                    )
                )
        if (isinstance(row["eleve_date_naissance"], datetime.datetime)) :
            g.add(
                    (
                        URIRef(uriEleve),
                        URIRef(HEMEF["année_de_naissance"]),
                        Literal(row["eleve_date_naissance"], datatype=XSD.Date)
                    )
            )
        else :
            print("Erreur de date : ", row["identifiant_1"]," ", row["eleve_date_naissance"])

        if str(row["eleve_cote_AN_registre"] != 'nan') :
                g.add(
                        (
                            URIRef(uriEleve),
                            URIRef(HEMEF["cote_AN_registre"]),
                            Literal(row["eleve_cote_AN_registre"])
                        )
                    )

        if ((str(row["eleve_observations"]) != 'nan') and (str(row["eleve_observations"]) != 'NaN')) :
                g.add(
                        (
                            URIRef(uriEleve),
                            URIRef(HEMEF["observations"]),
                            Literal(row["eleve_observations"])
                        )
                    )

        if ((str(row["eleve_remarques de saisie"]) != 'nan') and (str(row["eleve_remarques de saisie"]) != 'NaN')) :
            g.add(
                    (
                        URIRef(uriEleve),
                        URIRef(HEMEF["eleve_remarques_de_saisie"]),
                        Literal(row["eleve_remarques de saisie"])
                    )
                )

        #Creation des métiers parents
        if (str(row["eleve_profession_pere"]) != 'nan' and  (str(row["eleve_profession_pere"]) != 'NaN')):
                g.add(
                        (
                            URIRef(uriEleve),
                            URIRef(HEMEF["profession_pere"]),
                            Literal(row["eleve_profession_pere"])
                        )
                    )

        if (str(row["eleve_profession_mere"]) != 'nan' and  (str(row["eleve_profession_mere"]) != 'NaN')) :
                g.add(
                        (
                            URIRef(uriEleve),
                            URIRef(HEMEF["profession_mere"]),
                            Literal(row["eleve_profession_mere"])
                        )
                    )

        #Creation des villes
        uriVille = None
        if (str(row["eleve_ville_naissance "]) != 'NaN'):
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

        #creation des departements
        uriDep = None
        if (str(row["eleve_departement_naissance"]) != 'NaN'):
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

            if (str(row["eleve_ville_naissance "]) != 'NaN') :
                g.add(
                    (
                        URIRef(uriVille),
                        URIRef(SKOS.broader),
                        URIRef(uriDep)
                    )
                )
                g.add(
                    (
                        URIRef(uriDep),
                        URIRef(SKOS.narrower),
                        URIRef(uriVille)
                        
                    )
                )

        #creation des Pays
        if (str(row["eleve_pays_naissance"]) != 'NaN'):
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
                    URIRef(SKOS.prefLabel),
                    Literal(row["eleve_pays_naissance"])
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

            if (str(row["eleve_ville_naissance "]) != 'NaN') :
                g.add(
                    (
                        URIRef(uriVille),
                        URIRef(SKOS.broader),
                        URIRef(uriPays)
                    )
                )
                g.add(
                    (
                        URIRef(uriPays),
                        URIRef(SKOS.narrower),
                        URIRef(uriVille)
                        
                    )
                )

            if (str(row["eleve_departement_naissance"]) != 'NaN') :
                g.add(
                        (
                            URIRef(uriDep),
                            URIRef(SKOS.broader),
                            URIRef(uriPays)
                        )
                )
                g.add(
                    (
                        URIRef(uriPays),
                        URIRef(SKOS.narrower),
                        URIRef(uriDep)
                        
                    )
                )
        

        if (str(row["eleve_ville_naissance_ancien_nom"]) != 'nan'):
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

        #Lien ville - eleve
        g.add(
            (
                URIRef(uriEleve),
                URIRef(HEMEF["nait_a"]),
                URIRef(uriVille)
            )
        )


        #Gestion des cursus

        g.add(
            (
                URIRef(cursus[row['identifiant_1']]),
                URIRef(is_a),
                URIRef("Cursus")
            )
        )
        if (str(row["cursus_motif_admission"]) != 'NaN' and str(row["cursus_motif_admission"]) != 'nan') :
            g.add(
                (
                    URIRef(cursus[row['identifiant_1']]),
                    URIRef(HEMEF["motif_admission"]),
                    Literal(row["cursus_motif_admission"])
                )
            )

        #cursus_date_epreuve_admission non traité, vide
        if (str(row["cursus_date_entree_conservatoire"]) != 'NaN') :
            g.add(
                (
                    URIRef(cursus[row['identifiant_1']]),
                    URIRef(HEMEF["date_entree_conservatoire"]),
                    Literal(row["cursus_date_entree_conservatoire"], datatype=XSD.Date)
                )
            )
        
        if (str(row["cursus_date_sortie_conservatoire"]).lower() != 'nan') :
            g.add(
                (
                    URIRef(cursus[row['identifiant_1']]),
                    URIRef(HEMEF["date_sortie_conservatoire"]),
                    Literal(row["cursus_date_sortie_conservatoire"], datatype=XSD.Date)
                )
            )

        if (str(row["cursus_motif_sortie"]).lower() != 'nan') :
            g.add(
                (
                    URIRef(cursus[row['identifiant_1']]),
                    URIRef(HEMEF["motif_sortie"]),
                    Literal(row["cursus_motif_sortie"])
                )
            )

        #Relation Eleve - Cursus

        g.add(
            (
                URIRef(eleves[row['identifiant_1']]),
                URIRef(HEMEF["a_pour_cursus"]),
                URIRef(cursus[row['identifiant_1']])
            )
        )

        # g.add(
        #     (    
        #         URIRef(cursus[row['identifiant_1']]),
        #         URIRef(HEMEF["est_cursus_de"]),
        #         URIRef(eleves[row['identifiant_1']])
        #     )
        # )

        #Gestion des prix
        uriPrix = None
        if (str(row["prix_date"]) != 'nan' and str(row["prix_nom"]) != 'nan' and str(row["prix_discipline"]) != 'nan'):
            id_prix = tuple((row["prix_date"], row["prix_nom"], row["prix_discipline"]))
            uriPrix = prix[id_prix]

            if (str(row["prix_discipline"]) != 'NaN' and str(row["prix_discipline"]) != 'nan') :
                intitule_prix = str(row["prix_nom"]) + " " + str(row["prix_date"]) + " : " + str(row["prix_discipline"])
            else :
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
            
            if (str(row["prix_nom"]) != 'NaN'):
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['nom_prix']),
                        Literal(row["prix_nom"])
                    )
                )

            g.add(
                (
                    URIRef(uriPrix),
                    URIRef('date_prix'),
                    Literal(row["prix_date"], datatype=XSD.Date)
                )
            )
            if (str(row["prix_discipline"]) != 'NaN' and str(row["prix_discipline"]) != 'nan') :
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['discipline_prix']),
                        Literal(row["prix_discipline"])
                    )
                )

          #Gestion des classes

            if (str(row['classe_discipline']) != 'NaN') and (str(row['classe_discipline']) != 'nan'):
                g.add(
                    (
                        URIRef(classe[row['classe_discipline']]),
                        URIRef(SKOS.inScheme),
                        URIRef(ConceptSchemes['Disciplines'])
                    )
                )

                g.add(
                    (
                        URIRef(ConceptSchemes['Disciplines']),
                        URIRef(SKOS.hasTopConcept),
                        URIRef(classe[row['classe_discipline']])
                    )
                )

                g.add(
                    (
                        URIRef(classe[row['classe_discipline']]),
                        URIRef(is_a),
                        URIRef(SKOS.Concept)
                    )
                )

                g.add(
                    (
                        URIRef(classe[row['classe_discipline']]),
                        URIRef(SKOS.prefLabel),
                        Literal(row['classe_discipline'])
                    )
                )

                if (str(row['classe_nom_professeur']) != 'NaN' and str(row['classe_nom_professeur']) != 'nan'):

                    g.add (
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

                    g.add(
                        (
                            URIRef(professeur[row['classe_nom_professeur']]),
                            URIRef(HEMEF['enseigne']),
                            URIRef(classe[row['classe_discipline']])
                        )
                    )


        #Gestion Parcours_classe

        if (row["identifiant_1"] != 'nan' and row["parcours_classe_date_entree"] != 'nan' and row["parcours_classe_date_sortie"] != 'nan' and row["classe_discipline"] != 'nan'):
            id_parcours_classe = tuple((row["identifiant_1"], row["parcours_classe_date_entree"], row["parcours_classe_date_sortie"], row["classe_discipline"]))
            uri_parcours_classe = parcours_classe[id_parcours_classe]

            g.add(
                (
                    URIRef(uri_parcours_classe),
                    URIRef(is_a),
                    URIRef(HEMEF["Parcours_classe"])
                )
            )

            g.add(
                (
                    URIRef(cursus[row['identifiant_1']]),
                    URIRef(HEMEF["a_pour_parcours"]),
                    URIRef(uri_parcours_classe)
                )
            )

            g.add(
                (
                    URIRef(uri_parcours_classe),
                    URIRef(HEMEF["est_parcours_de"]),
                    URIRef(cursus[row['identifiant_1']])
                )
            )

            if (str(row['parcours_classe_motif_entree']) != 'NaN') and (str(row['parcours_classe_motif_entree']) != 'nan') :
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["motif_entree"]),
                        Literal(row['parcours_classe_motif_entree'])
                    )
                )
            if (str(row['parcours_classe_observations_eleve']) != 'NaN') and (str(row['parcours_classe_observations_eleve']) != 'nan') :
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["observations_eleve"]),
                        Literal(row['parcours_classe_observations_eleve'])
                    )
                )

            g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF['classe_parcourue']),
                        URIRef(classe[row['classe_discipline']])
                    )
                )

            # g.add(
            #     (
            #         URIRef(classe[row['classe_discipline']]),
            #         URIRef(HEMEF['parcours_associe']),
            #         URIRef(uri_parcours_classe)
            #     )
            # )

            if uriPrix != None :
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
