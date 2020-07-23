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
g.bind("dcterms", DCTERMS)
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
toponymes = {}

villes = {}
departements = {}
pays = {}

eleves = {}

metiers = {}
prixNom = {}
prixType = {}
prixDiscipline = {}
prixNomComplément = {}
prix = {}
parcours_classe = {}
discipline = {}
classe = {}  # un tuple discipline, professeur
professeur = {}

for id, row in pandas.read_excel(args.xlsx, sheet_name="Sheet1", encoding='utf-8').iterrows():
    if row["identifiant_1"] in lignes_pourries_à_ne_pas_traiter:
        continue
    else:
        eleves[row["identifiant_1"]] = générer_uuid(
            "eleves", row["identifiant_1"])

        ConceptSchemes['Toponymes'] = générer_uuid(
            "ConceptSchemes", 'Toponymes')

        ConceptSchemes['Métiers'] = générer_uuid("ConceptSchemes", 'Métiers')
        # CS Thesaurus Prix
        ConceptSchemes['PrixNom'] = générer_uuid("ConceptSchemes", 'PrixNom')
        ConceptSchemes['PrixType'] = générer_uuid("ConceptSchemes", 'PrixType')
        ConceptSchemes['PrixDiscipline'] = générer_uuid(
            "ConceptSchemes", 'PrixDiscipline')
        ConceptSchemes['PrixNomComplément'] = générer_uuid("ConceptSchemes", 'PrixNomComplément')

        if (pandas.notna(row["eleve_ville_naissance"])):
            villes[row["eleve_ville_naissance"].strip().capitalize()] = générer_uuid(
                "villes", row["eleve_ville_naissance"].strip().capitalize())
        else:
            villes["Ville inconnue"] = générer_uuid("villes", "Ville inconnue")

        if (pandas.notna(row["eleve_departement_naissance"])):
            departements[row["eleve_departement_naissance"].strip().capitalize(
            )] = générer_uuid("departements", row["eleve_departement_naissance"])
        else:
            if pandas.notna(row["eleve_pays_naissance"]) :
                ssZoneDe = row["eleve_pays_naissance"]
            else : ssZoneDe = 'France'
            departements[ssZoneDe + " - Département inconnu"] = générer_uuid(
                "departements", ssZoneDe + " - Département inconnu")

        if (pandas.notna(row["eleve_pays_naissance"])):
            pays[row["eleve_pays_naissance"].strip().capitalize()] = générer_uuid(
                "pays", row["eleve_pays_naissance"])
        else:
            pays['France'] = générer_uuid("pays", "France")

        # Thesaurus Prix
        if (pandas.notna(row['prix_nom'])):
            prixNom[row['prix_nom'].strip().capitalize()] = générer_uuid(
                'PrixNom', row['prix_nom'].strip().capitalize())

        if (pandas.notna(row['prix_nom_complément'])):
            prixNomComplément[row['prix_nom_complément'].strip().capitalize()] = générer_uuid(
                'PrixNomComplément', row['prix_nom_complément'].strip().capitalize())

        if (pandas.notna(row['prix_type'])):
            prixType[row['prix_type'].strip().capitalize()] = générer_uuid(
                'PrixType', row['prix_type'].strip().capitalize())

        if (pandas.notna(row['prix_discipline'])):
            prixDiscipline[row['prix_discipline'].strip().capitalize()] = générer_uuid(
                'PrixDiscipline', row['prix_discipline'].strip().capitalize())

        if (pandas.notna(row["prix_date"]) and pandas.notna(row["prix_nom"].strip().capitalize())):
            if pandas.notna(row["prix_discipline"]):
                id_prix = tuple((row['identifiant_1'], row["prix_date"], row["prix_nom"].strip().capitalize(), row["prix_discipline"].strip().capitalize()))
            else : id_prix = tuple((row['identifiant_1'], row["prix_date"], row["prix_nom"].strip().capitalize()))
            prix[id_prix] = générer_uuid('prix', id_prix)

        # Creation des clés pour les Parcours_classe
        if (pandas.notna(row["classe_nom_professeur"]) and pandas.notna(row["identifiant_1"]) and pandas.notna(row["parcours_classe_date_entree"]) and pandas.notna(row["classe_discipline"])):
            id_parcours_classe = tuple((row["identifiant_1"], row["classe_nom_professeur"].strip().capitalize(
            ), row["parcours_classe_date_entree"], row["classe_discipline"].strip().capitalize()))
            parcours_classe[id_parcours_classe] = générer_uuid(
                'parcours_classe', id_parcours_classe)
        else:
            # Il existe des éléments vides dans la clé, il va falloir bricoler d'après les cas identifies
            if pandas.isna(row["classe_nom_professeur"]):
                if pandas.isna(row["parcours_classe_date_entree"]):
                    if pandas.isna(row["classe_discipline"]):
                        parcours_classe[row["identifiant_1"]] = générer_uuid(
                            'parcours_classe', row["identifiant_1"])
                    else:
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["classe_discipline"].strip().capitalize()))
                        parcours_classe[id_parcours_classe] = générer_uuid(
                            'parcours_classe', id_parcours_classe)
                else:
                    if pandas.isna(row["classe_discipline"]):
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["parcours_classe_date_entree"]))
                        parcours_classe[id_parcours_classe] = générer_uuid(
                            'parcours_classe', id_parcours_classe)
                    else:
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["parcours_classe_date_entree"], row["classe_discipline"].strip().capitalize()))
                        parcours_classe[id_parcours_classe] = générer_uuid(
                            'parcours_classe', id_parcours_classe)
            else:
                # dernier cas possible : seul row["parcours_classe_date_entree"] est vide
                id_parcours_classe = tuple((row["identifiant_1"], row["classe_nom_professeur"].strip(
                ).capitalize(), row["classe_discipline"].strip().capitalize()))
                parcours_classe[id_parcours_classe] = générer_uuid(
                    'parcours_classe', id_parcours_classe)

        ConceptSchemes['Disciplines'] = générer_uuid(
            "ConceptSchemes", 'Disciplines')

        if ((pandas.notna(row['eleve_profession_pere']))):
            metiers[row['eleve_profession_pere'].strip().capitalize()] = générer_uuid(
                'Métiers', row['eleve_profession_pere'].strip().capitalize())

        if ((pandas.notna(row['eleve_profession_mere']))):
            metiers[row['eleve_profession_mere'].strip().capitalize()] = générer_uuid(
                'Métiers', row['eleve_profession_mere'].strip().capitalize())

        if ((pandas.notna(row['classe_discipline']))):
            discipline[row['classe_discipline'].strip().capitalize()] = générer_uuid(
                'Disciplines', row['classe_discipline'].strip().capitalize())

        if ((pandas.notna(row['classe_nom_professeur']))):
            professeur[row['classe_nom_professeur'].strip().capitalize()] = générer_uuid(
                'Professeurs', row['classe_nom_professeur'].strip().capitalize())

        if ((pandas.notna(row['classe_discipline']) and pandas.notna(row['classe_nom_professeur']))):
            id_classe = tuple((row['classe_discipline'].strip().capitalize(
            ), row['classe_nom_professeur'].strip().capitalize()))
            classe[id_classe] = générer_uuid('Classe', id_classe)
        elif pandas.notna(row['classe_discipline']):
            id_classe = row['classe_discipline'].strip().capitalize()
            classe[id_classe] = générer_uuid('Classe', id_classe)

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
        URIRef(DCTERMS.title),
        Literal('Discipline_Classes')
    )
)

g.add(
    (
        URIRef(ConceptSchemes['Métiers']),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)

g.add(
    (
        URIRef(ConceptSchemes['Métiers']),
        URIRef(DCTERMS.title),
        Literal('Métiers')
    )
)

NoeudToponymes = ConceptSchemes['Toponymes']

g.add(
    (
        URIRef(NoeudToponymes),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudToponymes),
        URIRef(DCTERMS.title),
        Literal('Toponymes')
    )
)

NoeudPrixNom = ConceptSchemes['PrixNom']
NoeudPrixType = ConceptSchemes['PrixType']
NoeudPrixDiscipline = ConceptSchemes['PrixDiscipline']
NoeudPrixNomComplément = ConceptSchemes['PrixNomComplément']

g.add(
    (
        URIRef(NoeudPrixNom),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudPrixNom),
        URIRef(DCTERMS.title),
        Literal('Noms de Prix')
    )
)

g.add(
    (
        URIRef(NoeudPrixNomComplément),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudPrixNomComplément),
        URIRef(DCTERMS.title),
        Literal('Compléments noms de Prix')
    )
)

g.add(
    (
        URIRef(NoeudPrixType),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudPrixType),
        URIRef(DCTERMS.title),
        Literal('Types de Prix')
    )
)

g.add(
    (
        URIRef(NoeudPrixDiscipline),
        URIRef(is_a),
        URIRef(SKOS.ConceptScheme)
    )
)
g.add(
    (
        URIRef(NoeudPrixDiscipline),
        URIRef(DCTERMS.title),
        Literal('Disciplines de Prix')
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
        g.add(
            (
                URIRef(uriEleve),
                URIRef(is_a),
                URIRef(HEMEF["Eleve"])
            )
        )

        if (pandas.notna(row['eleve_prenom_2'])):
            prenom = row['eleve_prenom_1'] + " " + row['eleve_prenom_2']
        else:
            prenom = row['eleve_prenom_1']
        if (pandas.notna(row['eleve_complement_prenom'])):
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
        date_time = None
        if (isinstance(row["eleve_date_naissance"], datetime.date)):
            date_time = str(row["eleve_date_naissance"]).split()
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["date_de_naissance"]),
                    Literal(date_time[0], datatype=XSD.Date)
                )
            )
        elif(pandas.notna(row["eleve_date_naissance"])):
            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["hypothèse_date_de_naissance"]),
                    Literal(row["eleve_date_naissance"])
                )
            )

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
        if pandas.notna(row["eleve_profession_pere"]):
            profession_pere = str(
                row["eleve_profession_pere"]).strip().capitalize()
            g.add(
                (
                    URIRef(metiers[profession_pere]),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )

            g.add(
                (
                    URIRef(metiers[profession_pere]),
                    URIRef(SKOS.inScheme),
                    URIRef(ConceptSchemes['Métiers'])
                )
            )
            g.add(
                (
                    URIRef(ConceptSchemes['Métiers']),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(metiers[profession_pere])
                )
            )

            g.add(
                (
                    URIRef(metiers[profession_pere]),
                    URIRef(SKOS.prefLabel),
                    Literal(profession_pere)
                )
            )

            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["profession_pere"]),
                    URIRef(metiers[profession_pere])
                )
            )

        if pandas.notna(row["eleve_profession_mere"]):
            profession_mere = row["eleve_profession_mere"].strip().capitalize()
            g.add(
                (
                    URIRef(metiers[profession_mere]),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )

            g.add(
                (
                    URIRef(metiers[profession_mere]),
                    URIRef(SKOS.inScheme),
                    URIRef(ConceptSchemes['Métiers'])
                )
            )
            g.add(
                (
                    URIRef(ConceptSchemes['Métiers']),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(metiers[profession_mere])
                )
            )

            g.add(
                (
                    URIRef(metiers[profession_mere]),
                    URIRef(SKOS.prefLabel),
                    Literal(profession_mere)
                )
            )

            g.add(
                (
                    URIRef(uriEleve),
                    URIRef(HEMEF["profession_mere"]),
                    URIRef(metiers[profession_mere])
                )
            )

        # print(departements)

        # Creation des données géogaphiques

        if (pandas.notna(row["eleve_ville_naissance"])):
            uriVille = villes[row["eleve_ville_naissance"].strip(
            ).capitalize()]
            nomVille = row["eleve_ville_naissance"].strip(
            ).capitalize()
        else:
            nomVille = "Ville inconnue"
            uriVille = villes["Ville inconnue"]

        if (pandas.notna(row["eleve_departement_naissance"])):
            uriDep = departements[row["eleve_departement_naissance"].strip(
            ).capitalize()]
            nomDep = row["eleve_departement_naissance"].strip(
            ).capitalize()
        else:
            if pandas.notna(row["eleve_pays_naissance"]) :
                ssZoneDe = row["eleve_pays_naissance"]
            else : 
                ssZoneDe = 'France'
            
            nomDep = ssZoneDe + " - Département inconnu"
            uriDep = departements[nomDep]

        if pandas.notna(row["eleve_pays_naissance"]):
            uriPays = pays[row["eleve_pays_naissance"].strip().capitalize()]
            nomPays = row["eleve_pays_naissance"].strip().capitalize()
        else:
            uriPays = pays['France']
            nomPays = 'France'

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
                Literal(nomVille)
            )
        )

        g.add(
            (
                URIRef(uriVille),
                URIRef(SKOS.inScheme),
                URIRef(NoeudToponymes)
            )
        )

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
                Literal(nomDep)
            )
        )

        g.add(
            (
                URIRef(uriDep),
                URIRef(SKOS.inScheme),
                URIRef(NoeudToponymes)
            )
        )

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
                Literal(nomPays)
            )
        )

        g.add(
            (
                URIRef(uriPays),
                URIRef(SKOS.inScheme),
                URIRef(NoeudToponymes)
            )
        )

        g.add(
            (
                URIRef(NoeudToponymes),
                URIRef(SKOS.hasTopConcept),
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

        g.add(
            (
                URIRef(uriDep),
                URIRef(SKOS.broader),
                URIRef(uriPays)
            )
        )

        g.add(
            (
                URIRef(uriDep),
                URIRef(SKOS.narrower),
                URIRef(uriVille)
            )
        )

        g.add(
            (
                URIRef(uriVille),
                URIRef(SKOS.broader),
                URIRef(uriDep)
            )
        )

        # Lien ville - eleve
        g.add(
            (
                URIRef(uriEleve),
                URIRef(HEMEF["nait_a"]),
                URIRef(uriVille)
            )
        )

        if (pandas.notna(row["eleve_ville_naissance_ancien_nom"])):
            g.add(
                (
                    URIRef(uriVille),
                    URIRef(HEMEF["ancien_nom"]),
                    Literal(row["eleve_ville_naissance_ancien_nom"])
                )
            )

        # Gestion des précursus
        if (pandas.notna(row["pre-cursus_nom_etablissement"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["etablissement_pre_cursus"]),
                    Literal(row["pre-cursus_nom_etablissement"])
                )
            )

        if (pandas.notna(row["pre-cursus_type_etablissement_"])):
            g.add(
                (
                    URIRef(eleves[row['identifiant_1']]),
                    URIRef(HEMEF["type_etablissement_pre_cursus"]),
                    Literal(row["pre-cursus_type_etablissement_"])
                )
            )

        villePC = None
        if (pandas.notna(row["pre-cursus_ville_etablissement_"])):
            villePC = row["pre-cursus_ville_etablissement_"].strip().capitalize()
            if villePC in villes:
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["ville_pre_cursus"]),
                        URIRef(villes[villePC])
                    )
                )
            else:
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["ville_pre_cursus"]),
                        Literal(
                            row["pre-cursus_ville_etablissement_"].strip().capitalize())
                    )
                )

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
            if isinstance(row["cursus_date_entree_conservatoire"], datetime.date):
                date_time = str(
                    row["cursus_date_entree_conservatoire"]).split()
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["cursus_date_entree_conservatoire"]),
                        Literal(date_time[0], datatype=XSD.Date)
                    )
                )
            else:
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(
                            HEMEF["hypothese_cursus_date_entree_conservatoire"]),
                        Literal(row["cursus_date_entree_conservatoire"])
                    )
                )

        if (pandas.notna(row["cursus_date_sortie_conservatoire"])):
            if isinstance(row["cursus_date_sortie_conservatoire"], datetime.date):
                date = row["cursus_date_sortie_conservatoire"].date()
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(HEMEF["cursus_date_sortie_conservatoire"]),
                        Literal(date, datatype=XSD.Date)
                    )
                )
            else:
                g.add(
                    (
                        URIRef(eleves[row['identifiant_1']]),
                        URIRef(
                            HEMEF["hypothese_cursus_date_sortie_conservatoire"]),
                        Literal(row["cursus_date_sortie_conservatoire"])
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

        # Gestion des prix

        uriPrix = None
        if (pandas.notna(row["prix_date"]) and pandas.notna(row["prix_nom"])): 
            if pandas.notna(row["prix_discipline"]):
                id_prix = tuple((row['identifiant_1'], row["prix_date"], row["prix_nom"].strip().capitalize(), row["prix_discipline"].strip().capitalize()))
            else : id_prix = tuple((row['identifiant_1'], row["prix_date"], row["prix_nom"].strip().capitalize()))
            uriPrix = prix[id_prix]

            if (pandas.notna(row["prix_discipline"])):
                intitule_prix = str(
                    row["prix_nom"]) + " - " + str(row["prix_date"]) + " : " + str(row["prix_discipline"])
            else:
                intitule_prix = str(row["prix_nom"]) + \
                    " - " + str(row["prix_date"])

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
                        URIRef(prixNom[row['prix_nom'].strip().capitalize()]),
                        URIRef(is_a),
                        URIRef(SKOS.Concept)
                    )
                )

                g.add(
                    (
                        URIRef(prixNom[row['prix_nom'].strip().capitalize()]),
                        URIRef(SKOS.inScheme),
                        URIRef(NoeudPrixNom)
                    )
                )

                g.add(
                    (
                        URIRef(NoeudPrixNom),
                        URIRef(SKOS.hasTopConcept),
                        URIRef(prixNom[row['prix_nom'].strip().capitalize()]),
                    )
                )

                g.add(
                    (
                        URIRef(prixNom[row['prix_nom'].strip().capitalize()]),
                        URIRef(SKOS.prefLabel),
                        Literal(row['prix_nom'].strip().capitalize())
                    )
                )

                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['nom_prix']),
                        URIRef(prixNom[row['prix_nom'].strip().capitalize()])
                    )
                )
            if (pandas.notna(row["prix_nom_complément"])):
                g.add(
                    (
                        URIRef(prixNomComplément[row['prix_nom_complément'].strip().capitalize()]),
                        URIRef(is_a),
                        URIRef(SKOS.Concept)
                    )
                )

                g.add(
                    (
                        URIRef(prixNomComplément[row['prix_nom_complément'].strip().capitalize()]),
                        URIRef(SKOS.prefLabel),
                        Literal(row['prix_nom_complément'].strip().capitalize())
                    )
                )

                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['complément_nom_prix']),
                        URIRef(prixNomComplément[row['prix_nom_complément'].strip().capitalize()])
                    )
                )

                g.add(
                    (
                        URIRef(prixNomComplément[row['prix_nom_complément'].strip().capitalize()]),
                        URIRef(SKOS.inScheme),
                        URIRef(NoeudPrixNomComplément)
                    )
                )

                g.add(
                    (
                        URIRef(NoeudPrixNomComplément),
                        URIRef(SKOS.hasTopConcept),
                        URIRef(prixNomComplément[row['prix_nom_complément'].strip().capitalize()]),
                    )
                )

            # gYeat represents a specific calendar year. The letter g signifies "Gregorian." The format of xsd:gYear is CCYY
            if (len(str(row["prix_date"]).strip()) == 4):
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['année_prix']),
                        Literal(row["prix_date"], datatype=XSD.gYear)
                    )
                )
            else:
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['hypothese_année_prix']),
                        Literal(row["prix_date"])
                    )
                )

            # Checker si dans dico discipline si oui on lit à l'UUID sinon on créer une discipline, pas de lien à une string
            # voir avant avec Marie si distinction avec les disciplines de classes
            if (pandas.notna(row["prix_discipline"])):
                g.add(
                    (
                        URIRef(
                            prixDiscipline[row['prix_discipline'].strip().capitalize()]),
                        URIRef(is_a),
                        URIRef(SKOS.Concept)
                    )
                )

                g.add(
                    (
                        URIRef(
                            prixDiscipline[row['prix_discipline'].strip().capitalize()]),
                        URIRef(SKOS.inScheme),
                        URIRef(NoeudPrixDiscipline)
                    )
                )

                g.add(
                    (
                        URIRef(NoeudPrixDiscipline),
                        URIRef(SKOS.hasTopConcept),
                        URIRef(
                            prixDiscipline[row['prix_discipline'].strip().capitalize()]),
                    )
                )

                g.add(
                    (
                        URIRef(
                            prixDiscipline[row['prix_discipline'].strip().capitalize()]),
                        URIRef(SKOS.prefLabel),
                        Literal(row['prix_discipline'].strip().capitalize())
                    )
                )

                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['discipline_prix']),
                        URIRef(
                            prixDiscipline[row['prix_discipline'].strip().capitalize()])
                    )
                )

            if (pandas.notna(row["prix_type"])):
                g.add(
                    (
                        URIRef(
                            prixType[row['prix_type'].strip().capitalize()]),
                        URIRef(is_a),
                        URIRef(SKOS.Concept)
                    )
                )

                g.add(
                    (
                        URIRef(
                            prixType[row['prix_type'].strip().capitalize()]),
                        URIRef(SKOS.inScheme),
                        URIRef(NoeudPrixType)
                    )
                )

                g.add(
                    (
                        URIRef(NoeudPrixType),
                        URIRef(SKOS.hasTopConcept),
                        URIRef(
                            prixType[row['prix_type'].strip().capitalize()]),
                    )
                )

                g.add(
                    (
                        URIRef(
                            prixType[row['prix_type'].strip().capitalize()]),
                        URIRef(SKOS.prefLabel),
                        Literal(row['prix_type'].strip().capitalize())
                    )
                )

                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['type_prix']),
                        URIRef(prixType[row['prix_type'].strip().capitalize()])
                    )
                )
            if (pandas.notna(row["prix_rang"])):
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['rang_du_prix']),
                        Literal(row["prix_rang"])
                    )
                )

        # Gestion des classes
        Discipline = None
        if (pandas.notna(row['classe_discipline'])):
            Discipline = row['classe_discipline'].strip().capitalize()
            g.add(
                (
                    URIRef(
                        discipline[row['classe_discipline'].strip().capitalize()]),
                    URIRef(SKOS.inScheme),
                    URIRef(ConceptSchemes['Disciplines'])
                )
            )

            g.add(
                (
                    URIRef(ConceptSchemes['Disciplines']),
                    URIRef(SKOS.hasTopConcept),
                    URIRef(
                        discipline[row['classe_discipline'].strip().capitalize()])
                )
            )

            g.add(
                (
                    URIRef(
                        discipline[row['classe_discipline'].strip().capitalize()]),
                    URIRef(is_a),
                    URIRef(SKOS.Concept)
                )
            )

            g.add(
                (
                    URIRef(
                        discipline[row['classe_discipline'].strip().capitalize()]),
                    URIRef(SKOS.prefLabel),
                    Literal(row['classe_discipline'].strip().capitalize())
                )
            )

        Prof = None
        if (pandas.notna(row['classe_nom_professeur'])):
            Prof = row['classe_nom_professeur'].strip().capitalize()
            g.add(
                (
                    URIRef(
                        professeur[row['classe_nom_professeur'].strip().capitalize()]),
                    URIRef(is_a),
                    URIRef(HEMEF['Professeur'])
                )
            )

            g.add(
                (
                    URIRef(
                        professeur[row['classe_nom_professeur'].strip().capitalize()]),
                    URIRef(HEMEF['nom_professeur']),
                    Literal(row['classe_nom_professeur'].strip().capitalize())
                )
            )

        uriClasse = None
        if Discipline and Prof:
            id_classe = tuple((Discipline, Prof))
            uriClasse = classe[id_classe]
        elif Discipline:
            id_classe = Discipline
            uriClasse = classe[id_classe]
        if uriClasse:
            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(is_a),
                    URIRef(HEMEF['Classe'])
                )
            )
            if pandas.notna(row['classe_nom_professeur']):
                g.add(
                    (
                        URIRef(uriClasse),
                        URIRef(HEMEF['professeur']),
                        URIRef(professeur[Prof])
                    )
                )

            g.add(
                (
                    URIRef(uriClasse),
                    URIRef(HEMEF['discipline_enseignee']),
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

        if pandas.notna(row["classe_nom_professeur"]) and pandas.notna(row["identifiant_1"]) and pandas.notna(row["parcours_classe_date_entree"]) and pandas.notna(row["classe_discipline"]):
            id_parcours_classe = tuple((row["identifiant_1"], row["classe_nom_professeur"].strip().capitalize(
            ), row["parcours_classe_date_entree"], row["classe_discipline"].strip().capitalize()))
            uri_parcours_classe = parcours_classe[id_parcours_classe]
        else:
            # Il existe des éléments vides dans la clé, il va falloir bricoler d'après les cas identifiers
            if pandas.isna(row["classe_nom_professeur"]):
                if pandas.isna(row["parcours_classe_date_entree"]):
                    if pandas.isna(row["classe_discipline"]):
                        uri_parcours_classe = parcours_classe[row["identifiant_1"]]
                    else:
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["classe_discipline"].strip().capitalize()))
                        uri_parcours_classe = parcours_classe[id_parcours_classe]
                else:
                    if pandas.isna(row["classe_discipline"]):
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["parcours_classe_date_entree"]))
                        uri_parcours_classe = parcours_classe[id_parcours_classe]
                    else:
                        id_parcours_classe = tuple(
                            (row["identifiant_1"], row["parcours_classe_date_entree"], row["classe_discipline"].strip().capitalize()))
                        uri_parcours_classe = parcours_classe[id_parcours_classe]
            else:
                # dernier cas possible : seul row["parcours_classe_date_entree"] est vide
                id_parcours_classe = tuple((row["identifiant_1"], row["classe_nom_professeur"].strip(
                ).capitalize(), row["classe_discipline"].strip().capitalize()))
                uri_parcours_classe = parcours_classe[id_parcours_classe]

        g.add(
            (
                URIRef(uri_parcours_classe),
                URIRef(is_a),
                URIRef(HEMEF["Parcours_classe"])
            )
        )

        # modif : mtn lié a eleve et non plus a cursus
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

        if (pandas.notna(row['parcours_classe_date_entree'])):
            if isinstance(row['parcours_classe_date_entree'], datetime.date):
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["date_entree"]),
                        Literal(row['parcours_classe_date_entree'],
                                datatype=XSD.Date)
                    )
                )
            else:
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["hypothèse_date_entree"]),
                        Literal(row['parcours_classe_date_entree'])
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

        if (pandas.notna(row['parcours_classe_date_sortie'])):
            if isinstance(row['parcours_classe_date_sortie'], datetime.date):
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["date_sortie"]),
                        Literal(row['parcours_classe_date_sortie'],
                                datatype=XSD.Date)
                    )
                )
            else:
                g.add(
                    (
                        URIRef(uri_parcours_classe),
                        URIRef(HEMEF["hypothèse_date_sortie"]),
                        Literal(row['parcours_classe_date_sortie'])
                    )
                )

        if (pandas.notna(row['parcours_classe_motifs_sortie'])):
            g.add(
                (
                    URIRef(uri_parcours_classe),
                    URIRef(HEMEF["motif_sortie"]),
                    Literal(row['parcours_classe_motifs_sortie'])
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
            if str(row["prix_type"]).strip().capitalize()== 'Prix de Rome'.capitalize() or str(row["prix_type"]).strip().capitalize() == 'Grand Prix de Rome'.capitalize():
                g.add(
                    (
                        URIRef(uriPrix),
                        URIRef(HEMEF['recompense_eleve']),
                        URIRef(uriEleve)
                    )
                )
                g.add(
                    (
                        URIRef(uriEleve),
                        URIRef(HEMEF['prix_decerne']),
                        URIRef(uriPrix),
                    )
                )
            else:
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

################################################################################
################################################################################

with open(args.turtle, "w") as file:
    output = f"@base <{BASE_NS}> .\n" + g.serialize(
        format="turtle", base=BASE_NS
    ).decode("utf-8")
    file.write(output)

with open("registre.yaml", "w") as file:
    yaml.dump(registre, file)

print('fin script')

# with open("eleves_sans_pc.yaml", "w") as file:
#     yaml.dump(debug_eleve_sans_pc, file)
