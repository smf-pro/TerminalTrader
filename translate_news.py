# -*- coding: utf-8 -*-
"""
Script de traduction FR des news deja collectees - TerminalTrader (cloud)
------------------------------------------------------------------------------
Retraduit en francais les news des collections "ff_news" et "cb_articles",
qui ne sont plus traduites depuis que ff_cloud.py / centralbanks_cloud.py /
investinglive_cloud.py ne stockent plus que la langue d'origine (sept.
2026, champs "titre_fr"/"extrait_fr"/"contenu_fr" retires du scraping).

Ce script est SEPARE des 3 scrapers : il ne scrape rien lui-meme, il relit
Firestore et complete les documents existants avec les champs traduits
("titre_fr", "extrait_fr" pour ff_news ; "titre_fr", "contenu_fr" pour
cb_articles), via un merge=True qui ne touche a aucun autre champ.

Pourquoi un curseur de progression (cache_traduction.py) plutot qu'un
cache "deja vu" comme cache_dedup.py :
- Objectif = retraduire TOUT l'historique existant en plus des nouvelles
  news au fil de l'eau. Un curseur (date_publication du dernier document
  traite) permet de balayer chronologiquement toute la collection au fil
  des runs successifs (rattrapage du backlog), puis de rester "colle" aux
  nouvelles news une fois le retard rattrape - un seul mecanisme pour les
  deux cas, meme principe que le curseur incremental de
  site_generator.py (_generer_archive_jour_courant).
- Chaque run traite au maximum TAILLE_LOT documents par collection (voir
  plus bas), pour rester dans des temps d'execution et des quotas
  Firestore raisonnables ; le retard se rattrape progressivement sur
  plusieurs runs successifs (cron), pas en un seul passage.

Moteur de traduction : deep_translator.GoogleTranslator (gratuit), avec
plusieurs ameliorations par rapport a l'ancien usage du pipeline :
- Decoupage du texte en morceaux sous la limite de caracteres de l'API
  (evite les troncatures silencieuses sur les articles longs).
- Traduction paragraphe par paragraphe (separateur "\n\n" deja utilise
  par centralbanks_cloud.py pour structurer le contenu), pour garder la
  mise en forme d'origine.
- Retries avec pause croissante en cas d'erreur reseau/quota cote Google
  Translate.
- Un document n'est ECRIT dans Firestore que si TOUTES ses traductions
  ont reussi (jamais de traduction partielle enregistree) ; en cas
  d'echec, le curseur n'avance pas au-dela de ce document, il sera
  retente au prochain run.

Champs deja traduits (ex: vieux documents qui avaient encore titre_fr
d'avant septembre) ne sont PAS retraduits : si le champ destination est
deja non vide, il est laisse tel quel.
"""

import os
import re
import time
from datetime import datetime

from deep_translator import GoogleTranslator

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_traduction import charger_progression, obtenir_curseur, avancer_curseur, sauvegarder_progression

# ---------- CONFIGURATION ----------
NOM_SOURCE = "traduction"  # identifiant unique de ce script dans pipeline_status

# Collections a traiter, avec la liste des paires (champ source, champ
# traduit a produire) a traduire pour chacune.
COLLECTIONS_CONFIG = {
    "ff_news": [("titre", "titre_fr"), ("extrait", "extrait_fr")],
    "cb_articles": [("titre", "titre_fr"), ("contenu", "contenu_fr")],
}

# Nombre maximum de documents traites par collection, a CHAQUE run. Le
# backlog historique se rattrape progressivement sur plusieurs runs
# successifs (cron) plutot qu'en un seul passage tres long.
TAILLE_LOT = 40

# Limite de caracteres par appel a Google Translate (marge de securite
# sous la limite reelle de l'API, ~5000 caracteres).
LIMITE_CARACTERES = 4500

# Pause entre deux appels de traduction, pour ne pas se faire bloquer par
# Google Translate en cas d'usage intensif (rattrapage de gros backlog).
DELAI_ENTRE_APPELS = 0.6

# Nombre de tentatives avant d'abandonner la traduction d'un morceau de
# texte (pause croissante entre chaque tentative).
TENTATIVES_MAX = 3


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, documents_vus=0, documents_traduits=0, erreur=None):
    """Meme mecanisme de battement de coeur que les 3 scrapers, dans
    'pipeline_status/traduction'."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": documents_vus,
        "articles_nouveaux": documents_traduits,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- TRADUCTION ----------
def _decouper_texte(texte, limite=LIMITE_CARACTERES):
    """Decoupe un texte trop long en morceaux <= `limite` caracteres, en
    coupant de preference a une fin de phrase ('. '), sinon au dernier
    espace disponible avant la limite, jamais au milieu d'un mot."""
    if len(texte) <= limite:
        return [texte]

    morceaux = []
    reste = texte
    while len(reste) > limite:
        coupure = reste.rfind(". ", 0, limite)
        if coupure != -1:
            coupure += 2  # inclure le point et l'espace dans le morceau courant
        else:
            coupure = reste.rfind(" ", 0, limite)
            if coupure != -1:
                coupure += 1
            else:
                coupure = limite  # aucun espace trouve (texte tres dense) : coupure brute
        morceaux.append(reste[:coupure])
        reste = reste[coupure:]
    if reste:
        morceaux.append(reste)
    return morceaux


def _nettoyer(texte):
    """Nettoyage post-traduction minimal et sans risque : espaces
    multiples et sauts de ligne excessifs, rien qui touche a la
    ponctuation (Google Translate applique deja les conventions
    typographiques francaises correctement)."""
    if not texte:
        return texte
    texte = re.sub(r"[ \t]+", " ", texte)
    texte = re.sub(r" ?\n ?", "\n", texte)
    texte = re.sub(r"\n{3,}", "\n\n", texte)
    return texte.strip()


def _traduire_un_morceau(texte):
    dernier_erreur = None
    for essai in range(TENTATIVES_MAX):
        try:
            resultat = GoogleTranslator(source="auto", target="fr").translate(texte)
            if resultat:
                return resultat
        except Exception as e:
            dernier_erreur = e
            time.sleep(2 * (essai + 1))
    print(f"Echec de traduction d'un morceau de texte apres {TENTATIVES_MAX} tentative(s) : {dernier_erreur}")
    return None


def traduire_texte(texte):
    """Traduit un texte (court comme un titre, ou long avec plusieurs
    paragraphes separes par '\n\n') vers le francais. Retourne None si
    une seule partie du texte echoue, plutot que d'ecrire une traduction
    partielle en base."""
    if not texte or not texte.strip():
        return texte

    paragraphes_traduits = []
    for paragraphe in texte.split("\n\n"):
        if not paragraphe.strip():
            paragraphes_traduits.append(paragraphe)
            continue

        morceaux_traduits = []
        for morceau in _decouper_texte(paragraphe):
            traduit = _traduire_un_morceau(morceau)
            if traduit is None:
                return None
            morceaux_traduits.append(traduit)
            time.sleep(DELAI_ENTRE_APPELS)

        paragraphes_traduits.append("".join(morceaux_traduits))

    return _nettoyer("\n\n".join(paragraphes_traduits))


# ---------- TRAITEMENT D'UNE COLLECTION ----------
def traiter_collection(db, collection, champs, progression):
    curseur_iso = obtenir_curseur(progression, collection)
    curseur_dt = datetime.fromisoformat(curseur_iso)

    requete = (
        db.collection(collection)
        .where("date_publication", ">", curseur_dt)
        .order_by("date_publication", direction="ASCENDING")
        .limit(TAILLE_LOT)
    )
    documents = list(requete.stream())

    if not documents:
        return 0, 0

    vus = 0
    traduits = 0

    for doc in documents:
        data = doc.to_dict()
        vus += 1

        date_pub = data.get("date_publication")
        date_pub_iso = date_pub.isoformat() if hasattr(date_pub, "isoformat") else curseur_iso

        deja_traduit = all(data.get(champ_dest) for (_, champ_dest) in champs)
        if deja_traduit:
            avancer_curseur(progression, collection, date_pub_iso)
            continue

        updates = {}
        echec = False
        for champ_src, champ_dest in champs:
            if data.get(champ_dest):
                continue  # ce champ precis est deja traduit, on ne le retouche pas
            texte_src = data.get(champ_src, "")
            if not texte_src:
                continue
            traduit = traduire_texte(texte_src)
            if traduit is None:
                echec = True
                break
            updates[champ_dest] = traduit

        if echec:
            print(f"Echec de traduction sur le document {doc.id} ({collection}), arret de ce lot ici (reessai au prochain run).")
            break  # le curseur n'avance PAS au-dela de ce document

        if updates:
            try:
                doc.reference.set(updates, merge=True)
                traduits += 1
                print(f"Traduit ({collection}) : {str(data.get('titre', ''))[:60]}")
            except ResourceExhausted as e:
                print(f"Quota Firestore depasse en ecriture, arret : {e}")
                break
            except Exception as e:
                print(f"Erreur d'ecriture Firestore sur {doc.id} : {e}")
                break

        avancer_curseur(progression, collection, date_pub_iso)

    return vus, traduits


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    progression = charger_progression()

    total_vus = 0
    total_traduits = 0
    erreur_globale = None

    for collection, champs in COLLECTIONS_CONFIG.items():
        try:
            vus, traduits = traiter_collection(db, collection, champs, progression)
            total_vus += vus
            total_traduits += traduits
        except ResourceExhausted as e:
            print(f"Quota Firestore depasse en lecture sur {collection} : {e}")
            erreur_globale = e
            break
        except Exception as e:
            print(f"Erreur sur la collection {collection} : {e}")
            erreur_globale = e

    sauvegarder_progression(progression)

    print(f"\nTermine. {total_vus} document(s) examine(s), {total_traduits} traduit(s).")

    if erreur_globale:
        enregistrer_statut_pipeline(
            db, statut="erreur",
            documents_vus=total_vus, documents_traduits=total_traduits,
            erreur=erreur_globale,
        )
    else:
        enregistrer_statut_pipeline(
            db, statut="ok",
            documents_vus=total_vus, documents_traduits=total_traduits,
        )


if __name__ == "__main__":
    cycle()
