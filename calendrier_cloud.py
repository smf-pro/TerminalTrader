# -*- coding: utf-8 -*-
"""
Script d'extraction du calendrier economique - Forex Factory (version cloud)
------------------------------------------------------------------------------
Version adaptee pour tourner sur GitHub Actions (cron toutes les 5 min),
calquee sur centralbanks_cloud.py, avec deux differences majeures liees a
la nature de cette source (voir cache_calendrier.py pour le detail) :

1. DEDUP PAR SNAPSHOT, PAS PAR "VU/PAS VU" : un evenement calendaire est
   republie a l'identique pendant toute la semaine (forecast/previous),
   puis `actual` se remplit a l'heure de la publication reelle. On ne
   reecrit Firestore que si forecast/previous/actual ont change depuis
   le dernier cycle (cache_calendrier.a_change), jamais un simple "deja
   vu -> on saute pour toujours" comme pour les news.

2. AUCUNE SUPPRESSION FIRESTORE : contrairement au cache local (purge
   apres DUREE_RETENTION_JOURS), les documents Firestore de la collection
   "eco_calendar" ne sont JAMAIS supprimes par ce script (choix valide
   avec l'utilisateur). Le filtrage des evenements trop anciens pour
   l'affichage se fait cote site (requete/filtre JS), pas ici.

Pas de traduction (deep_translator) : le "titre" d'un evenement macro
(ex: "Non-Farm Employment Change") reste en anglais, ce n'est pas un
article de presse.

Pas d'appel a site_generator.generer_json() : en v1, le calendrier n'a
pas d'archive JSON (voir contexte projet, decision explicite) - seule la
lecture Firestore temps reel alimente le site.
"""

import os
import hashlib
from datetime import datetime, timedelta, timezone

import requests

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_calendrier import charger_cache, sauvegarder_cache, marquer_traite, a_change

# ---------- CONFIGURATION ----------
URL_CALENDRIER = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

COLLECTION = "eco_calendar"
NOM_SOURCE = "ecocalendar"  # identifiant unique de ce script dans pipeline_status

# Mapping impact Forex Factory -> valeur normalisee stockee dans Firestore
# (coherent avec mapImpactFirestore cote site : high/medium/low ; Holiday
# est une categorie a part, pas un niveau d'impact statistique).
IMPACT_MAP = {
    "high": "high",
    "medium": "medium",
    "low": "low",
    "holiday": "holiday",
}


# ---------- INITIALISATION FIREBASE ----------
# Meme mecanisme que les 3 autres scripts : la cle de service est fournie
# via GOOGLE_APPLICATION_CREDENTIALS (secret FIREBASE_SERVICE_ACCOUNT).
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Identique aux 3 autres scripts : battement de coeur ecrit a CHAQUE
    cycle dans 'pipeline_status/ecocalendar', meme sans changement."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- RECUPERATION DU CALENDRIER ----------
def fetch_calendar():
    """Recupere le flux JSON non officiel Forex Factory (semaine en
    cours). Repris du script fourni, adapte pour utiliser `requests`
    (deja une dependance du projet) plutot que urllib."""
    reponse = requests.get(URL_CALENDRIER, headers=HEADERS, timeout=30)
    reponse.raise_for_status()
    data = reponse.json()
    if not isinstance(data, list):
        raise ValueError("Format de reponse inattendu (liste JSON attendue).")
    return data


def normaliser_valeur(v):
    """Le flux FF renvoie une chaine vide plutot que null quand une
    valeur (forecast/previous/actual) n'est pas encore connue. On
    normalise en None pour un stockage/une comparaison propres."""
    if v is None or v == "":
        return None
    return v


def parser_date_evenement(date_brute):
    """Le flux FF fournit une date ISO 8601 avec fuseau (ex:
    '2026-09-09T08:30:00-04:00'). Retourne None si le format est
    inattendu plutot que de planter tout le cycle pour un seul
    evenement mal forme."""
    if not date_brute:
        return None
    try:
        return datetime.fromisoformat(date_brute)
    except ValueError:
        return None


def hash_evenement(titre, pays, date_brute):
    """ID de document Firestore deterministe. Il n'y a pas d'URL propre
    a chaque evenement dans ce flux (contrairement aux news) : on hash
    donc la combinaison titre+pays+date, stable d'un cycle a l'autre pour
    un meme evenement, meme si son contenu (forecast/actual) change."""
    brut = f"{titre}|{pays}|{date_brute}"
    return hashlib.sha256(brut.encode("utf-8")).hexdigest()


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    try:
        evenements = fetch_calendar()
    except Exception as e:
        print(f"Erreur recuperation du calendrier : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(evenements)} evenement(s) recuperes du flux Forex Factory.")

    # Cache local de deduplication PAR SNAPSHOT (voir cache_calendrier.py) :
    # contrairement aux news, un evenement deja vu peut quand meme avoir
    # change (actual rempli apres publication) - on ne saute que si RIEN
    # n'a change depuis le dernier cycle.
    cache = charger_cache(NOM_SOURCE)

    evenements_ecrits = 0
    for ev in evenements:
        titre = ev.get("title") or "Sans titre"
        pays = ev.get("country") or ""
        date_brute = ev.get("date") or ""
        impact_brut = (ev.get("impact") or "").lower()
        impact = IMPACT_MAP.get(impact_brut, "low")
        forecast = normaliser_valeur(ev.get("forecast"))
        previous = normaliser_valeur(ev.get("previous"))
        actual = normaliser_valeur(ev.get("actual"))

        if not date_brute:
            print(f"Date manquante, evenement ignore : {titre}")
            continue

        event_id = hash_evenement(titre, pays, date_brute)

        # Rien n'a change depuis le dernier cycle -> on ne touche pas a
        # Firestore, inutile de gaspiller le quota d'ecriture.
        if not a_change(cache, event_id, forecast, previous, actual):
            continue

        date_evenement = parser_date_evenement(date_brute)

        try:
            doc_ref = db.collection(COLLECTION).document(event_id)
            doc_ref.set({
                "titre": titre,
                "pays": pays,
                "date": date_evenement,
                "date_brute": date_brute,
                "impact": impact,
                "precedent": previous,
                "prevision": forecast,
                "reel": actual,
                "date_maj": maintenant,
            }, merge=True)
            marquer_traite(cache, event_id, date_brute, forecast, previous, actual)
            evenements_ecrits += 1
            print(f"Ecrit/mis a jour dans Firestore : {titre} ({pays})")

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {evenements_ecrits} evenement(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(evenements),
                articles_nouveaux=evenements_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur l'evenement '{titre}' : {e}")

    # Sauvegarde du cache local (purge des evenements trop anciens,
    # jamais de suppression cote Firestore - voir cache_calendrier.py).
    sauvegarder_cache(NOM_SOURCE, cache)

    print(f"\nTermine. {evenements_ecrits} evenement(s) nouveau(x) ou mis a jour dans Firestore.")

    # Pas d'appel a generer_json(db) : le calendrier n'a pas d'archive
    # JSON en v1 (decision explicite, voir docstring du module).

    enregistrer_statut_pipeline(db, statut="ok", liens_vus=len(evenements), articles_nouveaux=evenements_ecrits)


if __name__ == "__main__":
    cycle()
