# -*- coding: utf-8 -*-
"""
Script d'extraction - Taux implicites des banques centrales (rateprobability.com)
------------------------------------------------------------------------------
Contrairement aux autres scripts (ff_cloud.py, centralbanks_cloud.py,
investinglive_cloud.py) qui scrapent des ARTICLES a dedupliquer par URL,
celui-ci scrape des VALEURS NUMERIQUES (taux implicite, probabilite de
hausse/baisse) qui CHANGENT dans le temps pour un meme "objet" (une
reunion de banque centrale donnee). Le but n'est pas d'eviter de retraiter
un lien deja vu, mais de detecter quand une valeur a change pour
construire un historique de son evolution.

Principe general :
1. Scrape les 10 pages banque (rateprobability.com/fed, /ecb, etc.), qui
   contiennent chacune un tableau "reunion par reunion" (date, taux
   implicite, probabilite, delta en bps).
2. Pour chaque (banque, date_reunion), on calcule un hash des valeurs
   extraites et on le compare a la derniere valeur connue, stockee
   LOCALEMENT dans cache/rateprobability.json (jamais de lecture
   Firestore, meme logique que cache_dedup.py mais schema different :
   ici on doit comparer des VALEURS, pas juste verifier une presence).
3. Si le hash est identique au dernier cycle -> rien n'est ecrit dans
   Firestore (la valeur n'a pas bouge, inutile de gaspiller le quota
   d'ecriture).
4. Si le hash differe (ou objet jamais vu) -> on ecrit/ecrase le document
   "etat courant" dans la collection taux_implicites, ET on ajoute un
   nouveau document dans taux_implicites_historique (collection
   append-only, jamais ecrasee) qui garde une trace datee de l'ancienne
   et de la nouvelle valeur. C'est cet historique qui permet de suivre
   l'evolution des changements dans le temps.

IMPORTANT - portee de cette version :
Ce script ecrit uniquement dans Firestore. Il ne touche PAS a
site_generator.py, ni a docs/, ni a docs/index.html : l'affichage sur le
site MERIDIAN sera fait dans une etape separee. Pas d'appel a
generer_json() ici.

NOTE DE FIABILITE (a lire avant le premier test) :
Le parsing HTML ci-dessous est base sur la structure TEXTUELLE observee
via une recuperation de page (pas une inspection directe du HTML source
avec les vraies classes CSS, inaccessible depuis l'environnement de
developpement qui a prepare ce script). Un tableau HTML <table> est
suppose, identifie par le texte de ses en-tetes ("Meeting", "Implied
Rate", ...) plutot que par un nom de classe CSS (plus robuste si le site
change son style visuel, mais peut necessiter un ajustement si la
structure reelle differe). En cas d'echec de parsing sur une banque, le
script log un message clair et passe a la banque suivante sans faire
planter tout le cycle. Le tout premier lancement via workflow_dispatch
(voir checklist etape 8) fera foi.
"""

import os
import re
import time
import json
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

# ---------- CONFIGURATION ----------
NOM_SOURCE = "rateprobability"
COLLECTION_COURANT = "taux_implicites"
COLLECTION_HISTORIQUE = "taux_implicites_historique"
DOSSIER_CACHE = "cache"
CHEMIN_CACHE = os.path.join(DOSSIER_CACHE, f"{NOM_SOURCE}.json")
DUREE_RETENTION_JOURS = 7  # purge du cache local pour les reunions passees depuis > 7 jours

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

BANQUES = {
    "fed":  "Federal Reserve",
    "ecb":  "European Central Bank",
    "boj":  "Bank of Japan",
    "boe":  "Bank of England",
    "boc":  "Bank of Canada",
    "rba":  "Reserve Bank of Australia",
    "rbnz": "Reserve Bank of New Zealand",
    "snb":  "Swiss National Bank",
    "srb":  "Riksbank",
    "rbi":  "Reserve Bank of India",
}

BASE_URL = "https://rateprobability.com"


# ---------- INITIALISATION FIREBASE (identique aux autres scripts) ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Meme battement de coeur que les autres scripts, dans la meme
    collection pipeline_status (cle NOM_SOURCE = 'rateprobability').
    Ici 'liens_vus' = nb de reunions vues au total (toutes banques
    confondues), 'articles_nouveaux' = nb de changements de valeur reellement
    ecrits (historique) pendant ce cycle."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- CACHE LOCAL DE VALEURS (schema propre a ce script) ----------
# cache_dedup.py stocke {cle: date_ajout} pour repondre a "deja vu ou
# pas ?" (booleen). Ici on doit repondre a "cette valeur a-t-elle change
# depuis la derniere fois ?", donc on stocke {cle: {hash, valeurs, maj}}.
# Fichier separe (cache/rateprobability.json), meme convention de dossier
# et meme esprit de purge que cache_dedup.py, mais module propre pour ne
# pas modifier un fichier partage par les 3 autres scripts.

def charger_cache_valeurs():
    if not os.path.exists(CHEMIN_CACHE):
        return {}
    try:
        with open(CHEMIN_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def sauvegarder_cache_valeurs(cache):
    """Purge les entrees dont la reunion est passee depuis plus de
    DUREE_RETENTION_JOURS (une reunion passee ne sera plus jamais mise a
    jour par le site source, inutile de la garder indefiniment)."""
    aujourdhui = datetime.now(timezone.utc).date()
    seuil = aujourdhui - timedelta(days=DUREE_RETENTION_JOURS)
    cache_purge = {}
    for cle, valeur in cache.items():
        # cle = "{code_banque}_{date_reunion_iso}", ex: "fed_2026-01-28"
        try:
            date_reunion_iso = cle.split("_", 1)[1]
            date_reunion = datetime.fromisoformat(date_reunion_iso).date()
        except (ValueError, IndexError):
            continue
        if date_reunion >= seuil:
            cache_purge[cle] = valeur

    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(CHEMIN_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache_purge, f, ensure_ascii=False, indent=2)


def hash_valeurs(valeurs):
    """Hash stable des champs qui comptent pour detecter un changement
    (on ignore volontairement as_of/date_recuperation, qui bougent a
    chaque cycle sans que la donnee de fond ait change)."""
    texte = json.dumps(valeurs, sort_keys=True)
    return hashlib.sha256(texte.encode("utf-8")).hexdigest()


# ---------- PARSING ----------
MOIS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parser_date_reunion(texte):
    """Parse un texte du type 'Jan 28, 2026' -> date ISO '2026-01-28'.
    Retourne None si non reconnu (ligne ignoree plutot que de planter)."""
    m = re.search(r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})", texte)
    if not m:
        return None
    mois_texte, jour, annee = m.groups()
    mois_num = MOIS.get(mois_texte[:3].lower())
    if not mois_num:
        return None
    try:
        return f"{int(annee):04d}-{mois_num:02d}-{int(jour):02d}"
    except ValueError:
        return None


def parser_nombre(texte):
    """Extrait le premier nombre (signe/decimal inclus) d'un texte du
    type '3.60%', '(18.0%)', '-4.5', '(0.18)'. Retourne None si aucun
    nombre trouve."""
    if texte is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", texte.replace(",", "."))
    return float(m.group()) if m else None


def trouver_table_reunions(soup):
    """Cherche, parmi tous les <table> de la page, celui dont l'en-tete
    contient 'Meeting' ET 'Implied' (insensible a la casse) - on
    identifie la table par son CONTENU TEXTUEL plutot que par une classe
    CSS, pour rester robuste si le site change son style visuel."""
    for table in soup.find_all("table"):
        entete = table.find("tr")
        if not entete:
            continue
        texte_entete = entete.get_text(" ", strip=True).lower()
        if "meeting" in texte_entete and "implied" in texte_entete:
            return table
    return None


def extraire_as_of(texte_page):
    """Cherche un motif 'As of: <horodatage>' dans le texte brut de la
    page. Best-effort : renvoie le texte brut tel quel (pas de parsing en
    datetime, les formats de date/heure du site n'etant pas garantis
    d'une banque a l'autre), ou None si absent. Format attendu observe :
    'As of: 08:00 09/06/2026' (heure + date separees par un espace)."""
    m = re.search(r"As of:?\s*(\d{1,2}:\d{2}\s+\d{1,2}/\d{1,2}/\d{4})", texte_page)
    return m.group(1).strip() if m else None


def scraper_banque(code):
    """Scrape la page d'une banque centrale et retourne une liste de
    dicts, un par reunion a venir :
    {date_reunion, taux_implicite, probabilite, nb_mouvements, delta_bps, as_of}
    Leve une exception si la page est inaccessible ou si la table n'est
    pas trouvee (charge a l'appelant de logguer/continuer)."""
    url = f"{BASE_URL}/{code}"
    reponse = requests.get(url, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    table = trouver_table_reunions(soup)
    if table is None:
        raise ValueError(f"Table 'reunion par reunion' introuvable sur {url}")

    as_of = extraire_as_of(soup.get_text(" ", strip=True))

    reunions = []
    lignes = table.find_all("tr")[1:]  # on saute la ligne d'en-tete
    for ligne in lignes:
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < 5:
            continue
        textes = [c.get_text(" ", strip=True) for c in cellules]

        date_reunion = parser_date_reunion(textes[0])
        if date_reunion is None:
            continue  # ligne inattendue (pas une vraie ligne de reunion)

        reunions.append({
            "date_reunion": date_reunion,
            "taux_implicite": parser_nombre(textes[1]),
            "probabilite": parser_nombre(textes[2]),
            "nb_mouvements": parser_nombre(textes[3]),
            "delta_bps": parser_nombre(textes[4]),
            "as_of": as_of,
        })

    return reunions


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    cache = charger_cache_valeurs()

    reunions_vues = 0
    changements_ecrits = 0
    banques_en_echec = []

    for code, nom_banque in BANQUES.items():
        try:
            reunions = scraper_banque(code)
        except Exception as e:
            print(f"Erreur scraping {nom_banque} ({code}) : {e}")
            banques_en_echec.append(code)
            continue

        print(f"{nom_banque} ({code}) : {len(reunions)} reunion(s) trouvee(s).")
        reunions_vues += len(reunions)

        for r in reunions:
            cle = f"{code}_{r['date_reunion']}"

            valeurs_comparables = {
                "taux_implicite": r["taux_implicite"],
                "probabilite": r["probabilite"],
                "nb_mouvements": r["nb_mouvements"],
                "delta_bps": r["delta_bps"],
            }
            nouveau_hash = hash_valeurs(valeurs_comparables)
            entree_cache = cache.get(cle)

            if entree_cache is not None and entree_cache.get("hash") == nouveau_hash:
                continue  # valeur identique au dernier cycle, rien a ecrire

            anciennes_valeurs = entree_cache.get("valeurs") if entree_cache else None

            try:
                # Etat courant : ecrase a chaque changement detecte.
                db.collection(COLLECTION_COURANT).document(cle).set({
                    "banque": code,
                    "banque_nom": nom_banque,
                    "date_reunion": r["date_reunion"],
                    "as_of": r["as_of"],
                    "date_recuperation": maintenant,
                    **valeurs_comparables,
                })

                # Historique : append-only, une entree par changement reel.
                db.collection(COLLECTION_HISTORIQUE).add({
                    "banque": code,
                    "banque_nom": nom_banque,
                    "date_reunion": r["date_reunion"],
                    "horodatage": firestore.SERVER_TIMESTAMP,
                    "anciennes_valeurs": anciennes_valeurs,  # None si jamais vu avant
                    "nouvelles_valeurs": valeurs_comparables,
                })

                cache[cle] = {
                    "hash": nouveau_hash,
                    "valeurs": valeurs_comparables,
                    "maj": maintenant.isoformat(),
                }
                changements_ecrits += 1
                print(f"  -> Changement ecrit : {cle}")

            except ResourceExhausted as e:
                print(f"Quota Firestore depasse, arret du cycle : {e}")
                sauvegarder_cache_valeurs(cache)
                enregistrer_statut_pipeline(
                    db, statut="erreur",
                    liens_vus=reunions_vues,
                    articles_nouveaux=changements_ecrits,
                    erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
                )
                return

        time.sleep(1)  # courtoisie envers le site source entre 2 banques

    sauvegarder_cache_valeurs(cache)

    print(f"\nTermine. {reunions_vues} reunion(s) vue(s), {changements_ecrits} changement(s) ecrit(s).")

    if banques_en_echec and len(banques_en_echec) == len(BANQUES):
        statut = "erreur"
    elif banques_en_echec:
        statut = "ok_partiel"
    else:
        statut = "ok"

    erreur = f"Echec sur : {', '.join(banques_en_echec)}" if banques_en_echec else None
    enregistrer_statut_pipeline(
        db, statut=statut,
        liens_vus=reunions_vues,
        articles_nouveaux=changements_ecrits,
        erreur=erreur,
    )


if __name__ == "__main__":
    cycle()
