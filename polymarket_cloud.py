# -*- coding: utf-8 -*-
"""
Script d'extraction - Cotes de decision des banques centrales (Polymarket)
------------------------------------------------------------------------------
Meme principe general que rateprobability_cloud.py (voir ce fichier pour le
detail du raisonnement "diff de valeurs + historique append-only"), mais la
source ici est l'API JSON officielle et GRATUITE de Polymarket (Gamma API,
https://docs.polymarket.com), pas du HTML scrape. Pas de fragilite de parsing
HTML : on lit des champs JSON structures et documentes.

Difference de nature avec rateprobability :
rateprobability donne un TAUX IMPLICITE continu derive des marches de swaps
(OIS). Polymarket donne des COTES DE PARIS en argent reel sur des resultats
binaires ("Oui/Non, pas de changement", "Oui/Non, hausse de 25 bps", etc.).
Une meme reunion de banque centrale y est representee par PLUSIEURS marches
distincts (un par resultat possible), pas une seule ligne. On stocke donc
chaque sous-marche individuellement (cle = slug du marche Polymarket, deja
unique globalement), avec son propre suivi d'historique.

Principe de recuperation :
Polymarket n'expose pas un tag par banque centrale de facon fiable et stable
(les IDs numeriques de tag changent, et il n'existe pas de tag "banques
centrales" generique). On recupere donc tous les evenements actifs sous le
tag "Economy" (id=100328, verifie le 07/09/2026 via l'API), puis on filtre
COTE SCRIPT les evenements dont le titre correspond a une banque centrale
suivie (Fed, ECB, BoJ, BoE, BoC, RBA, RBNZ, SNB, Riksbank, RBI) - meme
logique de robustesse par mots-cles que pour rateprobability_cloud.py.

IMPORTANT - portee de cette version :
Comme rateprobability_cloud.py, ce script ecrit uniquement dans Firestore.
Pas d'appel a generer_json(), pas de modification de docs/ ni de
site_generator.py : l'affichage sur le site sera fait dans une etape
separee, une fois que stockage + historique des deux sources sont valides.
"""

import os
import re
import time
import json
import hashlib
from datetime import datetime, timedelta, timezone

import requests

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

# ---------- CONFIGURATION ----------
NOM_SOURCE = "polymarket"
COLLECTION_COURANT = "taux_polymarket"
COLLECTION_HISTORIQUE = "taux_polymarket_historique"
DOSSIER_CACHE = "cache"
CHEMIN_CACHE = os.path.join(DOSSIER_CACHE, f"{NOM_SOURCE}.json")
DUREE_RETENTION_JOURS = 7  # purge du cache local pour les marches clotures depuis > 7 jours

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
TAG_ID_ECONOMY = 100328  # verifie le 07/09/2026 via GET /events?slug=fed-decision-in-october
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Mots-cles (regex, insensible a la casse) pour reconnaitre une banque
# centrale dans le TITRE d'un evenement Polymarket. Verifies contre des
# titres reels observes le 07/09/2026 : "Fed Decision in September?",
# "ECB Interest Rates: October 2026", "Bank of Japan Decision in
# September?", "Bank of England Decision in September?".
BANQUES_POLYMARKET = [
    (re.compile(r"\bfomc\b|federal reserve|\bfed\b", re.I), "fed", "Federal Reserve"),
    (re.compile(r"european central bank|\becb\b", re.I), "ecb", "European Central Bank"),
    (re.compile(r"bank of japan|\bboj\b", re.I), "boj", "Bank of Japan"),
    (re.compile(r"bank of england|\bboe\b", re.I), "boe", "Bank of England"),
    (re.compile(r"bank of canada|\bboc\b", re.I), "boc", "Bank of Canada"),
    (re.compile(r"reserve bank of australia|\brba\b", re.I), "rba", "Reserve Bank of Australia"),
    (re.compile(r"reserve bank of new zealand|\brbnz\b", re.I), "rbnz", "Reserve Bank of New Zealand"),
    (re.compile(r"swiss national bank|\bsnb\b", re.I), "snb", "Swiss National Bank"),
    (re.compile(r"riksbank", re.I), "srb", "Riksbank"),
    (re.compile(r"reserve bank of india|\brbi\b", re.I), "rbi", "Reserve Bank of India"),
]

# Le titre doit aussi ressembler a un evenement de DECISION de taux (pas
# n'importe quel evenement "Economy" - CPI, PIB, chomage, etc. sont hors
# perimetre ici).
MOTIF_DECISION = re.compile(r"decision|interest rate|rate hike|rate cut", re.I)


# ---------- INITIALISATION FIREBASE (identique aux autres scripts) ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- CACHE LOCAL DE VALEURS (meme schema que rateprobability_cloud.py) ----------
def charger_cache_valeurs():
    if not os.path.exists(CHEMIN_CACHE):
        return {}
    try:
        with open(CHEMIN_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def sauvegarder_cache_valeurs(cache):
    """Purge les entrees dont le marche est cloture depuis plus de
    DUREE_RETENTION_JOURS (info stockee dans le cache a chaque ecriture)."""
    aujourdhui = datetime.now(timezone.utc).date()
    seuil = aujourdhui - timedelta(days=DUREE_RETENTION_JOURS)
    cache_purge = {}
    for cle, valeur in cache.items():
        date_cloture_iso = valeur.get("date_cloture")
        garder = True
        if date_cloture_iso:
            try:
                date_cloture = datetime.fromisoformat(date_cloture_iso.replace("Z", "+00:00")).date()
                garder = date_cloture >= seuil
            except ValueError:
                pass
        if garder:
            cache_purge[cle] = valeur

    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(CHEMIN_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache_purge, f, ensure_ascii=False, indent=2)


def hash_valeurs(valeurs):
    texte = json.dumps(valeurs, sort_keys=True)
    return hashlib.sha256(texte.encode("utf-8")).hexdigest()


# ---------- RECUPERATION API ----------
def recuperer_evenements_economy():
    """Recupere tous les evenements actifs sous le tag Economy, avec
    pagination (l'API limite a 100 par page)."""
    evenements = []
    offset = 0
    limite = 100
    while True:
        params = {
            "tag_id": TAG_ID_ECONOMY,
            "active": "true",
            "closed": "false",
            "limit": limite,
            "offset": offset,
        }
        reponse = requests.get(f"{GAMMA_BASE_URL}/events", params=params, headers=HEADERS, timeout=15)
        reponse.raise_for_status()
        page = reponse.json()
        if not page:
            break
        evenements.extend(page)
        if len(page) < limite:
            break
        offset += limite
        if offset > 1000:  # garde-fou anti-boucle infinie
            break
    return evenements


def identifier_banque(titre):
    for motif, code, nom in BANQUES_POLYMARKET:
        if motif.search(titre):
            return code, nom
    return None, None


def extraire_marches_banques_centrales(evenements):
    """Filtre les evenements par banque centrale + mot-cle de decision, et
    retourne une liste de dicts, un par SOUS-MARCHE (ex: 'Pas de
    changement', 'Hausse 25 bps') :
    {cle, banque, banque_nom, event_titre, question, probabilite, volume,
    date_cloture}"""
    resultats = []
    for evt in evenements:
        titre = evt.get("title", "")
        if not MOTIF_DECISION.search(titre):
            continue
        code, nom_banque = identifier_banque(titre)
        if code is None:
            continue

        for marche in evt.get("markets", []):
            slug_marche = marche.get("slug")
            if not slug_marche:
                continue
            try:
                outcomes = json.loads(marche.get("outcomes", "[]"))
                prix = json.loads(marche.get("outcomePrices", "[]"))
            except (json.JSONDecodeError, TypeError):
                continue
            if not outcomes or not prix or len(outcomes) != len(prix):
                continue

            # On garde la probabilite de l'issue "Oui" (1er outcome),
            # coherent avec la convention Polymarket (voir doc officielle).
            try:
                probabilite = round(float(prix[0]) * 100, 2)
            except (ValueError, IndexError):
                continue

            question = marche.get("groupItemTitle") or marche.get("question", "")

            resultats.append({
                "cle": slug_marche,
                "banque": code,
                "banque_nom": nom_banque,
                "event_titre": titre,
                "question": question,
                "probabilite": probabilite,
                "volume": marche.get("volumeNum"),
                "date_cloture": marche.get("endDate") or evt.get("endDate"),
            })

    return resultats


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    cache = charger_cache_valeurs()

    try:
        evenements = recuperer_evenements_economy()
    except Exception as e:
        print(f"Erreur recuperation des evenements Polymarket : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(evenements)} evenement(s) 'Economy' recupere(s) au total.")

    marches = extraire_marches_banques_centrales(evenements)
    print(f"{len(marches)} sous-marche(s) de decision de banque centrale identifie(s).")

    changements_ecrits = 0
    for m in marches:
        cle = m["cle"]
        valeurs_comparables = {"probabilite": m["probabilite"]}
        nouveau_hash = hash_valeurs(valeurs_comparables)
        entree_cache = cache.get(cle)

        if entree_cache is not None and entree_cache.get("hash") == nouveau_hash:
            continue  # probabilite identique au dernier cycle, rien a ecrire

        ancienne_probabilite = entree_cache.get("valeurs", {}).get("probabilite") if entree_cache else None

        try:
            db.collection(COLLECTION_COURANT).document(cle).set({
                "banque": m["banque"],
                "banque_nom": m["banque_nom"],
                "event_titre": m["event_titre"],
                "question": m["question"],
                "probabilite": m["probabilite"],
                "volume": m["volume"],
                "date_cloture": m["date_cloture"],
                "date_recuperation": maintenant,
            })

            db.collection(COLLECTION_HISTORIQUE).add({
                "banque": m["banque"],
                "banque_nom": m["banque_nom"],
                "event_titre": m["event_titre"],
                "question": m["question"],
                "horodatage": firestore.SERVER_TIMESTAMP,
                "ancienne_probabilite": ancienne_probabilite,
                "nouvelle_probabilite": m["probabilite"],
            })

            cache[cle] = {
                "hash": nouveau_hash,
                "valeurs": valeurs_comparables,
                "date_cloture": m["date_cloture"],
                "maj": maintenant.isoformat(),
            }
            changements_ecrits += 1

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse, arret du cycle : {e}")
            sauvegarder_cache_valeurs(cache)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(marches),
                articles_nouveaux=changements_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return

    sauvegarder_cache_valeurs(cache)
    print(f"\nTermine. {len(marches)} sous-marche(s) vu(s), {changements_ecrits} changement(s) ecrit(s).")

    enregistrer_statut_pipeline(db, statut="ok", liens_vus=len(marches), articles_nouveaux=changements_ecrits)


if __name__ == "__main__":
    cycle()
