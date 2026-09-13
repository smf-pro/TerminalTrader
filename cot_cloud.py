#!/usr/bin/env python3
"""
Collecteur de données COT (Commitments of Traders) - pipeline cloud
====================================================================

Version "pipeline" de COT.py : au lieu de produire des fichiers xlsx/csv
locaux, ce script écrit l'historique COT dans Firestore, pour affichage en
temps réel sur le site MERIDIAN (onglet "COT"). COT.py (export manuel
xlsx/csv) reste inchangé et continue de fonctionner indépendamment.

Source : rapport "Traders in Financial Futures" (TFF) du CFTC, publié UNE
FOIS PAR SEMAINE (vendredi ~15h30 ET). Les deux variantes (futures_only /
combined) sont toujours collectées, chacune dans sa propre collection
Firestore.

MODELE DE STOCKAGE - PATRON REUTILISE (voir ff_indicator_history / 
INDICATEURS_SUIVIS cote site, meme principe) :
Un document Firestore = UNE publication hebdomadaire pour UN marche donne.
Chaque marche a son propre historique, trie par un champ "position" :
0 = publication la plus recente pour ce marche, 1 = semaine precedente,
etc. Ca permet au site de faire un listener simple par marche
(where(market)+orderBy(position), limit N) sans jamais avoir a relire tout
l'historique - et sans avoir besoin d'un champ de date en cle de tri (pas
de souci de format/fuseau horaire cote requete).

Avantage par rapport a une simple lecture/tri par date a chaque cycle :
le cout en lectures Firestore est proportionnel au nombre de MARCHES
(~12), pas au volume d'historique accumule. A cadence hebdomadaire, ce
cout est de toute facon negligeable (quelques dizaines de lectures/semaine
sur un quota de 50 000/jour), mais on garde le meme patron que le reste
du site pour la coherence et la simplicite de maintenance.

DEDUP : pas de cache_dedup.py ici (pas de liens a dedupliquer). La
"nouveaute" se determine en comparant les dates recuperees depuis l'API
CFTC aux dates deja presentes dans l'historique Firestore de chaque
marche - si la derniere publication est deja connue, rien n'est ecrit
(le batch de decalage de position n'est declenche QUE s'il y a au moins
une date reellement nouvelle).

Usage (cron hebdomadaire, voir cot_scraper.yml) :
    python cot_cloud.py
"""

import hashlib
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import firebase_admin
from firebase_admin import credentials, firestore

# ===================== CONFIGURATION CFTC (reprise de COT.py) =====================

CFTC_API_URLS = {
    "futures_only": "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
    "combined": "https://publicreporting.cftc.gov/resource/yw9f-hn96.json",
}

# Une collection Firestore par rapport (plutot qu'un champ report_type dans
# une collection unique) : garde les requetes simples (un seul filtre
# d'egalite : le marche) et correspond au selecteur "futures_only /
# combined" prevu cote site.
COLLECTIONS_FIRESTORE = {
    "futures_only": "cot_futures_only_history",
    "combined": "cot_combined_history",
}

FOREX_KEYWORDS = [
    "EURO FX",
    "JAPANESE YEN",
    "BRITISH POUND",
    "SWISS FRANC",
    "CANADIAN DOLLAR",
    "AUSTRALIAN DOLLAR",
    "NEW ZEALAND DOLLAR",
    "MEXICAN PESO",
    "BRAZILIAN REAL",
    "RUSSIAN RUBLE",
    "SOUTH AFRICAN RAND",
    "U.S. DOLLAR INDEX",
]

# Mêmes colonnes utiles que COT.py (voir ce fichier pour le détail des
# quirks de nommage CFTC) - "report_date_as_yyyy_mm_dd" et
# "market_and_exchange_names" sont traitées à part (clé + regroupement),
# pas stockées comme "colonne parmi d'autres".
COLONNES_NUMERIQUES = [
    "open_interest_all",
    "dealer_positions_long_all",
    "dealer_positions_short_all",
    "asset_mgr_positions_long",
    "asset_mgr_positions_short",
    "lev_money_positions_long",
    "lev_money_positions_short",
    "other_rept_positions_long",
    "other_rept_positions_short",
    "tot_rept_positions_long_all",
    "tot_rept_positions_short",
    "nonrept_positions_long_all",
    "nonrept_positions_short_all",
]

# Profondeur d'historique conservee PAR MARCHE (comme le limit(20) de
# ecouterIndicateursTempsReel cote site). A cadence hebdomadaire, 20
# publications = un peu moins de 5 mois d'historique glissant.
HISTORIQUE_MAX = 20

# Fenetre de secours interrogee a chaque cycle : largement superieure a 1
# semaine pour absorber un cycle manque (script en panne une semaine,
# rattrapage automatique au cycle suivant sans intervention manuelle).
FENETRE_SECOURS_JOURS = 40


def build_date_filter(start):
    """Construit la clause SoQL $where pour filtrer depuis une date de
    debut (reprise simplifiee de COT.py, on n'a pas besoin de --end ici)."""
    return f"report_date_as_yyyy_mm_dd >= '{start}T00:00:00.000'"


def fetch_cot_data(start, report_type, limit=5000):
    """Recupere les donnees COT recentes depuis l'API CFTC, filtrees sur
    les devises forex, depuis `start`. Identique dans l'esprit a
    fetch_cot_data() de COT.py, simplifie pour l'usage pipeline (pas de
    borne de fin, fenetre de secours fixe)."""
    api_url = CFTC_API_URLS[report_type]

    market_clause = " OR ".join(
        f"upper(market_and_exchange_names) like '%{kw}%'" for kw in FOREX_KEYWORDS
    )
    where_clause = f"({market_clause}) AND {build_date_filter(start)}"

    params = {
        "$where": where_clause,
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": limit,
    }

    print(f"[{report_type}] Interrogation de l'API CFTC...")
    response = requests.get(api_url, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    if not data:
        print(f"[{report_type}] Aucune donnee retournee pour cette fenetre.")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    print(f"[{report_type}] {len(df)} ligne(s) recuperee(s).")
    return df


def clean_dataframe(df):
    """Garde les colonnes utiles, convertit les types, calcule les
    positions nettes (repris de COT.py)."""
    if df.empty:
        return df

    colonnes_gardees = ["report_date_as_yyyy_mm_dd", "market_and_exchange_names"] + [
        c for c in COLONNES_NUMERIQUES if c in df.columns
    ]
    df = df[colonnes_gardees].copy()

    for col in COLONNES_NUMERIQUES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
        df["report_date_as_yyyy_mm_dd"]
    ).dt.date

    if {"lev_money_positions_long", "lev_money_positions_short"}.issubset(df.columns):
        df["lev_money_net_position"] = (
            df["lev_money_positions_long"] - df["lev_money_positions_short"]
        )
    if {"asset_mgr_positions_long", "asset_mgr_positions_short"}.issubset(df.columns):
        df["asset_mgr_net_position"] = (
            df["asset_mgr_positions_long"] - df["asset_mgr_positions_short"]
        )

    return df.sort_values(
        ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]
    ).reset_index(drop=True)


# ===================== FIRESTORE =====================


def init_firestore():
    """Initialise le client Firestore via le compte de service (meme
    variable d'environnement GOOGLE_APPLICATION_CREDENTIALS que les
    autres scripts *_cloud.py du projet, ecrite par le workflow GitHub
    Actions a partir du secret FIREBASE_SERVICE_ACCOUNT)."""
    chemin_cle = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    cred = credentials.Certificate(chemin_cle)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def id_document(collection, market, date_str):
    """ID de document deterministe (comme le hash SHA256 de l'URL pour
    les news) : evite tout doublon si le script est relance sur la meme
    semaine (ex: rattrapage manuel)."""
    brut = f"{collection}|{market}|{date_str}"
    return hashlib.sha256(brut.encode("utf-8")).hexdigest()


def serialiser_ligne(ligne, market, date_str):
    """Convertit une ligne pandas en dict pret pour Firestore. Necessaire
    car les valeurs numpy (int64, float64...) ne sont pas serialisables
    telles quelles par le SDK Firestore - d'ou l'appel a .item()."""
    champs = {
        "market_and_exchange_names": market,
        "report_date_as_yyyy_mm_dd": date_str,
        "position": 0,
    }
    for col in ligne.index:
        if col in ("market_and_exchange_names", "report_date_as_yyyy_mm_dd"):
            continue
        valeur = ligne[col]
        if pd.isna(valeur):
            champs[col] = None
        elif hasattr(valeur, "item"):
            champs[col] = valeur.item()
        else:
            champs[col] = valeur
    return champs


def recuperer_historique_existant(db, collection, market):
    """Lit l'historique deja stocke pour CE marche (une seule requete,
    filtree sur le marche, triee par position). Necessite un index
    composite market_and_exchange_names (Croissant) + position
    (Croissant) sur la collection - a creer manuellement (voir checklist
    projet), comme pour les collections de news."""
    docs = (
        db.collection(collection)
        .where("market_and_exchange_names", "==", market)
        .order_by("position")
        .limit(HISTORIQUE_MAX + 5)
        .stream()
    )
    return [
        {"id": d.id, "position": d.to_dict().get("position", 0),
         "date": d.to_dict().get("report_date_as_yyyy_mm_dd")}
        for d in docs
    ]


def mettre_a_jour_historique_marche(db, collection, market, market_df):
    """Ecrit dans Firestore les publications NOUVELLES pour ce marche
    (celles dont la date n'est pas deja connue), en maintenant le champ
    'position' (0 = plus recente) par decalage des documents existants.
    Retourne le nombre de nouvelles publications ecrites (0 si rien de
    neuf - cas le plus frequent, hors vendredi soir)."""
    market_df = market_df.sort_values("report_date_as_yyyy_mm_dd")
    dates_recuperees = [d.isoformat() for d in market_df["report_date_as_yyyy_mm_dd"]]

    existants = recuperer_historique_existant(db, collection, market)
    dates_existantes = {e["date"] for e in existants}

    nouvelles_dates = [d for d in dates_recuperees if d not in dates_existantes]
    if not nouvelles_dates:
        return 0

    # Etat en memoire des positions actuelles : evite de relire Firestore
    # a chaque nouvelle date traitee dans cette meme execution (utile si
    # plusieurs semaines de retard sont rattrapees d'un coup).
    positions_actuelles = {e["id"]: e["position"] for e in existants}

    for date_str in nouvelles_dates:  # du plus ancien au plus recent
        batch = db.batch()

        a_oublier = []
        for doc_id, position in positions_actuelles.items():
            nouvelle_position = position + 1
            ref = db.collection(collection).document(doc_id)
            if nouvelle_position >= HISTORIQUE_MAX:
                # Sort de la fenetre d'historique conservee -> purge.
                batch.delete(ref)
                a_oublier.append(doc_id)
            else:
                batch.update(ref, {"position": nouvelle_position})
        for doc_id in a_oublier:
            positions_actuelles.pop(doc_id, None)
        for doc_id in positions_actuelles:
            positions_actuelles[doc_id] += 1

        ligne = market_df[
            market_df["report_date_as_yyyy_mm_dd"].astype(str) == date_str
        ].iloc[0]
        nouveau_id = id_document(collection, market, date_str)
        batch.set(db.collection(collection).document(nouveau_id),
                   serialiser_ligne(ligne, market, date_str))
        batch.commit()

        positions_actuelles[nouveau_id] = 0

    return len(nouvelles_dates)


def enregistrer_statut_pipeline(db, nom_source, liens_vus, articles_nouveaux, statut, erreur=None):
    """Battement de coeur, meme format que les autres scripts du projet
    (pipeline_status/{nom_source})."""
    donnees = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        donnees["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(nom_source).set(donnees)


# ===================== CYCLE PRINCIPAL =====================

# Un battement de coeur separe par rapport (plutot qu'un seul "cot"
# global) : si futures_only echoue mais que combined fonctionne (ou
# l'inverse), on veut pouvoir le voir immediatement sur le site plutot
# que d'avoir un statut agrege qui masquerait lequel des deux est en
# panne - meme logique que les colonnes de news, une pastille par source.
NOMS_HEARTBEAT = {
    "futures_only": "cot-futures",
    "combined": "cot-combined",
}


def traiter_rapport(db, report_type):
    """Traite un seul rapport TFF (futures_only OU combined) : recupere
    la fenetre de secours, ecrit les nouvelles publications par marche,
    enregistre le statut pipeline (succes ET echec)."""
    collection = COLLECTIONS_FIRESTORE[report_type]
    heartbeat = NOMS_HEARTBEAT[report_type]
    liens_vus = 0
    articles_nouveaux = 0
    statut = "ok"
    erreur = None

    try:
        debut = (datetime.now(timezone.utc) - timedelta(days=FENETRE_SECOURS_JOURS)).strftime("%Y-%m-%d")
        raw_df = fetch_cot_data(debut, report_type)
        df = clean_dataframe(raw_df)
        liens_vus = len(df)

        if not df.empty:
            marches_en_erreur = 0
            for market in sorted(df["market_and_exchange_names"].unique()):
                market_df = df[df["market_and_exchange_names"] == market]
                try:
                    articles_nouveaux += mettre_a_jour_historique_marche(db, collection, market, market_df)
                except Exception as e:
                    # Un marche en erreur ne doit pas faire echouer tous
                    # les autres (meme esprit que le statut "ok_partiel"
                    # des autres scripts du projet).
                    marches_en_erreur += 1
                    print(f"[{report_type}] Erreur sur le marche '{market}' : {e}", file=sys.stderr)
            if marches_en_erreur:
                statut = "ok_partiel"
                erreur = f"{marches_en_erreur} marche(s) en erreur sur {df['market_and_exchange_names'].nunique()}"

    except requests.exceptions.RequestException as e:
        statut = "erreur"
        erreur = str(e)
        print(f"[{report_type}] Erreur reseau lors de l'appel a l'API CFTC : {e}", file=sys.stderr)
    except Exception as e:
        statut = "erreur"
        erreur = str(e)
        print(f"[{report_type}] Erreur inattendue : {e}", file=sys.stderr)

    enregistrer_statut_pipeline(db, heartbeat, liens_vus, articles_nouveaux, statut, erreur)
    print(f"[{report_type}] Termine : {articles_nouveaux} nouvelle(s) semaine(s) ecrite(s) "
          f"({liens_vus} ligne(s) recuperee(s) sur la fenetre de secours). Statut : {statut}")


def cycle():
    """Point d'entree unique, appele par le workflow GitHub Actions.
    Patron single-pass (pas de boucle infinie) : c'est le cron qui
    relance periodiquement, comme les autres scripts *_cloud.py du
    projet."""
    db = init_firestore()
    for report_type in COLLECTIONS_FIRESTORE:
        traiter_rapport(db, report_type)


if __name__ == "__main__":
    cycle()
