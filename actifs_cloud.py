# -*- coding: utf-8 -*-
"""
Historique des prix horaires (bougies 1h) d'une liste d'actifs — version cloud
------------------------------------------------------------------------------
Version single-pass pour tourner sur GitHub Actions (cron horaire).

Objectif : accumuler SANS LIMITE dans Firestore l'historique des bougies
horaires (Open/High/Low/Close) de ~100 actifs (forex, indices, matières
premières, cryptos, taux, ETF, actions), pour afficher leur évolution sur
le site MERIDIAN.

Source : yfinance (Yahoo Finance), intervalle 1h.
  - Yahoo ne donne le 1h que sur ~730 jours maximum : le tout premier run
    (mode "backfill", JOURS_HISTORIQUE=700) remplit le passé disponible,
    ensuite chaque run horaire ajoute les nouvelles bougies.
  - auto_adjust=False : on stocke les prix BRUTS. Avec une accumulation
    dans le temps, des prix ajustés (dividendes/splits) rendraient
    l'historique incohérent d'un run à l'autre.

Stockage Firestore :
  - Collection "asset_history" : UN DOCUMENT PAR ACTIF ET PAR MOIS (UTC),
    ID = "<SLUG>_<AAAA-MM>" (ex: "EURUSD_2026-09"). Champs :
      slug, ticker, nom, categorie, mois, derniere_maj,
      points : { "t<timestamp_unix>": {o, h, l, c}, ... }
    Les points sont écrits en set(merge=True) : c'est idempotent (la
    bougie en cours est simplement réécrite au run suivant, jamais de
    doublon) et ça ne demande AUCUNE lecture Firestore. Le préfixe "t"
    évite des noms de champs purement numériques.
    Chaque document reste petit (~720 points max/mois) ; le site charge
    les mois passés une seule fois et n'écoute que le mois en cours.
    Requête côté site : where("slug", "==", X) (un seul champ d'égalité,
    donc AUCUN index composite nécessaire), tri par "mois" côté client.
  - Collection "asset_latest", document "all" : dernier prix de chaque
    actif, map { SLUG: {prix, ts, ticker} } fusionnée (merge) à chaque
    cycle — un seul document à écouter pour un tableau de bord.
  - "pipeline_status/actifs" : heartbeat à chaque cycle (même patron que
    fxforex_cloud.py / centralbanks_cloud.py).

Marchés fermés (week-ends, jours fériés) :
  Rien n'est inventé : yfinance ne renvoie simplement aucune nouvelle bougie
  pour un marché fermé, la dernière bougie ne change pas, la signature
  locale est identique et AUCUNE écriture n'est faite pour cet actif. Comme
  la fenêtre de téléchargement (14 jours) contient toujours les dernières
  bougies avant la fermeture, l'actif reste compté comme "récupéré" et le
  statut reste "ok". Le dernier prix d'un marché fermé garde son horodatage
  d'origine (champ "ts" de asset_latest) : le site peut ainsi afficher
  "marché fermé / dernier prix il y a X h" en comparant ts à l'heure actuelle.

Sécurité / robustesse :
  - Si un lot yfinance échoue (limitation de débit fréquente depuis les IP
    partagées de GitHub Actions), rien n'est écrasé : les actifs concernés
    sont simplement listés comme manquants et le statut passe à ok_partiel.
  - Signatures locales (cache/actifs_signatures.json) : si la dernière
    bougie d'un actif n'a pas changé (marché fermé), on n'écrit rien.
    Une signature n'est mise à jour qu'APRÈS un commit Firestore réussi.
  - Coût Firestore par cycle : au plus ~1 écriture par actif (+2), soit
    ~2 500/jour pour 100 actifs (quota gratuit : 20 000/jour). Zéro lecture.
    Les commits sont petits (20 documents / 500 Ko max) et réessayés en cas de
    504 Deadline Exceeded : indispensable pour le backfill de ~2 500 documents.

Variable d'environnement optionnelle :
  JOURS_HISTORIQUE : nombre de jours à (re)télécharger (défaut 14 ; max 700).
  Au-delà de 14, le script passe en mode "backfill" : il ignore les
  signatures et réécrit tout ce qu'il télécharge (idempotent).
"""

import os
import json
import time
import math
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import (
    ResourceExhausted, DeadlineExceeded, GatewayTimeout, ServiceUnavailable,
    Aborted, InternalServerError, RetryError,
)

# ---------- CONFIGURATION ----------

NOM_SOURCE = "actifs"
COLLECTION_HISTORIQUE = "asset_history"
COLLECTION_DERNIER = "asset_latest"
DOC_ID_DERNIER = "all"

# Fenêtre d'un run normal. 14 jours : couvre les week-ends ET les longs jours fériés
# (ex: Golden Week chinoise, ~9 jours de fermeture) pour qu'un marché fermé ne soit
# jamais compté comme 'sans donnée', et rattrape aussi une courte panne du workflow.
JOURS_HISTORIQUE_DEFAUT = 14
JOURS_HISTORIQUE_MAX = 700    # Yahoo : le 1h est limité à 730 jours

TAILLE_LOT_TICKERS = 25            # tickers par appel yfinance
PAUSE_ENTRE_LOTS_SECONDES = 2
NB_TENTATIVES_LOT = 3

# Batchs volontairement PETITS : chaque point d'un document est indexé champ par champ
# (4 champs x ~750 points par mois = ~3 000 entrées d'index par document). Des commits
# de plusieurs Mo faisaient dépasser le délai de Firestore ("504 Deadline Exceeded")
# lors du backfill. ~500 Ko / 20 documents max par commit passe sans problème.
TAILLE_MAX_BATCH_OPERATIONS = 20
TAILLE_MAX_BATCH_OCTETS = 500_000
TIMEOUT_COMMIT_SECONDES = 120
NB_TENTATIVES_COMMIT = 4           # l'écriture est idempotente (set merge) : on peut réessayer
ERREURS_TRANSITOIRES = (DeadlineExceeded, GatewayTimeout, ServiceUnavailable,
                        Aborted, InternalServerError, RetryError)

DOSSIER_CACHE = "cache"
FICHIER_SIGNATURES = os.path.join(DOSSIER_CACHE, "actifs_signatures.json")

# (slug, ticker Yahoo, nom affiché, catégorie)
_ACTIFS_BRUTS = [
    # Forex — majeures
    ("EURUSD", "EURUSD=X", "EUR/USD", "forex_majeures"),
    ("GBPUSD", "GBPUSD=X", "GBP/USD", "forex_majeures"),
    ("USDJPY", "USDJPY=X", "USD/JPY", "forex_majeures"),
    ("USDCHF", "USDCHF=X", "USD/CHF", "forex_majeures"),
    ("USDCAD", "USDCAD=X", "USD/CAD", "forex_majeures"),
    ("AUDUSD", "AUDUSD=X", "AUD/USD", "forex_majeures"),
    ("NZDUSD", "NZDUSD=X", "NZD/USD", "forex_majeures"),
    # Forex — exotiques
    ("USDMXN", "USDMXN=X", "USD/MXN", "forex_exotiques"),
    ("USDZAR", "USDZAR=X", "USD/ZAR", "forex_exotiques"),
    ("USDTRY", "USDTRY=X", "USD/TRY", "forex_exotiques"),
    ("USDSGD", "USDSGD=X", "USD/SGD", "forex_exotiques"),
    ("USDNOK", "USDNOK=X", "USD/NOK", "forex_exotiques"),
    ("USDSEK", "USDSEK=X", "USD/SEK", "forex_exotiques"),
    ("USDHKD", "USDHKD=X", "USD/HKD", "forex_exotiques"),
    ("USDBRL", "USDBRL=X", "USD/BRL", "forex_exotiques"),
    ("USDCNY", "CNY=X", "USD/CNY", "forex_exotiques"),
    ("USDINR", "INR=X", "USD/INR", "forex_exotiques"),
    # Indice dollar
    ("DXY", "DX-Y.NYB", "Indice dollar (DXY)", "indice_dollar"),
    # Indices — États-Unis
    ("SPX", "^GSPC", "S&P 500", "indices_us"),
    ("DJI", "^DJI", "Dow Jones", "indices_us"),
    ("IXIC", "^IXIC", "Nasdaq Composite", "indices_us"),
    ("NDX", "^NDX", "Nasdaq 100", "indices_us"),
    ("RUT", "^RUT", "Russell 2000", "indices_us"),
    ("VIX", "^VIX", "VIX", "indices_us"),
    # Indices — Europe
    ("DAX", "^GDAXI", "DAX", "indices_europe"),
    ("CAC40", "^FCHI", "CAC 40", "indices_europe"),
    ("FTSE100", "^FTSE", "FTSE 100", "indices_europe"),
    ("ESTX50", "^STOXX50E", "Euro Stoxx 50", "indices_europe"),
    ("IBEX35", "^IBEX", "IBEX 35", "indices_europe"),
    ("SMI", "^SSMI", "SMI", "indices_europe"),
    ("AEX", "^AEX", "AEX", "indices_europe"),
    # Indices — Asie-Pacifique
    ("N225", "^N225", "Nikkei 225", "indices_asie"),
    ("HSI", "^HSI", "Hang Seng", "indices_asie"),
    ("SSEC", "000001.SS", "Shanghai Composite", "indices_asie"),
    ("KOSPI", "^KS11", "KOSPI", "indices_asie"),
    ("ASX200", "^AXJO", "ASX 200", "indices_asie"),
    ("NIFTY50", "^NSEI", "Nifty 50", "indices_asie"),
    ("SENSEX", "^BSESN", "Sensex", "indices_asie"),
    # Métaux
    ("GOLD", "GC=F", "Or", "metaux"),
    ("SILVER", "SI=F", "Argent", "metaux"),
    ("PLATINUM", "PL=F", "Platine", "metaux"),
    ("PALLADIUM", "PA=F", "Palladium", "metaux"),
    ("COPPER", "HG=F", "Cuivre", "metaux"),
    # Énergie
    ("WTI", "CL=F", "Pétrole WTI", "energie"),
    ("BRENT", "BZ=F", "Pétrole Brent", "energie"),
    ("NATGAS", "NG=F", "Gaz naturel", "energie"),
    ("HEATOIL", "HO=F", "Fioul", "energie"),
    ("GASOLINE", "RB=F", "Essence", "energie"),
    # Agricoles
    ("CORN", "ZC=F", "Maïs", "agricoles"),
    ("WHEAT", "ZW=F", "Blé", "agricoles"),
    ("SOYBEAN", "ZS=F", "Soja", "agricoles"),
    ("COFFEE", "KC=F", "Café", "agricoles"),
    ("SUGAR", "SB=F", "Sucre", "agricoles"),
    ("COCOA", "CC=F", "Cacao", "agricoles"),
    ("COTTON", "CT=F", "Coton", "agricoles"),
    # Cryptos
    ("BTCUSD", "BTC-USD", "Bitcoin", "cryptos"),
    ("ETHUSD", "ETH-USD", "Ethereum", "cryptos"),
    ("BNBUSD", "BNB-USD", "BNB", "cryptos"),
    ("SOLUSD", "SOL-USD", "Solana", "cryptos"),
    ("XRPUSD", "XRP-USD", "XRP", "cryptos"),
    ("ADAUSD", "ADA-USD", "Cardano", "cryptos"),
    ("DOGEUSD", "DOGE-USD", "Dogecoin", "cryptos"),
    ("AVAXUSD", "AVAX-USD", "Avalanche", "cryptos"),
    ("LINKUSD", "LINK-USD", "Chainlink", "cryptos"),
    ("DOTUSD", "DOT-USD", "Polkadot", "cryptos"),
    ("LTCUSD", "LTC-USD", "Litecoin", "cryptos"),
    ("TRXUSD", "TRX-USD", "TRON", "cryptos"),
    # Taux US
    ("US10Y", "^TNX", "Taux US 10 ans", "taux_us"),
    ("US5Y", "^FVX", "Taux US 5 ans", "taux_us"),
    ("US30Y", "^TYX", "Taux US 30 ans", "taux_us"),
    ("US13W", "^IRX", "Taux US 13 semaines", "taux_us"),
    # Futures obligataires
    ("ZN", "ZN=F", "Future T-Note 10 ans", "futures_obligataires"),
    ("ZB", "ZB=F", "Future T-Bond 30 ans", "futures_obligataires"),
    ("ZF", "ZF=F", "Future T-Note 5 ans", "futures_obligataires"),
    ("ZT", "ZT=F", "Future T-Note 2 ans", "futures_obligataires"),
    # ETF
    ("SPY", "SPY", "SPDR S&P 500 ETF", "etf"),
    ("QQQ", "QQQ", "Invesco QQQ", "etf"),
    ("DIA", "DIA", "SPDR Dow Jones ETF", "etf"),
    ("IWM", "IWM", "iShares Russell 2000", "etf"),
    ("GLD", "GLD", "SPDR Gold Shares", "etf"),
    ("SLV", "SLV", "iShares Silver Trust", "etf"),
    ("USO", "USO", "United States Oil Fund", "etf"),
    ("TLT", "TLT", "iShares 20+ Year Treasury", "etf"),
    ("IEF", "IEF", "iShares 7-10 Year Treasury", "etf"),
    ("HYG", "HYG", "iShares High Yield Corp Bond", "etf"),
    ("LQD", "LQD", "iShares Investment Grade Corp Bond", "etf"),
    ("UUP", "UUP", "Invesco DB US Dollar Bullish", "etf"),
    ("EEM", "EEM", "iShares MSCI Emerging Markets", "etf"),
    ("FXI", "FXI", "iShares China Large-Cap", "etf"),
    # Actions
    ("AAPL", "AAPL", "Apple", "actions"),
    ("MSFT", "MSFT", "Microsoft", "actions"),
    ("NVDA", "NVDA", "Nvidia", "actions"),
    ("AMZN", "AMZN", "Amazon", "actions"),
    ("GOOGL", "GOOGL", "Alphabet", "actions"),
    ("META", "META", "Meta", "actions"),
    ("TSLA", "TSLA", "Tesla", "actions"),
    ("JPM", "JPM", "JPMorgan Chase", "actions"),
    ("MC_PA", "MC.PA", "LVMH", "actions"),
    ("ASML_AS", "ASML.AS", "ASML", "actions"),
    ("SAP_DE", "SAP.DE", "SAP", "actions"),
    ("TTE_PA", "TTE.PA", "TotalEnergies", "actions"),
    ("7203_T", "7203.T", "Toyota", "actions"),
]

ACTIFS = [
    {"slug": slug, "ticker": ticker, "nom": nom, "categorie": categorie}
    for (slug, ticker, nom, categorie) in _ACTIFS_BRUTS
]

# Garde-fou : un doublon de slug ou de ticker écraserait silencieusement des données.
assert len({a["slug"] for a in ACTIFS}) == len(ACTIFS), "Slug en double dans ACTIFS"
assert len({a["ticker"] for a in ACTIFS}) == len(ACTIFS), "Ticker en double dans ACTIFS"


# ---------- INITIALISATION FIREBASE (patron identique aux autres scripts) ----------

def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, actifs_recuperes=0, actifs_totaux=0,
                                actifs_ecrits=0, mode="normal", manquants=None, erreur=None):
    """Battement de coeur dans 'pipeline_status', à CHAQUE cycle."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "actifs_recuperes": actifs_recuperes,
        "actifs_totaux": actifs_totaux,
        "actifs_ecrits": actifs_ecrits,
        "mode": mode,
        "statut": statut,
        "actifs_manquants": (manquants or [])[:30],
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SIGNATURES LOCALES (évite les écritures inutiles marché fermé) ----------

def _charger_signatures():
    if not os.path.exists(FICHIER_SIGNATURES):
        return {}
    try:
        with open(FICHIER_SIGNATURES, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _sauvegarder_signatures(signatures):
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(FICHIER_SIGNATURES, "w", encoding="utf-8") as f:
        json.dump(signatures, f, ensure_ascii=False, indent=0, sort_keys=True)


def _signature_barre(barre):
    ts, o, h, l, c = barre
    return f"{ts}|{o}|{h}|{l}|{c}"


# ---------- 1. TÉLÉCHARGEMENT YFINANCE ----------

def lire_jours_historique():
    brut = os.environ.get("JOURS_HISTORIQUE", "").strip()
    if not brut:
        return JOURS_HISTORIQUE_DEFAUT
    try:
        jours = int(brut)
    except ValueError:
        print(f"⚠️  JOURS_HISTORIQUE invalide ({brut!r}), valeur par défaut utilisée.")
        return JOURS_HISTORIQUE_DEFAUT
    return max(1, min(jours, JOURS_HISTORIQUE_MAX))


def telecharger_lot(tickers, debut, fin):
    """Télécharge un lot de tickers en bougies 1h, avec quelques tentatives.
    Retourne le DataFrame yfinance, ou None si tout a échoué."""
    for tentative in range(1, NB_TENTATIVES_LOT + 1):
        try:
            df = yf.download(
                tickers, start=debut, end=fin, interval="1h",
                auto_adjust=False, progress=False, group_by="ticker", threads=False,
            )
            if df is not None and not df.empty:
                return df
            print(f"⚠️  Lot vide (tentative {tentative}/{NB_TENTATIVES_LOT}).")
        except Exception as e:
            print(f"⚠️  Échec du téléchargement (tentative {tentative}/{NB_TENTATIVES_LOT}) : {e}")
        if tentative < NB_TENTATIVES_LOT:
            time.sleep(5 * tentative)
    return None


def _nombre_valide(x):
    return x is not None and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))


def extraire_barres(df, ticker, nb_tickers_lot):
    """Extrait les bougies d'un ticker : liste de (ts_unix, o, h, l, c), triée.
    Retourne [] si le ticker est absent ou sans donnée."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            niveau0 = df.columns.get_level_values(0)
            niveau1 = df.columns.get_level_values(1)
            if ticker in niveau0:
                sous = df[ticker]
            elif ticker in niveau1:
                sous = df.xs(ticker, axis=1, level=1)
            else:
                return []
        elif nb_tickers_lot == 1:
            sous = df
        else:
            return []
        sous = sous[["Open", "High", "Low", "Close"]].dropna(subset=["Close"])
    except (KeyError, TypeError):
        return []

    if sous.empty:
        return []

    index = sous.index
    if index.tz is None:
        index = index.tz_localize("UTC")
    else:
        index = index.tz_convert("UTC")

    barres = []
    for ts, (o, h, l, c) in zip(index, sous[["Open", "High", "Low", "Close"]].itertuples(index=False, name=None)):
        c = float(c)
        if not _nombre_valide(c):
            continue
        o = float(o) if _nombre_valide(o) else c
        h = float(h) if _nombre_valide(h) else c
        l = float(l) if _nombre_valide(l) else c
        barres.append((int(ts.timestamp()), round(o, 6), round(h, 6), round(l, 6), round(c, 6)))

    barres.sort(key=lambda b: b[0])
    return barres


def recuperer_barres(actifs, debut, fin):
    """Retourne ({slug: [barres]}, [slugs sans donnée])."""
    resultat = {}
    manquants = []
    lots = [actifs[i:i + TAILLE_LOT_TICKERS] for i in range(0, len(actifs), TAILLE_LOT_TICKERS)]

    for numero, lot in enumerate(lots, start=1):
        tickers = [a["ticker"] for a in lot]
        print(f"⬇️  Lot {numero}/{len(lots)} : {len(tickers)} tickers")
        df = telecharger_lot(tickers, debut, fin)
        if df is None:
            manquants.extend(a["slug"] for a in lot)
        else:
            for a in lot:
                barres = extraire_barres(df, a["ticker"], len(tickers))
                if barres:
                    resultat[a["slug"]] = barres
                else:
                    manquants.append(a["slug"])
        if numero < len(lots):
            time.sleep(PAUSE_ENTRE_LOTS_SECONDES)

    return resultat, manquants


# ---------- 2. PRÉPARATION DES ÉCRITURES ----------

def grouper_par_mois(barres):
    """{ 'AAAA-MM': { 't<ts>': {o,h,l,c} } } — mois en UTC."""
    par_mois = {}
    for ts, o, h, l, c in barres:
        cle_mois = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")
        par_mois.setdefault(cle_mois, {})[f"t{ts}"] = {"o": o, "h": h, "l": l, "c": c}
    return par_mois


def preparer_documents_actif(actif, barres):
    """Retourne [(doc_id, donnees_firestore, taille_estimee_octets), ...]."""
    documents = []
    for cle_mois, points in grouper_par_mois(barres).items():
        donnees = {
            "slug": actif["slug"],
            "ticker": actif["ticker"],
            "nom": actif["nom"],
            "categorie": actif["categorie"],
            "mois": cle_mois,
            "points": points,
            "derniere_maj": firestore.SERVER_TIMESTAMP,
        }
        taille = len(json.dumps(points))
        documents.append((f"{actif['slug']}_{cle_mois}", donnees, taille))
    return documents


def _commit_avec_retry(batch):
    """Commit d'un batch avec quelques tentatives sur les erreurs transitoires
    (504 Deadline Exceeded, 503...). Sans risque : les écritures sont des
    set(merge=True), donc rejouables à l'identique. ResourceExhausted (quota)
    n'est JAMAIS réessayé : on remonte tout de suite."""
    for tentative in range(1, NB_TENTATIVES_COMMIT + 1):
        try:
            batch.commit(timeout=TIMEOUT_COMMIT_SECONDES)
            return
        except ResourceExhausted:
            raise
        except ERREURS_TRANSITOIRES as e:
            if tentative == NB_TENTATIVES_COMMIT:
                raise
            pause = 3 * tentative * tentative
            print(f"⚠️  Commit Firestore refusé ({type(e).__name__}), "
                  f"tentative {tentative}/{NB_TENTATIVES_COMMIT}, nouvel essai dans {pause}s...")
            time.sleep(pause)


def ecrire_actifs(db, a_ecrire, signatures, slugs_ecrits, erreurs):
    """Écrit les documents par petits batchs (opérations ET octets).
    `a_ecrire` : liste de (actif, barres, documents). Un actif peut être réparti
    sur PLUSIEURS batchs (backfill : ~24 documents par actif) ; sa signature
    n'est mise à jour qu'une fois TOUS ses documents commités avec succès.
    `slugs_ecrits` et `erreurs` sont remplis EN PLACE, pour rester exacts même
    si ResourceExhausted interrompt la boucle (levée vers l'appelant)."""
    operations = []      # (slug, doc_id, donnees, taille_octets)
    restants = {}        # slug -> nombre de documents pas encore commités
    derniere_barre = {}
    for actif, barres, documents in a_ecrire:
        slug = actif["slug"]
        restants[slug] = len(documents)
        derniere_barre[slug] = barres[-1]
        for doc_id, donnees, taille in documents:
            operations.append((slug, doc_id, donnees, taille))

    total_docs = len(operations)
    echecs = set()       # slugs dont au moins un document n'a pas pu être écrit
    batch = db.batch()
    slugs_du_batch = []  # un slug par document du batch courant
    nb_octets = 0
    docs_ecrits = 0
    nb_commits = 0

    def commit_batch():
        nonlocal batch, slugs_du_batch, nb_octets, docs_ecrits, nb_commits
        if not slugs_du_batch:
            return
        try:
            _commit_avec_retry(batch)
        except ResourceExhausted:
            raise
        except Exception as e:
            print(f"❌ Échec du commit Firestore ({len(slugs_du_batch)} documents) : {e}")
            erreurs.append(f"{type(e).__name__}: {e}"[:200])
            echecs.update(slugs_du_batch)
        else:
            docs_ecrits += len(slugs_du_batch)
            nb_commits += 1
            for slug in slugs_du_batch:
                restants[slug] -= 1
                if restants[slug] == 0 and slug not in echecs:
                    signatures[slug] = _signature_barre(derniere_barre[slug])
                    slugs_ecrits.append(slug)
            if nb_commits % 10 == 0:
                print(f"   … {docs_ecrits}/{total_docs} documents écrits")
        batch = db.batch()
        slugs_du_batch = []
        nb_octets = 0

    for slug, doc_id, donnees, taille in operations:
        if slugs_du_batch and (len(slugs_du_batch) >= TAILLE_MAX_BATCH_OPERATIONS
                               or nb_octets + taille > TAILLE_MAX_BATCH_OCTETS):
            commit_batch()
        ref = db.collection(COLLECTION_HISTORIQUE).document(doc_id)
        batch.set(ref, donnees, merge=True)
        slugs_du_batch.append(slug)
        nb_octets += taille

    commit_batch()
    if total_docs:
        print(f"{docs_ecrits}/{total_docs} documents écrits en {nb_commits} commits.")


def ecrire_derniers_prix(db, actifs_par_slug, barres_par_slug, slugs):
    """Fusionne les derniers prix des actifs `slugs` dans asset_latest/all."""
    if not slugs:
        return
    actifs_map = {}
    for slug in slugs:
        ts, _o, _h, _l, c = barres_par_slug[slug][-1]
        actifs_map[slug] = {"prix": c, "ts": ts, "ticker": actifs_par_slug[slug]["ticker"]}
    db.collection(COLLECTION_DERNIER).document(DOC_ID_DERNIER).set(
        {"actifs": actifs_map, "derniere_maj": firestore.SERVER_TIMESTAMP}, merge=True
    )


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------

def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    jours = lire_jours_historique()
    mode_backfill = jours > JOURS_HISTORIQUE_DEFAUT
    mode = "backfill" if mode_backfill else "normal"
    print(f"Mode {mode} : {jours} jours d'historique demandés pour {len(ACTIFS)} actifs.")

    debut = maintenant - timedelta(days=jours)
    fin = maintenant + timedelta(days=1)  # marge : inclure la bougie en cours

    barres_par_slug, manquants = recuperer_barres(ACTIFS, debut, fin)
    actifs_par_slug = {a["slug"]: a for a in ACTIFS}

    signatures = _charger_signatures()

    # Actifs à écrire : signature de la dernière bougie différente (ou backfill)
    a_ecrire = []
    for actif in ACTIFS:
        barres = barres_par_slug.get(actif["slug"])
        if not barres:
            continue
        if not mode_backfill and signatures.get(actif["slug"]) == _signature_barre(barres[-1]):
            continue  # rien de nouveau (marché fermé ou bougie inchangée)
        a_ecrire.append((actif, barres, preparer_documents_actif(actif, barres)))

    print(f"{len(barres_par_slug)}/{len(ACTIFS)} actifs récupérés, {len(a_ecrire)} à écrire, "
          f"{len(manquants)} manquants.")

    slugs_ecrits = []
    erreurs_commit = []
    erreur_quota = None
    try:
        ecrire_actifs(db, a_ecrire, signatures, slugs_ecrits, erreurs_commit)
    except ResourceExhausted as e:
        print(f"Quota Firestore dépassé : {e}")
        erreur_quota = "Quota Firestore dépassé (ResourceExhausted)"

    # Derniers prix des actifs effectivement écrits (même si le quota a coupé la suite)
    try:
        ecrire_derniers_prix(db, actifs_par_slug, barres_par_slug, slugs_ecrits)
    except ResourceExhausted as e:
        print(f"Quota Firestore dépassé lors de l'écriture des derniers prix : {e}")
        erreur_quota = "Quota Firestore dépassé (ResourceExhausted)"

    _sauvegarder_signatures(signatures)

    # ---- Statut du pipeline ----
    actifs_recuperes = len(barres_par_slug)
    erreur = erreur_quota or (erreurs_commit[0] if erreurs_commit else None)

    if erreur or actifs_recuperes == 0:
        statut = "erreur"
    elif actifs_recuperes == len(ACTIFS):
        statut = "ok"
    else:
        statut = "ok_partiel"

    try:
        enregistrer_statut_pipeline(
            db, statut=statut,
            actifs_recuperes=actifs_recuperes, actifs_totaux=len(ACTIFS),
            actifs_ecrits=len(slugs_ecrits), mode=mode,
            manquants=manquants, erreur=erreur,
        )
    except ResourceExhausted as e:
        print(f"Quota Firestore dépassé lors de l'écriture du statut : {e}")

    if manquants:
        print(f"⚠️  Sans donnée : {manquants}")
    print(f"Terminé : statut={statut}, {len(slugs_ecrits)} actifs écrits.")


if __name__ == "__main__":
    cycle()
