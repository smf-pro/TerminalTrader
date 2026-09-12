# -*- coding: utf-8 -*-
"""
Script de récupération des taux FX en temps quasi réel (version cloud)
------------------------------------------------------------------------
Version single-pass pour tourner sur GitHub Actions (cron 1 min via
cron-job.org, secours natif 5 min).

CORRECTIF IMPORTANT : le endpoint REST /quote de Finnhub est PAYANT pour
le forex sur le plan gratuit (403 Forbidden constaté en production sur
les 7 paires). Seul le WebSocket de trades forex est gratuit chez
Finnhub. On ne peut donc pas faire un simple appel REST bloquant comme
prévu initialement.

Solution retenue : un WebSocket BORNÉ DANS LE TEMPS (FENETRE_WS_SECONDES,
20-25s) au lieu d'un ws.run_forever() infini. Le script ouvre la
connexion, écoute pendant cette fenêtre courte, capture les derniers
prix reçus, ferme la connexion, puis continue son cycle normalement
(écriture Firestore, archive...). Le modèle reste single-pass : le
script se termine toujours après quelques dizaines de secondes.

Comme une fenêtre de 20-25s peut ne recevoir aucun tick sur une paire
peu tradée à certaines heures (NZDUSD, CHFUSD hors sessions actives), un
cache local du dernier prix connu (cache/fxforex_last_price.json)
persiste entre les runs : si aucun tick n'arrive pendant la fenêtre, on
réutilise le dernier prix connu plutôt que d'écrire None en boucle.

- Les références historiques (24h/7j/30j glissants + jour/semaine/mois
  calendaires en cours) sont coûteuses à recalculer (téléchargement
  yfinance) et ne changent de toute façon qu'une fois par heure (données
  horaires) : elles sont mises en cache localement
  (cache/fxforex_refs.json) et rafraîchies seulement si le cache a plus
  d'une heure, jamais à chaque minute.
- Écriture Firestore : UN SEUL document, collection "fx_rates", ID fixe
  "latest", écrasé à chaque cycle (pas une collection qui grossit sans
  fin comme les articles) : le site l'écoute en onSnapshot pour
  l'affichage live.
- Archive horaire : PAS dans Firestore (éviterait d'ajouter une 2e
  collection à interroger), directement en fichier JSON committé,
  docs/archive/fx/AAAA-MM-JJ.json, un point ajouté par heure (marqueur
  local cache/.derniere_archive_fx pour éviter les doublons si le cron
  tombe plusieurs fois dans la même heure).
- enregistrer_statut_pipeline() : même patron que centralbanks_cloud.py,
  NOM_SOURCE = "fxforex", pour que le site affiche la pastille de statut
  de cette source comme les autres.
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone

import requests
import websocket
import pandas as pd
import numpy as np
import yfinance as yf

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

# ---------- CONFIGURATION ----------

NOM_SOURCE = "fxforex"
COLLECTION = "fx_rates"
DOC_ID_LIVE = "latest"

FENETRE_WS_SECONDES = 22  # durée d'écoute du WebSocket avant de fermer et continuer le cycle

DOSSIER_CACHE = "cache"
FICHIER_REFS_CACHE = os.path.join(DOSSIER_CACHE, "fxforex_refs.json")
FICHIER_MARQUEUR_ARCHIVE = os.path.join(DOSSIER_CACHE, ".derniere_archive_fx")
FICHIER_DERNIER_PRIX = os.path.join(DOSSIER_CACHE, "fxforex_last_price.json")

DOSSIER_SITE = "docs"
DOSSIER_ARCHIVE_FX = os.path.join(DOSSIER_SITE, "archive", "fx")

DUREE_VALIDITE_REFS_HEURES = 1
DUREE_MIN_ENTRE_POINTS_ARCHIVE_MINUTES = 55  # marge sous 60 min pour tolérer la gigue du cron

# Symboles Finnhub (format REST /quote, identique au format WebSocket)
SYMBOLS_FINNHUB = {
    "EURUSD": "OANDA:EUR_USD",
    "GBPUSD": "OANDA:GBP_USD",
    "USDCHF": "OANDA:USD_CHF",
    "USDCAD": "OANDA:USD_CAD",
    "AUDUSD": "OANDA:AUD_USD",
    "NZDUSD": "OANDA:NZD_USD",
    "USDJPY": "OANDA:USD_JPY",
}

# Tickers yfinance équivalents (pour les références historiques uniquement)
YF_TICKERS = {
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDCHF": "USDCHF=X",
    "USDCAD": "USDCAD=X",
    "AUDUSD": "AUDUSD=X",
    "NZDUSD": "NZDUSD=X",
    "USDJPY": "USDJPY=X",
}

INVERTED = ("USDCHF", "USDCAD", "USDJPY")  # paires à inverser pour obtenir XXXUSD
USD_NAME = {
    "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "USDCHF": "CHFUSD",
    "USDCAD": "CADUSD", "AUDUSD": "AUDUSD", "NZDUSD": "NZDUSD", "USDJPY": "JPYUSD",
}

LABELS_REFS = ("24h", "7d", "30d", "jour", "semaine", "mois")


# ---------- INITIALISATION FIREBASE (patron identique aux autres scripts) ----------

def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, devises_recuperees=0, devises_totales=0, erreur=None):
    """Écrit un battement de coeur dans 'pipeline_status', à CHAQUE cycle,
    même si toutes les devises n'ont pas pu être récupérées."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "devises_recuperees": devises_recuperees,
        "devises_totales": devises_totales,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


def to_usd_quote(raw_name, value):
    """Convertit un prix brut (Finnhub ou yfinance) en cotation XXXUSD
    (inverse si besoin pour USDCHF/USDCAD/USDJPY)."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if raw_name in INVERTED:
        if value == 0:
            return None
        return 1 / value
    return value


# ---------- 1. PRIX LIVE VIA FINNHUB WEBSOCKET (borné dans le temps) ----------
# Le REST /quote est payant pour le forex sur le plan gratuit Finnhub
# (403 Forbidden constaté en production). Seul le WebSocket est gratuit :
# on ouvre la connexion, on écoute pendant FENETRE_WS_SECONDES, puis on
# ferme et on continue le cycle normalement (le script reste single-pass).

finnhub_to_raw_name = {v: k for k, v in SYMBOLS_FINNHUB.items()}


def fetch_live_quotes(api_key, fenetre_secondes=FENETRE_WS_SECONDES):
    """Ouvre un WebSocket Finnhub, écoute pendant `fenetre_secondes`, puis
    ferme. Retourne {raw_name: dernier_prix_recu_ou_None}."""
    prix_captures = {raw_name: None for raw_name in SYMBOLS_FINNHUB}
    verrou = threading.Lock()

    def on_message(ws, message):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        if data.get("type") != "trade":
            return
        for trade in data.get("data", []):
            raw_name = finnhub_to_raw_name.get(trade.get("s"))
            if raw_name is None:
                continue
            with verrou:
                prix_captures[raw_name] = trade.get("p")

    def on_error(ws, error):
        print(f"⚠️  Erreur WebSocket : {error}")

    def on_open(ws):
        for symbol in SYMBOLS_FINNHUB.values():
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    ws_url = f"wss://ws.finnhub.io?token={api_key}"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error)
    ws.on_open = on_open

    thread = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 10}, daemon=True)
    thread.start()

    time.sleep(fenetre_secondes)

    ws.close()
    thread.join(timeout=5)

    with verrou:
        resultat = dict(prix_captures)

    manquants = [k for k, v in resultat.items() if v is None]
    if manquants:
        print(f"⚠️  Aucun tick reçu pendant la fenêtre pour : {manquants} (fallback sur le dernier prix connu)")

    return resultat


# ---------- 1bis. CACHE DU DERNIER PRIX CONNU (fallback inter-cycles) ----------

def _charger_dernier_prix_cache():
    if not os.path.exists(FICHIER_DERNIER_PRIX):
        return {}
    try:
        with open(FICHIER_DERNIER_PRIX, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _sauvegarder_dernier_prix_cache(cache):
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(FICHIER_DERNIER_PRIX, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def appliquer_fallback_dernier_prix(prix_usd_par_devise, maintenant):
    """Pour chaque devise sans prix ce cycle (aucun tick reçu pendant la
    fenêtre WS), réutilise le dernier prix connu du cache local plutôt que
    de laisser None. Met aussi à jour le cache avec les prix fraîchement
    reçus ce cycle."""
    cache = _charger_dernier_prix_cache()
    resultat = {}

    for usd_name, prix in prix_usd_par_devise.items():
        if prix is not None:
            resultat[usd_name] = prix
            cache[usd_name] = {"prix": prix, "horodatage": maintenant.isoformat()}
        else:
            entree_cache = cache.get(usd_name)
            if entree_cache and entree_cache.get("prix") is not None:
                resultat[usd_name] = entree_cache["prix"]
                print(f"↩️  {usd_name} : aucun tick ce cycle, réutilisation du dernier prix connu ({entree_cache['prix']}, du {entree_cache.get('horodatage', '?')})")
            else:
                resultat[usd_name] = None

    _sauvegarder_dernier_prix_cache(cache)
    return resultat


# ---------- 2. RÉFÉRENCES HISTORIQUES (cache local, rafraîchi 1x/heure) ----------

def _charger_refs_cache():
    if not os.path.exists(FICHIER_REFS_CACHE):
        return None
    try:
        with open(FICHIER_REFS_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _sauvegarder_refs_cache(refs):
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    contenu = {
        "derniere_maj": datetime.now(timezone.utc).isoformat(),
        "refs": refs,
    }
    with open(FICHIER_REFS_CACHE, "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=2)


def _refs_perimees(cache):
    if not cache or "derniere_maj" not in cache:
        return True
    try:
        derniere_maj = datetime.fromisoformat(cache["derniere_maj"])
    except (ValueError, TypeError):
        return True
    return (datetime.now(timezone.utc) - derniere_maj) >= timedelta(hours=DUREE_VALIDITE_REFS_HEURES)


def _price_at(series, target_ts, method):
    """Retourne le prix dont le timestamp correspond à target_ts selon la
    méthode d'alignement pandas ('nearest' ou 'pad' = dernière valeur
    connue avant/à target_ts)."""
    if series.empty:
        return None
    idx = series.index.get_indexer([target_ts], method=method)[0]
    if idx == -1:
        return None
    valeur = series.iloc[idx]
    return None if pd.isna(valeur) else float(valeur)


def _calendar_boundaries(now_utc):
    start_of_day = now_utc.normalize()
    start_of_week = start_of_day - pd.Timedelta(days=start_of_day.weekday())
    start_of_month = start_of_day.replace(day=1)
    return start_of_day, start_of_week, start_of_month


def calculer_refs_historiques():
    """Télécharge des données HORAIRES yfinance (35 derniers jours) et
    calcule, pour chaque devise XXXUSD, les 6 références : 24h/7j/30j
    glissants exacts + début du jour/semaine/mois en cours (calendaire,
    méthode 'pad' pour ne jamais regarder dans le futur)."""
    print("📊 Recalcul des références historiques (cache périmé ou absent)...")
    now = pd.Timestamp.now(tz="UTC")
    start_of_day, start_of_week, start_of_month = _calendar_boundaries(now)

    rolling_targets = {
        "24h": now - pd.Timedelta(hours=24),
        "7d": now - pd.Timedelta(days=7),
        "30d": now - pd.Timedelta(days=30),
    }
    calendar_targets = {
        "jour": start_of_day,
        "semaine": start_of_week,
        "mois": start_of_month,
    }

    refs = {usd_name: {label: None for label in LABELS_REFS} for usd_name in USD_NAME.values()}

    try:
        raw = yf.download(
            list(YF_TICKERS.values()), period="35d", interval="1h",
            progress=False, group_by="ticker",
        )
    except Exception as e:
        print(f"⚠️  Échec du téléchargement yfinance : {e}")
        return refs  # on retourne des références vides plutôt que de planter le cycle

    for raw_name, ticker in YF_TICKERS.items():
        try:
            closes = raw[ticker]["Close"].dropna()
        except Exception:
            print(f"⚠️  Pas de données historiques pour {raw_name}")
            continue

        if closes.empty:
            continue

        if closes.index.tz is None:
            closes.index = closes.index.tz_localize("UTC")
        else:
            closes.index = closes.index.tz_convert("UTC")

        usd_name = USD_NAME[raw_name]
        for label, target_ts in rolling_targets.items():
            raw_price = _price_at(closes, target_ts, method="nearest")
            refs[usd_name][label] = to_usd_quote(raw_name, raw_price)
        for label, target_ts in calendar_targets.items():
            raw_price = _price_at(closes, target_ts, method="pad")
            refs[usd_name][label] = to_usd_quote(raw_name, raw_price)

    print("✅ Références historiques recalculées.")
    return refs


def obtenir_refs_historiques():
    """Retourne les références depuis le cache local si encore valides
    (< 1h), sinon les recalcule et met à jour le cache."""
    cache = _charger_refs_cache()
    if not _refs_perimees(cache):
        return cache["refs"]

    refs = calculer_refs_historiques()
    _sauvegarder_refs_cache(refs)
    return refs


# ---------- 3. ARCHIVE HORAIRE (fichier JSON direct, pas Firestore) ----------

def _dernier_point_archive_trop_recent():
    if not os.path.exists(FICHIER_MARQUEUR_ARCHIVE):
        return False
    try:
        with open(FICHIER_MARQUEUR_ARCHIVE, "r", encoding="utf-8") as f:
            dernier = datetime.fromisoformat(f.read().strip())
    except (ValueError, OSError):
        return False
    return (datetime.now(timezone.utc) - dernier) < timedelta(minutes=DUREE_MIN_ENTRE_POINTS_ARCHIVE_MINUTES)


def _marquer_point_archive():
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(FICHIER_MARQUEUR_ARCHIVE, "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())


FICHIER_INDEX_ARCHIVE_FX = os.path.join(DOSSIER_ARCHIVE_FX, "index.json")


def _lire_index_archive_fx():
    if not os.path.exists(FICHIER_INDEX_ARCHIVE_FX):
        return []
    try:
        with open(FICHIER_INDEX_ARCHIVE_FX, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _mettre_a_jour_index_archive_fx(date_str):
    """Ajoute date_str à docs/archive/fx/index.json si absente, trié du
    plus récent au plus ancien (même patron que _mettre_a_jour_index_archive
    dans site_generator.py, pour que le site sache quelles dates sont
    disponibles sans deviner/tomber sur des 404)."""
    dates = _lire_index_archive_fx()
    if date_str not in dates:
        dates.append(date_str)
    dates.sort(reverse=True)
    os.makedirs(DOSSIER_ARCHIVE_FX, exist_ok=True)
    with open(FICHIER_INDEX_ARCHIVE_FX, "w", encoding="utf-8") as f:
        json.dump(dates, f, ensure_ascii=False, indent=2)


def ajouter_point_archive_horaire(devises_doc, maintenant):
    """Ajoute un point (prix uniquement, pas les %) au fichier d'archive
    du jour, un point par heure maximum (voir marqueur ci-dessus)."""
    if _dernier_point_archive_trop_recent():
        print("Archive horaire : dernier point trop récent, on saute.")
        return

    date_str = maintenant.strftime("%Y-%m-%d")
    chemin = os.path.join(DOSSIER_ARCHIVE_FX, f"{date_str}.json")

    if os.path.exists(chemin):
        try:
            with open(chemin, "r", encoding="utf-8") as f:
                contenu = json.load(f)
        except (json.JSONDecodeError, OSError):
            contenu = {"date": date_str, "points": []}
    else:
        contenu = {"date": date_str, "points": []}

    point = {
        "horodatage": maintenant.isoformat(),
        "prix": {usd_name: devises_doc[usd_name]["prix"] for usd_name in USD_NAME.values() if usd_name in devises_doc},
    }
    contenu["points"].append(point)

    os.makedirs(DOSSIER_ARCHIVE_FX, exist_ok=True)
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=2)

    _mettre_a_jour_index_archive_fx(date_str)
    _marquer_point_archive()
    print(f"Archive horaire : point ajouté ({len(contenu['points'])} points aujourd'hui).")


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------

def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    api_key = os.environ.get("FINNHUB_API_KEY")
    if not api_key:
        print("❌ Variable d'environnement FINNHUB_API_KEY manquante.")
        enregistrer_statut_pipeline(db, statut="erreur", erreur="FINNHUB_API_KEY manquante")
        return

    # ---- 1. Références historiques (cache 1h) ----
    refs = obtenir_refs_historiques()

    # ---- 2. Prix live (WebSocket borné, ~22s d'écoute) ----
    prix_bruts = fetch_live_quotes(api_key)
    prix_usd_bruts = {usd_name: to_usd_quote(raw_name, prix_bruts.get(raw_name)) for raw_name, usd_name in USD_NAME.items()}

    # ---- 2bis. Fallback sur le dernier prix connu si aucun tick reçu ce cycle ----
    prix_usd_final = appliquer_fallback_dernier_prix(prix_usd_bruts, maintenant)

    devises_doc = {}
    devises_recuperees = 0
    for usd_name in USD_NAME.values():
        prix_usd = prix_usd_final.get(usd_name)
        entree = {"prix": prix_usd}

        refs_devise = refs.get(usd_name, {})
        for label in LABELS_REFS:
            ref_val = refs_devise.get(label)
            if prix_usd is not None and ref_val:
                entree[f"pct_{label}"] = (prix_usd - ref_val) / ref_val * 100
            else:
                entree[f"pct_{label}"] = None

        devises_doc[usd_name] = entree
        if prix_usd is not None:
            devises_recuperees += 1

    # ---- 3. Écriture Firestore : un seul document, écrasé à chaque cycle ----
    try:
        db.collection(COLLECTION).document(DOC_ID_LIVE).set({
            "devises": devises_doc,
            "derniere_maj": firestore.SERVER_TIMESTAMP,
        })
    except ResourceExhausted as e:
        print(f"Quota Firestore dépassé lors de l'écriture fx_rates : {e}")
        enregistrer_statut_pipeline(
            db, statut="erreur",
            devises_recuperees=devises_recuperees, devises_totales=len(USD_NAME),
            erreur="Quota Firestore dépassé (ResourceExhausted)",
        )
        return

    print(f"Écrit dans Firestore ({devises_recuperees}/{len(USD_NAME)} devises) : {devises_doc}")

    # ---- 4. Archive horaire (fichier JSON, pas Firestore) ----
    ajouter_point_archive_horaire(devises_doc, maintenant)

    # ---- 5. Statut du pipeline ----
    if devises_recuperees == len(USD_NAME):
        statut = "ok"
    elif devises_recuperees > 0:
        statut = "ok_partiel"
    else:
        statut = "erreur"

    enregistrer_statut_pipeline(
        db, statut=statut,
        devises_recuperees=devises_recuperees, devises_totales=len(USD_NAME),
    )


if __name__ == "__main__":
    cycle()
