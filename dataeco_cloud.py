# -*- coding: utf-8 -*-
"""
Script d'extraction - Calendrier economique + Data eco (version cloud)
------------------------------------------------------------------------------
Deux collections Firestore alimentees par ce script :

1. `calendrier_events` : TOUS les evenements de la semaine en cours du
   calendrier ForexFactory (pas seulement les indicateurs suivis) - sert a
   alimenter le widget "CALENDRIER ECONOMIQUE" de la page Accueil. Chaque
   evenement est un document, ID = hash(date+heure+devise+titre). Comme un
   evenement passe de "Actual vide" a "Actual rempli" avec le temps, on fait
   un UPSERT (set avec merge=True) a chaque cycle, pas juste une insertion
   unique comme les autres scripts du projet.

2. `dataeco_indicateurs` : uniquement les indicateurs macro suivis
   (config CONFIG_INDICATEURS ci-dessous), avec un historique de
   publications passees (pour construire un graphique cote site). Deux
   sources possibles par indicateur :
   - "forexfactory" : mis a jour automatiquement en reconnaissant
     l'indicateur dans le calendrier (correspondance devise + nom exact) -
     pas besoin de revisiter sa page dediee a chaque cycle.
   - "tradingeconomics" : absent du calendrier ForexFactory, donc revisite
     directement sa page dediee a chaque cycle (moins frequent que le
     calendrier, voir workflow .yml).

   AMORCAGE (backfill) : la toute premiere fois qu'un indicateur est vu
   (pas encore de champ `historique` dans Firestore), on visite sa page
   dediee UNE FOIS pour recuperer l'historique de depart (8 dernieres
   publications ForexFactory, ou 3 pour TradingEconomics) + les
   metadonnees (source, frequence). Ensuite, le calendrier suffit a
   completer au fil du temps pour les indicateurs ForexFactory.

ATTENTION SELECTEURS CSS DU CALENDRIER NON TESTES CONTRE LE VRAI HTML EN
DIRECT. Le calendrier ForexFactory suit vraisemblablement la meme
convention BEM que la page News deja scrapee avec succes
(`.news-block__item` etc dans ff_cloud.py), donc les selecteurs ci-dessous
(`.calendar__row`, `.calendar__event`, etc.) sont une estimation
raisonnable basee sur cette convention - mais DOIVENT etre verifies/
ajustes apres le premier run reel (voir logs GitHub Actions : si "0
evenement trouve" apparait, ouvrir https://www.forexfactory.com/calendar,
Inspecter une ligne du tableau, et corriger les selecteurs dans
`scraper_calendrier()`).

NOUVEAU (par rapport aux 3 scripts precedents) :
- UPSERT au lieu d'insertion unique (les valeurs changent avec le temps).
- Cache local reinterprete comme "cache de valeur" : on ne reecrit
  Firestore que si la valeur a change depuis le dernier cycle, pas juste
  "deja vu" (sinon le fichier ne se remplirait jamais de valeurs Actual).
"""

import os
import re
import time
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- CONFIGURATION ----------
URL_CALENDRIER = "https://www.forexfactory.com/calendar"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

COLLECTION_EVENTS = "calendrier_events"
COLLECTION_INDICATEURS = "dataeco_indicateurs"
NOM_SOURCE = "dataeco"  # identifiant unique de ce script dans pipeline_status

# Nombre de points d'historique a garder par indicateur (le site construira
# son propre graphique a partir de ce tableau - pas besoin d'en garder plus).
HISTORIQUE_MAX_POINTS = 24

# ---------- CONFIG DES INDICATEURS SUIVIS (USD pour l'instant) ----------
# "nom_calendrier" = nom EXACT tel qu'il apparait dans le tableau du
# calendrier ForexFactory (sans prefixe pays, ex: "CPI y/y" pas "US CPI
# y/y") - c'est la cle de correspondance avec calendrier_events. Verifie
# manuellement contre un vrai fetch du calendrier pour les entrees deja
# vues (CPI y/y, Core CPI m/m, PPI m/m, Unemployment Claims, ADP Weekly
# Employment Change confirmes ; les autres suivent le meme format mais
# n'etaient pas publies cette semaine-la, a confirmer au premier vrai
# cycle).
CONFIG_INDICATEURS = [
    # --- TAUX ---
    {"pays": "USD", "categorie": "taux", "nom_affichage": "Federal Funds Rate",
     "nom_calendrier": "Federal Funds Rate", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/1-us-federal-funds-rate"},

    # --- EMPLOIS ---
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "Non-Farm Employment Change",
     "nom_calendrier": "Non-Farm Employment Change", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/66-us-non-farm-employment-change"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "Prelim Benchmark Payrolls Revision",
     "nom_calendrier": "Prelim Benchmark Payrolls Revision", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/925-us-prelim-benchmark-payrolls-revision"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "Unemployment Rate",
     "nom_calendrier": "Unemployment Rate", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/56-us-unemployment-rate"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "ADP Non-Farm Employment Change",
     "nom_calendrier": "ADP Non-Farm Employment Change", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/75-us-adp-non-farm-employment-change"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "JOLTS Job Openings",
     "nom_calendrier": "JOLTS Job Openings", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/578-us-jolts-job-openings"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "Unemployment Claims",
     "nom_calendrier": "Unemployment Claims", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/11-us-unemployment-claims"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "ADP Weekly Employment Change",
     "nom_calendrier": "ADP Weekly Employment Change", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/929-us-adp-weekly-employment-change"},
    {"pays": "USD", "categorie": "emplois", "nom_affichage": "Challenger Job Cuts y/y",
     "nom_calendrier": "Challenger Job Cuts y/y", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/76-us-challenger-job-cuts-yy"},

    # --- INFLATION ---
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "CPI y/y",
     "nom_calendrier": "CPI y/y", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/884-us-cpi-yy"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Core CPI y/y",
     "nom_calendrier": "Core CPI y/y", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/933-us-core-cpi-yy"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "CPI m/m",
     "nom_calendrier": "CPI m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/78-us-cpi-mm"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Core CPI m/m",
     "nom_calendrier": "Core CPI m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/79-us-core-cpi-mm"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "PPI m/m",
     "nom_calendrier": "PPI m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/86-us-ppi-mm"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Core PPI m/m",
     "nom_calendrier": "Core PPI m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/87-us-core-ppi-mm"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "PCE Price Index y/y",
     "nom_calendrier": None, "source": "tradingeconomics",
     "url": "https://tradingeconomics.com/united-states/pce-price-index-annual-change"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Core PCE Price Index m/m",
     "nom_calendrier": "Core PCE Price Index m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/85-us-core-pce-price-index-mm"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Services Inflation",
     "nom_calendrier": None, "source": "tradingeconomics",
     "url": "https://tradingeconomics.com/united-states/services-inflation"},
    {"pays": "USD", "categorie": "inflation", "nom_affichage": "Average Hourly Earnings m/m",
     "nom_calendrier": "Average Hourly Earnings m/m", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/159-us-average-hourly-earnings-mm"},

    # --- CROISSANCE (inclut les GDP Price Index, confirme par l'utilisateur) ---
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "Advance GDP Price Index q/q",
     "nom_calendrier": "Advance GDP Price Index q/q", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/3-us-advance-gdp-price-index-qq"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "Prelim GDP Price Index q/q",
     "nom_calendrier": "Prelim GDP Price Index q/q", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/45-us-prelim-gdp-price-index-qq"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "Final GDP Price Index q/q",
     "nom_calendrier": "Final GDP Price Index q/q", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/46-us-final-gdp-price-index-qq"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "ISM Services PMI",
     "nom_calendrier": "ISM Services PMI", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/253-us-ism-services-pmi"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "ISM Manufacturing PMI",
     "nom_calendrier": "ISM Manufacturing PMI", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/252-us-ism-manufacturing-pmi"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "Philly Fed Manufacturing Index",
     "nom_calendrier": "Philly Fed Manufacturing Index", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/325-us-philly-fed-manufacturing-index"},
    {"pays": "USD", "categorie": "croissance", "nom_affichage": "Chicago PMI",
     "nom_calendrier": "Chicago PMI", "source": "forexfactory",
     "url": "https://www.forexfactory.com/calendar/189-us-chicago-pmi"},
]


def id_indicateur(cfg):
    """ID de document stable et lisible : pays_categorie_nomslug."""
    slug = re.sub(r"[^a-z0-9]+", "_", cfg["nom_affichage"].lower()).strip("_")
    return f"{cfg['pays'].lower()}_{cfg['categorie']}_{slug}"


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def hash_id(texte):
    return hashlib.sha256(texte.encode("utf-8")).hexdigest()


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


# ---------- SCRAPING DU CALENDRIER (tous evenements de la semaine) ----------
def nettoyer_valeur(texte):
    """Retire l'icone 'revised' et les espaces en trop d'une valeur comme
    Actual/Forecast/Previous."""
    if not texte:
        return None
    texte = texte.strip()
    return texte if texte else None


def scraper_calendrier():
    """Scrape la semaine en cours du calendrier ForexFactory. Retourne une
    liste de dicts bruts : date (str AAAA-MM-JJ), heure, devise, titre,
    actual, forecast, previous, impact.

    ATTENTION selecteurs a verifier au premier run reel (voir avertissement
    en tete de fichier)."""
    reponse = requests.get(URL_CALENDRIER, headers=HEADERS, timeout=20)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    evenements = []
    annee_courante = datetime.now(timezone.utc).year
    jour_courant = None

    lignes = soup.select("tr.calendar__row")
    for ligne in lignes:
        # Ligne d'en-tete de jour (ex: "Mon Sep 7") : met a jour jour_courant
        cell_date = ligne.select_one(".calendar__date")
        if cell_date:
            texte_date = cell_date.get_text(strip=True)
            if texte_date:
                try:
                    # Format attendu : "Mon Sep 7" -> on ajoute l'annee courante
                    date_parsee = datetime.strptime(f"{texte_date} {annee_courante}", "%a %b %d %Y")
                    jour_courant = date_parsee.date()
                except ValueError:
                    pass

        cell_event = ligne.select_one(".calendar__event")
        if not cell_event or not jour_courant:
            continue
        titre = cell_event.get_text(strip=True)
        if not titre:
            continue

        cell_currency = ligne.select_one(".calendar__currency")
        devise = cell_currency.get_text(strip=True) if cell_currency else ""

        cell_time = ligne.select_one(".calendar__time")
        heure = cell_time.get_text(strip=True) if cell_time else ""

        cell_impact = ligne.select_one(".calendar__impact span")
        impact = cell_impact.get("title", "") if cell_impact else ""

        cell_actual = ligne.select_one(".calendar__actual")
        cell_forecast = ligne.select_one(".calendar__forecast")
        cell_previous = ligne.select_one(".calendar__previous")

        evenements.append({
            "date": jour_courant.isoformat(),
            "heure": heure,
            "devise": devise,
            "titre": titre,
            "actual": nettoyer_valeur(cell_actual.get_text(strip=True) if cell_actual else None),
            "forecast": nettoyer_valeur(cell_forecast.get_text(strip=True) if cell_forecast else None),
            "previous": nettoyer_valeur(cell_previous.get_text(strip=True) if cell_previous else None),
            "impact": impact,
        })

    return evenements


# ---------- BACKFILL (amorcage historique, une seule fois par indicateur) ----------
def extraire_historique_forexfactory(url):
    """Visite une page indicateur ForexFactory, retourne un historique =
    liste de {date, actual, forecast, previous}, la plus recente en
    premier (jusqu'a 8 entrees visibles sans pagination)."""
    reponse = requests.get(url, headers=HEADERS, timeout=20)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    historique = []
    table = soup.find("table")
    if table:
        for ligne in table.select("tr")[1:]:  # on saute l'en-tete
            cellules = ligne.find_all("td")
            if len(cellules) < 4:
                continue
            date_texte = cellules[0].get_text(strip=True)
            historique.append({
                "date": date_texte,
                "actual": nettoyer_valeur(cellules[1].get_text(strip=True)),
                "forecast": nettoyer_valeur(cellules[2].get_text(strip=True)),
                "previous": nettoyer_valeur(cellules[3].get_text(strip=True)),
            })

    return historique[:HISTORIQUE_MAX_POINTS]


def extraire_historique_tradingeconomics(url):
    """Visite une page indicateur TradingEconomics, retourne un historique
    court (3 entrees visibles gratuitement, colonne 'Consensus' utilisee
    comme equivalent de Forecast)."""
    reponse = requests.get(url, headers=HEADERS, timeout=20)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    historique = []
    table = soup.find("table", {"class": re.compile("table")})
    if table:
        for ligne in table.select("tr")[1:]:
            cellules = ligne.find_all("td")
            if len(cellules) < 5:
                continue
            date_texte = cellules[0].get_text(strip=True)
            historique.append({
                "date": date_texte,
                "actual": nettoyer_valeur(cellules[2].get_text(strip=True)) if len(cellules) > 2 else None,
                "forecast": nettoyer_valeur(cellules[4].get_text(strip=True)) if len(cellules) > 4 else None,
                "previous": nettoyer_valeur(cellules[3].get_text(strip=True)) if len(cellules) > 3 else None,
            })

    return historique[:HISTORIQUE_MAX_POINTS]


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    # Cache local : ici utilise comme "cache de VALEUR" (pas juste "deja
    # vu") - on stocke la derniere valeur actual connue par indicateur/
    # evenement, pour ne reecrire Firestore QUE si elle a change (evite de
    # gaspiller le quota d'ecriture a chaque cycle pour rien).
    cache = charger_cache(NOM_SOURCE)

    evenements_vus = 0
    evenements_ecrits = 0

    # ---- 1) Calendrier : tous les evenements de la semaine ----
    try:
        evenements = scraper_calendrier()
        evenements_vus = len(evenements)
        print(f"{evenements_vus} evenement(s) trouve(s) sur le calendrier.")
    except Exception as e:
        print(f"Erreur scraping calendrier : {e}")
        evenements = []
        enregistrer_statut_pipeline(db, statut="erreur", erreur=f"Calendrier: {e}")
        sauvegarder_cache(NOM_SOURCE, cache)
        return

    # Index des evenements par (devise, titre) pour la correspondance avec
    # les indicateurs suivis (etape 2).
    evenements_par_cle = {}

    for ev in evenements:
        doc_id = hash_id(f"{ev['date']}|{ev['heure']}|{ev['devise']}|{ev['titre']}")
        cle_cache = f"event:{doc_id}"
        valeur_actuelle = ev.get("actual")
        derniere_valeur_connue = cache.get(cle_cache)

        evenements_par_cle[(ev["devise"], ev["titre"])] = ev

        # On ne reecrit Firestore que si la valeur Actual a change (ou
        # premiere fois qu'on voit cet evenement).
        if derniere_valeur_connue == valeur_actuelle:
            continue

        try:
            db.collection(COLLECTION_EVENTS).document(doc_id).set({
                "date": ev["date"],
                "heure": ev["heure"],
                "devise": ev["devise"],
                "titre": ev["titre"],
                "actual": ev["actual"],
                "forecast": ev["forecast"],
                "previous": ev["previous"],
                "impact": ev["impact"],
                "date_recuperation": maintenant,
            }, merge=True)
            marquer_traite(cache, cle_cache)
            cache[cle_cache] = valeur_actuelle if valeur_actuelle is not None else ""
            evenements_ecrits += 1
        except ResourceExhausted as e:
            print(f"Quota Firestore depasse (evenements), arret : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur", liens_vus=evenements_vus,
                articles_nouveaux=evenements_ecrits,
                erreur="Quota Firestore depasse (evenements)",
            )
            return
        except Exception as e:
            print(f"Erreur ecriture evenement {ev['titre']} : {e}")

    # ---- 2) Indicateurs suivis : backfill + mise a jour via calendrier ----
    indicateurs_maj = 0
    for cfg in CONFIG_INDICATEURS:
        doc_id = id_indicateur(cfg)
        doc_ref = db.collection(COLLECTION_INDICATEURS).document(doc_id)

        try:
            doc_existant = doc_ref.get()
            a_historique = doc_existant.exists and doc_existant.to_dict().get("historique")
        except ResourceExhausted as e:
            print(f"Quota Firestore depasse (lecture indicateur), arret : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur", liens_vus=evenements_vus,
                articles_nouveaux=evenements_ecrits + indicateurs_maj,
                erreur="Quota Firestore depasse (lecture indicateur)",
            )
            return

        maj = {
            "pays": cfg["pays"], "categorie": cfg["categorie"],
            "nom_affichage": cfg["nom_affichage"], "source": cfg["source"],
            "url_source": cfg["url"], "date_recuperation": maintenant,
        }

        # --- Amorcage (une seule fois) ---
        if not a_historique:
            try:
                if cfg["source"] == "forexfactory":
                    historique = extraire_historique_forexfactory(cfg["url"])
                else:
                    historique = extraire_historique_tradingeconomics(cfg["url"])
                maj["historique"] = historique
                if historique:
                    maj["valeur_actuelle"] = historique[0].get("actual")
                    maj["date_valeur_actuelle"] = historique[0].get("date")
                print(f"Amorce : {cfg['nom_affichage']} ({len(historique)} points d'historique)")
            except Exception as e:
                print(f"Erreur amorcage {cfg['nom_affichage']} : {e}")
            time.sleep(1)

        # --- Mise a jour via calendrier (indicateurs ForexFactory) ---
        if cfg["source"] == "forexfactory" and cfg["nom_calendrier"]:
            ev = evenements_par_cle.get((cfg["pays"], cfg["nom_calendrier"]))
            if ev and ev.get("actual"):
                maj["valeur_actuelle"] = ev["actual"]
                maj["date_valeur_actuelle"] = ev["date"]

        try:
            doc_ref.set(maj, merge=True)
            indicateurs_maj += 1
        except ResourceExhausted as e:
            print(f"Quota Firestore depasse (ecriture indicateur), arret : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur", liens_vus=evenements_vus,
                articles_nouveaux=evenements_ecrits + indicateurs_maj,
                erreur="Quota Firestore depasse (ecriture indicateur)",
            )
            return
        except Exception as e:
            print(f"Erreur ecriture indicateur {cfg['nom_affichage']} : {e}")

    sauvegarder_cache(NOM_SOURCE, cache)

    print(f"\nTermine. {evenements_ecrits} evenement(s) mis a jour, {indicateurs_maj} indicateur(s) traite(s).")
    enregistrer_statut_pipeline(
        db, statut="ok",
        liens_vus=evenements_vus,
        articles_nouveaux=evenements_ecrits + indicateurs_maj,
    )


if __name__ == "__main__":
    cycle()
