# -*- coding: utf-8 -*-
"""
Script d'extraction - Projections économiques FOMC / SEP (version cloud)
--------------------------------------------------------------------------
Collecte le "Summary of Economic Projections" (SEP) publié par la Fed à
chaque réunion FOMC qui en comporte une (mars, juin, septembre, décembre),
sur federalreserve.gov.

Différences volontaires avec le patron centralbanks_cloud.py / ff_cloud.py
/ investinglive_cloud.py :
- La source ne produit pas des "articles" mais des "réunions" : 1 document
  Firestore = 1 réunion FOMC, avec un tableau de chiffres imbriqué
  (variables -> stat_type -> annee -> valeur), pas un titre/contenu.
- L'ID de document Firestore est directement la date de la réunion au
  format AAAAMMJJ (ex: "20260916") : pas de hash d'URL, une réunion FOMC
  a une seule date possible, c'est déjà un identifiant unique et stable.
- Pas de traduction (deep_translator) : ce sont des données chiffrées.
- Pas de fenêtre glissante 24h ni de notion de "page trop vieille" : au
  contraire, on veut tout l'historique depuis ANNEE_MIN.
- Cadence : cron QUOTIDIEN (pas 5 min), cette source ne change que 4x/an.
  Voir fedsep_scraper.yml.
- Cache local (cache_dedup.py) : une réunion FOMC déjà traitée ne revient
  JAMAIS (date fixe, immuable), donc on appelle sauvegarder_cache() avec
  une retention_jours très longue (~10 ans) plutôt que les 7 jours par
  défaut, pour ne pas la perdre du cache et la re-scraper/ré-écrire dans
  Firestore inutilement à chaque cycle (voir cache_dedup.py, la mise à
  jour retention_jours a été faite spécifiquement pour ce cas).

Le reste de la mécanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, ResourceExhausted protégeant les écritures) est
calqué à l'identique sur centralbanks_cloud.py. Seul point volontairement
différent : PAS d'appel à generer_json(db) en fin de cycle (fed_sep n'est
pas concerné par l'archive JSON du site, voir plus bas).
"""

import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- CONFIGURATION ----------
CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
PROJ_URL_TMPL = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl{date}.htm"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

COLLECTION = "fed_sep"
NOM_SOURCE = "fedsep"  # identifiant unique de ce script dans pipeline_status

ANNEE_MIN = 2023  # historique récupéré à partir de cette année (décision cadrage)
RETENTION_CACHE_JOURS = 3650  # ~10 ans : une réunion FOMC déjà vue ne revient jamais

# Variables cherchées dans le Tableau 1, dans l'ordre où elles apparaissent
# sur la page. On matche sur un préfixe car le libellé exact varie
# légèrement d'une année à l'autre ("Change in real GDP", etc.)
VARIABLE_PATTERNS = [
    ("gdp_growth", re.compile(r"change in real gdp", re.I)),
    ("unemployment_rate", re.compile(r"unemployment rate", re.I)),
    ("pce_inflation", re.compile(r"^pce inflation", re.I)),
    ("core_pce_inflation", re.compile(r"core pce inflation", re.I)),
    ("fed_funds_rate", re.compile(r"federal funds rate", re.I)),
]

STAT_TYPES = ["median", "central_tendency", "range"]


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Ecrit un battement de coeur dans 'pipeline_status', a CHAQUE cycle,
    meme quand aucune nouvelle reunion n'est trouvee. Signature identique
    a celle de centralbanks_cloud.py (liens_vus = nb de dates de reunion
    vues sur le calendrier, articles_nouveaux = nb de reunions ecrites)."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SCRAPING / PARSING (repris de fed_sep_scraper.py) ----------
def fetch(url):
    reponse = requests.get(url, headers=HEADERS, timeout=30)
    reponse.raise_for_status()
    # federalreserve.gov ne declare pas toujours son charset dans l'en-tete
    # HTTP ; sans ca, requests suppose ISO-8859-1 par defaut et corrompt les
    # tirets "-" ("2.0a2.3" au lieu de "2.0-2.3"). On force UTF-8.
    reponse.encoding = "utf-8"
    return reponse.text


def find_sep_meeting_dates(calendar_html):
    """Extrait toutes les dates AAAAMMJJ presentes dans des liens
    fomcprojtabl*.htm, filtrees a partir de ANNEE_MIN."""
    dates = sorted(set(re.findall(r"fomcprojtabl(\d{8})\.htm", calendar_html)))
    return [d for d in dates if int(d[:4]) >= ANNEE_MIN]


def _clean_cell(text):
    return re.sub(r"\s+", " ", text).strip()


def _looks_like_table1(table):
    """Heuristique : la bonne table contient 'Median', 'Central Tendency'
    et 'Range' dans ses en-tetes, sans dependre d'un nom de classe CSS
    precis (qui peut changer)."""
    header_text = " ".join(_clean_cell(c.get_text(" ")) for c in table.find_all(["th", "td"])[:20])
    return (
        "median" in header_text.lower()
        and "central tendency" in header_text.lower()
        and "range" in header_text.lower()
    )


def _fill_stats(target, data_cells, blocks):
    """Reparti les valeurs d'une ligne de donnees dans Median / Central
    Tendency / Range, par annee, en s'appuyant sur les blocs d'annees
    detectes dans l'en-tete."""
    idx = 0
    for stat_type, block in zip(STAT_TYPES, blocks):
        stat_dict = target.setdefault(stat_type, {})
        for year in block:
            if idx >= len(data_cells):
                break
            value = data_cells[idx]
            if value and value != "-":
                stat_dict[year] = value
            idx += 1


def parse_table1(html):
    """Parse le Tableau 1 (projections economiques) d'une page
    fomcprojtabl*.htm. Retourne (variables, prior_label, prior_variables)."""
    soup = BeautifulSoup(html, "html.parser")

    table1 = None
    for table in soup.find_all("table"):
        if _looks_like_table1(table):
            table1 = table
            break

    if table1 is None:
        raise ValueError("Impossible de localiser le Tableau 1 sur la page.")

    rows = table1.find_all("tr")

    year_header_row = None
    for row in rows[:4]:
        cells = [_clean_cell(c.get_text(" ")) for c in row.find_all(["th", "td"])]
        if sum(1 for c in cells if re.fullmatch(r"(19|20)\d{2}", c)) >= 3:
            year_header_row = cells
            break

    if year_header_row is None:
        raise ValueError("Impossible de localiser la ligne des annees dans le Tableau 1.")

    years_in_order = [c for c in year_header_row if re.fullmatch(r"(19|20)\d{2}", c) or c.lower() == "longer run"]
    block_size = 4
    blocks = [years_in_order[i:i + block_size] for i in range(0, len(years_in_order), block_size)]
    blocks = [b for b in blocks if b]

    variables = {}
    prior_label = None
    prior_variables = {}
    current_var_key = None

    for row in rows:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        texts = [_clean_cell(c.get_text(" ")) for c in cells]
        if not texts or not texts[0]:
            continue

        label = texts[0]
        data_cells = texts[1:]

        prior_match = re.match(r"^(January|February|March|April|May|June|July|August|"
                                r"September|October|November|December)\s+projection$", label, re.I)
        if prior_match:
            prior_label = label
            if current_var_key and data_cells:
                prior_variables.setdefault(current_var_key, {})
                _fill_stats(prior_variables[current_var_key], data_cells, blocks)
            continue

        matched_key = None
        for key, pattern in VARIABLE_PATTERNS:
            if pattern.search(label):
                matched_key = key
                break

        if matched_key:
            current_var_key = matched_key
            variables.setdefault(matched_key, {"label": label})
            if data_cells:
                _fill_stats(variables[matched_key], data_cells, blocks)

    return variables, prior_label, prior_variables


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    try:
        calendar_html = fetch(CALENDAR_URL)
        dates = find_sep_meeting_dates(calendar_html)
    except Exception as e:
        print(f"Erreur recuperation du calendrier FOMC : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(dates)} reunion(s) SEP trouvee(s) depuis {ANNEE_MIN} : {', '.join(dates)}")

    # Cache local de deduplication (remplace les lectures Firestore
    # doc_ref.get()). Retention tres longue : une date de reunion FOMC
    # deja vue ne revient jamais, contrairement a un lien d'article.
    cache = charger_cache(NOM_SOURCE)

    reunions_ecrites = 0
    echecs = 0

    for date in dates:
        doc_id = date  # deja unique et stable, pas besoin de hash

        # Dedup : verification LOCALE (fichier cache/fedsep.json), aucune
        # lecture Firestore.
        if doc_id in cache:
            continue

        url = PROJ_URL_TMPL.format(date=date)

        try:
            html = fetch(url)
        except requests.HTTPError as e:
            # Peut arriver si la page n'est pas encore publiee (rare, vu
            # que find_sep_meeting_dates ne remonte que des liens deja
            # presents sur le calendrier). On NE marque PAS comme traite,
            # pour retenter au prochain cycle quotidien.
            print(f"  ! {date}: page introuvable ou erreur HTTP ({e})")
            echecs += 1
            continue

        try:
            variables, prior_label, prior_variables = parse_table1(html)
        except ValueError as e:
            # Echec de parsing (format de page inhabituel). On NE marque
            # PAS comme traite non plus : mieux vaut retenter et, si ca
            # persiste, l'investiguer manuellement, plutot que de perdre
            # silencieusement une reunion.
            print(f"  ! {date}: echec du parsing ({e})")
            echecs += 1
            continue

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": date,
                "date_iso": f"{date[0:4]}-{date[4:6]}-{date[6:8]}",
                "url": url,
                "variables": variables,
                "prior_projection_label": prior_label,
                "prior_variables": prior_variables,
                "date_recuperation": maintenant,
            })
            marquer_traite(cache, doc_id)
            reunions_ecrites += 1
            print(f"  -> {date} : ecrit dans Firestore.")

        except ResourceExhausted as e:
            # Quota d'ECRITURE Firestore depasse (rare). On arrete la
            # boucle : les tentatives suivantes echoueraient pareil.
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {reunions_ecrites} reunion(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(dates),
                articles_nouveaux=reunions_ecrites,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {date} : {e}")
            echecs += 1

        time.sleep(1)  # politesse envers le serveur de la Fed

    # On sauvegarde le cache local a jour (nouvelles dates vues ce
    # cycle), avec une retention tres longue (voir en-tete du fichier).
    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {reunions_ecrites} nouvelle(s) reunion(s) ecrite(s) dans Firestore.")

    # Pas d'appel a generer_json(db) ici : fed_sep n'est pas concerne par
    # l'archive JSON (voir etape 2 du projet, decision documentee dans
    # site_generator.py) - cet appel ne ferait rien d'utile pour cette
    # source, tout en l'exposant inutilement a des erreurs de quota
    # Firestore causees par les 3 AUTRES scripts (ff_cloud.py,
    # centralbanks_cloud.py, investinglive_cloud.py, cron 5 min), qui
    # n'ont rien a voir avec le bon deroulement de CE cycle.

    # Battement de coeur : "ok_partiel" si au moins une reunion a echoue
    # (page introuvable ou parsing), "ok" sinon - meme si
    # reunions_ecrites vaut 0 (rien de neuf a publier, cas le plus
    # frequent puisque cette source ne change que 4x/an).
    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(dates), articles_nouveaux=reunions_ecrites)


if __name__ == "__main__":
    cycle()
