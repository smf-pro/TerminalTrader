# -*- coding: utf-8 -*-
"""
Script d'extraction - Projections macroéconomiques BCE (version cloud)
--------------------------------------------------------------------------
Collecte les projections macroéconomiques publiées 4 fois par an par la
BCE (mars/sept, staff BCE) et l'Eurosystème (juin/déc, staff Eurosystème).

Différences volontaires avec fedsep_cloud.py (voir aussi le cadrage) :
- Pas de notion de médiane/fourchette : chaque variable/année n'a qu'UNE
  valeur ("current"), plus une révision vs le round précédent qu'on
  choisit de NE PAS stocker/afficher ici (décision de cadrage - la
  BCE republie de toute façon la valeur absolue à chaque round, la
  révision peut se recalculer plus tard cote site si besoin en
  comparant deux rounds consecutifs).
- Pas de projection de taux directeur (la BCE n'en publie pas) : les 4
  variables mises en avant sur le site sont real_gdp, hicp, hicpx,
  unemployment_rate (reprises de SUMMARY_VARIABLES dans le script
  d'origine), mais TOUTES les variables des 2 tableaux sont quand meme
  stockees en Firestore (18 au total), au cas ou le site voudrait en
  afficher davantage plus tard sans avoir a re-scraper.
- doc_id = date du round au format "AAAA-MM" (pas de jour precis
  disponible - un seul round possible par mois, deja unique).
- MIN_SUPPORTED_PERIOD = "2023-12" : contrainte technique du parseur
  (mise en page differente avant cette date, non geree), pas un choix
  de cadrage - reprise telle quelle du script d'origine.

Le reste de la mecanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, cache local via cache_dedup.py avec retention
longue, PAS d'appel a generer_json - voir fedsep_cloud.py) est calque a
l'identique sur fedsep_cloud.py.
"""

import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- CONFIGURATION ----------
ALL_RELEASES_URL = "https://www.ecb.europa.eu/press/projections/html/all-releases.en.html"
ECB_BASE = "https://www.ecb.europa.eu"

# Contrainte technique du parseur (mise en page differente avant cette
# date, tableaux non detectes) - voir en-tete de fichier.
MIN_SUPPORTED_PERIOD = "2023-12"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

COLLECTION = "ecb_projections"
NOM_SOURCE = "ecbproj"  # identifiant unique de ce script dans pipeline_status

RETENTION_CACHE_JOURS = 3650  # ~10 ans : un round BCE deja vu ne revient jamais

# Lien vers un rapport dans le HTML de la page de liste : hrefs en chemin
# RELATIF -> on capture le chemin puis reconstruit l'URL complete.
REPORT_LINK_RE = re.compile(
    r'href="(/press/projections/html/'
    r'(ecb\.projections(\d{6})_(ecbstaff|eurosystemstaff)(?:~[a-f0-9]+)?)\.en\.html)"'
)

TABLE2_ROW_PATTERNS = [
    ("real_gdp", re.compile(r"^real gdp$", re.I)),
    ("private_consumption", re.compile(r"^private consumption$", re.I)),
    ("government_consumption", re.compile(r"^government consumption$", re.I)),
    ("investment", re.compile(r"^investment$", re.I)),
    ("exports", re.compile(r"^exports", re.I)),
    ("imports", re.compile(r"^imports", re.I)),
    ("domestic_demand", re.compile(r"^domestic demand$", re.I)),
    ("net_exports", re.compile(r"^net exports$", re.I)),
    ("employment", re.compile(r"^employment", re.I)),
    ("unemployment_rate", re.compile(r"^unemployment rate$", re.I)),
]

TABLE3_ROW_PATTERNS = [
    ("hicp", re.compile(r"^hicp$", re.I)),
    ("hicpx", re.compile(r"^hicp excluding energy and food$", re.I)),
    ("hicp_excl_energy", re.compile(r"^hicp excluding energy$", re.I)),
    ("hicp_energy", re.compile(r"^hicp energy$", re.I)),
    ("hicp_food", re.compile(r"^hicp food$", re.I)),
    ("gdp_deflator", re.compile(r"^gdp deflator$", re.I)),
    ("compensation_per_employee", re.compile(r"^compensation per employee$", re.I)),
    ("unit_labour_costs", re.compile(r"^unit labour costs$", re.I)),
]


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Signature identique a fedsep_cloud.py / centralbanks_cloud.py
    (liens_vus = nb de rounds vus sur la page de liste, articles_nouveaux
    = nb de rounds ecrits)."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SCRAPING / PARSING (repris de ecb_projections_scraper.py) ----------
def fetch(url):
    reponse = requests.get(url, headers=HEADERS, timeout=30)
    reponse.raise_for_status()
    reponse.encoding = "utf-8"
    return reponse.text


def discover_reports(html):
    """Retourne une liste de (date 'AAAA-MM', staff_type, url) triee,
    a partir de MIN_SUPPORTED_PERIOD."""
    trouves = {}
    for m in REPORT_LINK_RE.finditer(html):
        chemin, yyyymm, staff_type = m.group(1), m.group(3), m.group(4)
        url = ECB_BASE + chemin
        date = f"{yyyymm[:4]}-{yyyymm[4:]}"
        if date < MIN_SUPPORTED_PERIOD:
            continue
        trouves[date] = (date, staff_type, url)  # dedoublonne (meme round reference plusieurs fois)
    return sorted(trouves.values())


def _clean(text):
    return re.sub(r"\s+", " ", text).strip()


def _find_table_by_content(soup, patterns, min_matches):
    """Repere la <table> voulue par son contenu (le numero 'Table 2'/'Table 3'
    peut varier selon les editions)."""
    best_table, best_score = None, 0
    for table in soup.find_all("table"):
        matched_keys = set()
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            label = _clean(cells[0].get_text(" "))
            if not label:
                continue
            for key, pattern in patterns:
                if pattern.search(label):
                    matched_keys.add(key)
        if len(matched_keys) > best_score:
            best_table, best_score = table, len(matched_keys)
    return best_table if best_score >= min_matches else None


def _expand_row(row):
    out = []
    for cell in row.find_all(["th", "td"]):
        text = _clean(cell.get_text(" "))
        colspan = int(cell.get("colspan", 1))
        out.extend([text] * colspan)
    return out


def _parse_table(table, patterns):
    """Parse une table a 2 lignes d'en-tete : bloc 'valeurs actuelles'
    (annees) puis bloc 'revisions vs round precedent' (memes annees).
    On ne garde ici que 'current' (voir decision de cadrage en tete de
    fichier - la revision n'est pas stockee)."""
    rows = table.find_all("tr")
    if len(rows) < 3:
        return {}

    year_row = _expand_row(rows[1])
    years = [y for y in year_row if re.fullmatch(r"(19|20)\d{2}", y)]
    n_years = len(years) // 2 if len(years) % 2 == 0 and len(years) >= 2 else len(years)
    current_years = years[:n_years]

    result = {}
    for row in rows[2:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        texts = [_clean(c.get_text(" ")) for c in cells]
        if not texts or not texts[0]:
            continue
        label = texts[0]

        matched_key = None
        for key, pattern in patterns:
            if pattern.search(label):
                matched_key = key
                break
        if not matched_key:
            continue

        data_cells = texts[1:]
        current = {}
        for i, year in enumerate(current_years):
            if i < len(data_cells) and data_cells[i]:
                current[year] = data_cells[i]

        result[matched_key] = {"label": label, "current": current}

    return result


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    try:
        html_liste = fetch(ALL_RELEASES_URL)
        entries = discover_reports(html_liste)
    except Exception as e:
        print(f"Erreur recuperation de la page de liste BCE : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(entries)} round(s) BCE trouve(s) depuis {MIN_SUPPORTED_PERIOD} : {', '.join(e[0] for e in entries)}")

    cache = charger_cache(NOM_SOURCE)

    rounds_ecrits = 0
    echecs = 0

    for date, staff_type, url in entries:
        doc_id = date  # "AAAA-MM", deja unique (1 round max par mois)

        if doc_id in cache:
            continue

        try:
            html = fetch(url)
        except requests.HTTPError as e:
            print(f"  ! {date}: page introuvable ou erreur HTTP ({e})")
            echecs += 1
            continue

        soup = BeautifulSoup(html, "html.parser")

        table2 = _find_table_by_content(soup, TABLE2_ROW_PATTERNS, min_matches=4)
        table2_data = _parse_table(table2, TABLE2_ROW_PATTERNS) if table2 is not None else {}

        table3 = _find_table_by_content(soup, TABLE3_ROW_PATTERNS, min_matches=3)
        table3_data = _parse_table(table3, TABLE3_ROW_PATTERNS) if table3 is not None else {}

        if not table2_data and not table3_data:
            print(f"  ! {date}: aucun des 2 tableaux trouve, echec du parsing")
            echecs += 1
            continue

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": date,
                "staff_type": staff_type,
                "url": url,
                "table2_gdp_labour": table2_data,
                "table3_prices_costs": table3_data,
                "date_recuperation": maintenant,
            })
            marquer_traite(cache, doc_id)
            rounds_ecrits += 1
            print(f"  -> {date} ({staff_type}) : ecrit dans Firestore.")

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {rounds_ecrits} round(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(entries),
                articles_nouveaux=rounds_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {date} : {e}")
            echecs += 1

        time.sleep(1)  # politesse envers le serveur de la BCE

    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {rounds_ecrits} nouveau(x) round(s) ecrit(s) dans Firestore.")

    # Pas d'appel a generer_json(db) : ecb_projections n'est pas
    # concernee par l'archive JSON du site (meme decision que fed_sep,
    # voir site_generator.py).

    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(entries), articles_nouveaux=rounds_ecrits)


if __name__ == "__main__":
    cycle()
