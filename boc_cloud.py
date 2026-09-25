# -*- coding: utf-8 -*-
"""
Script d'extraction - Monetary Policy Report (MPR / RPM) de la Banque du
Canada (BoC), version cloud.
--------------------------------------------------------------------------
Collecte le scénario de référence ("base-case projection") publié 4 fois
par an par la BoC (janvier, avril, juillet, octobre) : Tableau 2
(contributions à la croissance annuelle du PIB réel) et Tableau 3 (résumé
trimestriel : inflation IPC, inflation core, PIB réel).

VERIFIE contre le vrai site avant d'ecrire ce script (rapport d'avril
2026) : contrairement a la BoE, ces tableaux sont de VRAIS <table> HTML
(pas de texte colle sans separateur) - _find_table_by_content() et
find_all("tr")/("td") fonctionnent normalement ici. Le Tableau 3 a une
particularite verifiee sur le vrai rapport : une colonne "vide" (aucun
libelle d'annee NI de trimestre) separe le detail trimestriel a court
terme (ex: 2025 Q3, Q4, 2026 Q1, Q2) des chiffres annuels Q4/Q4 a plus
long terme (ex: 2025, 2026, 2027, 2028) - geree par gap_idx dans
parse_table3().

Format d'URL : la BoC a renouvele son site courant 2024-2025. Seuls les
rapports utilisant le nouveau schema (/publications/mpr/mpr-AAAA-MM-JJ/)
sont collectes - confirme par la liste reelle du site : les rapports
d'octobre 2024 et posterieurs l'utilisent, les plus anciens utilisent un
schema different (/AAAA/MM/mpr-AAAA-MM-JJ/) et sont ignores proprement
(le regex de decouverte ne les matche simplement pas, aucun crash).

Pagination de la liste des rapports (?mt_page=N) VERIFIEE : rendue
cote serveur, une simple requete GET suffit (pas de rendu JavaScript
necessaire, a la difference de ce qu'on avait crains initialement pour
la BoE).

Le reste de la mecanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, cache local a retention longue, PAS d'appel a
generer_json) est calque a l'identique sur boe_cloud.py / ecbproj_cloud.py.
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
LISTING_URL_TMPL = "https://www.bankofcanada.ca/publications/mpr/?mt_page={page}"
MAX_LISTING_PAGES = 15  # filet de securite pour ne pas boucler indefiniment

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

COLLECTION = "boc_mpr"
NOM_SOURCE = "bocmp"  # identifiant unique de ce script dans pipeline_status

RETENTION_CACHE_JOURS = 3650  # ~10 ans : un rapport BoC deja vu ne revient jamais

# Format moderne uniquement (post-refonte du site ~2024-2025). Les
# rapports plus anciens utilisent un schema d'URL different et ne
# matchent simplement pas ce regex (voir en-tete de fichier).
REPORT_URL_RE = re.compile(
    r'https://www\.bankofcanada\.ca/publications/mpr/(mpr-\d{4}-\d{2}-\d{2})/?"'
)

TABLE2_ROW_PATTERNS = [
    ("consumption", re.compile(r"^consumption$", re.I)),
    ("housing", re.compile(r"^housing$", re.I)),
    ("government", re.compile(r"^government$", re.I)),
    ("business_investment", re.compile(r"business fixed investment", re.I)),
    ("final_domestic_demand", re.compile(r"final domestic demand", re.I)),
    ("exports", re.compile(r"^exports$", re.I)),
    ("imports", re.compile(r"^imports$", re.I)),
    ("inventories", re.compile(r"^inventories$", re.I)),
    ("gdp", re.compile(r"^gdp$", re.I)),
    ("potential_output_range", re.compile(r"potential output", re.I)),
    ("cpi_inflation_annual", re.compile(r"cpi inflation", re.I)),
]

TABLE3_ROW_PATTERNS = [
    ("cpi_inflation", re.compile(r"^cpi inflation", re.I)),
    ("core_inflation", re.compile(r"^core inflation", re.I)),
    ("real_gdp_yoy", re.compile(r"real gdp \(year", re.I)),
    ("real_gdp_qoq", re.compile(r"real gdp \(quarter", re.I)),
]


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Signature identique aux autres sources (liens_vus = nb de rapports
    vus sur la liste, articles_nouveaux = nb de rapports ecrits)."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SCRAPING / PARSING (repris de boc_mpr_scraper.py, verifie contre le vrai site) ----------
def fetch(url):
    reponse = requests.get(url, headers=HEADERS, timeout=30)
    reponse.raise_for_status()
    reponse.encoding = "utf-8"
    return reponse.text


def discover_report_dates():
    """Parcourt les pages de la liste des RPM et retourne les dates
    (AAAA-MM-JJ) de tous les rapports utilisant le schema d'URL moderne.
    S'arrete des qu'une page ne ramene plus rien de neuf (rapports plus
    anciens au format d'URL different, ou fin de la liste)."""
    dates = set()
    for page in range(1, MAX_LISTING_PAGES + 1):
        url = LISTING_URL_TMPL.format(page=page)
        try:
            html = fetch(url)
        except requests.HTTPError:
            break

        trouve = set(m.group(1)[4:] for m in REPORT_URL_RE.finditer(html))
        if not trouve or trouve <= dates:
            # Rien de nouveau sur cette page -> fin de la liste utile
            # (rapports plus anciens au format d'URL different).
            break
        dates.update(trouve)
        time.sleep(0.5)

    return sorted(dates)


def _clean(text):
    return re.sub(r"\s+", " ", text).strip()


def _split_value_prior(text):
    """Separe une cellule "2.2 (2.2)" en (valeur_actuelle, valeur_rapport_precedent).
    Gere aussi les valeurs simples "2.5" (pas de comparaison) et les cellules vides."""
    text = _clean(text)
    if not text or text in {"-", "—", ".."}:
        return None, None
    m = re.match(r"^(.*?)(?:\s*\(([^()]+)\))?$", text)
    if not m:
        return text, None
    value = m.group(1).strip()
    prior = m.group(2).strip() if m.group(2) else None
    return (value or None), prior


def _expand_row(row):
    out = []
    for cell in row.find_all(["th", "td"]):
        text = _clean(cell.get_text(" "))
        colspan = int(cell.get("colspan", 1))
        out.extend([text] * colspan)
    return out


def _find_table_by_content(soup, patterns, min_matches):
    """Repere la <table> voulue par son contenu (libelles de lignes
    attendus) plutot que par son numero ("Table 2", "Table 3"), qui varie
    d'une edition du RPM a l'autre selon le nombre de graphiques qui la
    precedent sur la page."""
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


def parse_table2(html):
    """Tableau des contributions a la croissance annuelle du PIB reel."""
    soup = BeautifulSoup(html, "html.parser")
    table = _find_table_by_content(soup, TABLE2_ROW_PATTERNS, min_matches=4)
    if table is None:
        raise ValueError("Tableau des contributions au PIB introuvable.")

    rows = table.find_all("tr")
    header_cells = _expand_row(rows[0])
    years = header_cells[1:]

    result = {}
    for row in rows[1:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        texts = [_clean(c.get_text(" ")) for c in cells]
        if not texts or not texts[0]:
            continue
        label = texts[0]

        matched_key = None
        for key, pattern in TABLE2_ROW_PATTERNS:
            if pattern.search(label):
                matched_key = key
                break
        if not matched_key:
            continue

        entry = {}
        for year, cell_text in zip(years, texts[1:]):
            value, prior = _split_value_prior(cell_text)
            if value is not None:
                entry[year] = {"value": value, "prior": prior}
        result[matched_key] = {"label": label, "years": entry}

    return result


def parse_table3(html):
    """Tableau 3 : resume de la projection trimestrielle - deux lignes
    d'en-tete (annee puis trimestre), avec une colonne "vide" separant le
    detail trimestriel a court terme des chiffres annuels (Q4/Q4) a plus
    long terme (verifie sur le vrai rapport d'avril 2026)."""
    soup = BeautifulSoup(html, "html.parser")
    table = _find_table_by_content(soup, TABLE3_ROW_PATTERNS, min_matches=3)
    if table is None:
        raise ValueError("Tableau de resume trimestriel introuvable.")

    rows = table.find_all("tr")
    if len(rows) < 3:
        raise ValueError("Tableau 3 : structure d'en-tete inattendue.")

    year_row = _expand_row(rows[0])[1:]
    quarter_row = _expand_row(rows[1])

    n = min(len(year_row), len(quarter_row))
    year_row, quarter_row = year_row[:n], quarter_row[:n]

    gap_idx = None
    for i, (y, q) in enumerate(zip(year_row, quarter_row)):
        if not y and not q:
            gap_idx = i
            break

    quarterly_cols = []
    annual_cols = []

    if gap_idx is not None:
        quarterly_cols = [i for i in range(gap_idx) if year_row[i] and quarter_row[i]]
        annual_cols = [i for i in range(gap_idx + 1, n) if year_row[i]]
    else:
        # Pas de colonne vide trouvee : cherche le plus long suffixe
        # d'annees strictement consecutives (>= 2 colonnes) comme bloc
        # annuel, repli du script d'origine pour les editions qui
        # omettent la colonne de separation visuelle.
        annual_start = n
        for i in range(n - 1, 0, -1):
            try:
                y_cur, y_prev = int(year_row[i]), int(year_row[i - 1])
            except ValueError:
                break
            if y_cur == y_prev + 1:
                annual_start = i - 1
            else:
                break
        if annual_start < n - 1:
            annual_cols = [i for i in range(annual_start, n) if year_row[i]]
            quarterly_cols = [i for i in range(annual_start) if year_row[i] and quarter_row[i]]
        else:
            quarterly_cols = [i for i in range(n) if year_row[i] and quarter_row[i]]

    quarterly = {}
    annual = {}

    for row in rows[2:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        texts = [_clean(c.get_text(" ")) for c in cells]
        if not texts or not texts[0]:
            continue
        label = texts[0]
        data_cells = texts[1:1 + n]

        matched_key = None
        for key, pattern in TABLE3_ROW_PATTERNS:
            if pattern.search(label):
                matched_key = key
                break
        if not matched_key:
            continue

        q_entry = {}
        for i in quarterly_cols:
            if i >= len(data_cells):
                continue
            value, prior = _split_value_prior(data_cells[i])
            if value is not None:
                period = f"{year_row[i]}-{quarter_row[i]}"
                q_entry[period] = {"value": value, "prior": prior}

        a_entry = {}
        for i in annual_cols:
            if i >= len(data_cells):
                continue
            value, prior = _split_value_prior(data_cells[i])
            if value is not None:
                a_entry[year_row[i]] = {"value": value, "prior": prior}

        if q_entry:
            quarterly[matched_key] = {"label": label, "periods": q_entry}
        if a_entry:
            annual[matched_key] = {"label": label, "years": a_entry}

    return quarterly, annual


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    try:
        dates = discover_report_dates()
    except Exception as e:
        print(f"Erreur recuperation de la liste des RPM : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(dates)} RPM trouve(s) (format d'URL moderne) : {', '.join(dates)}")

    cache = charger_cache(NOM_SOURCE)

    rapports_ecrits = 0
    echecs = 0

    for date in dates:
        doc_id = date  # "AAAA-MM-JJ", deja unique

        if doc_id in cache:
            continue

        base_url = f"https://www.bankofcanada.ca/publications/mpr/mpr-{date}/"
        proj_url = base_url + "projections/"

        try:
            html = fetch(proj_url)
        except requests.HTTPError as e:
            print(f"  ! {date}: page 'Projections' introuvable ({e})")
            echecs += 1
            continue

        table2 = {}
        table3_quarterly = {}
        table3_annual = {}
        erreurs_partielles = []

        try:
            table2 = parse_table2(html)
        except ValueError as e:
            erreurs_partielles.append(f"Tableau 2: {e}")

        try:
            table3_quarterly, table3_annual = parse_table3(html)
        except ValueError as e:
            erreurs_partielles.append(f"Tableau 3: {e}")

        if not table2 and not table3_annual:
            print(f"  ! {date}: aucun des 2 tableaux extrait ({' / '.join(erreurs_partielles)})")
            echecs += 1
            continue

        if erreurs_partielles:
            print(f"  i {date}: extraction partielle ({' / '.join(erreurs_partielles)})")

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": date,
                "url": proj_url,
                "table2_contributions": table2,
                "table3_quarterly": table3_quarterly,
                "table3_annual": table3_annual,
                "date_recuperation": maintenant,
            })
            marquer_traite(cache, doc_id)
            rapports_ecrits += 1
            print(f"  -> {date} : ecrit dans Firestore.")

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {rapports_ecrits} rapport(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(dates),
                articles_nouveaux=rapports_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {date} : {e}")
            echecs += 1

        time.sleep(1)  # politesse envers le serveur de la BoC

    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {rapports_ecrits} nouveau(x) rapport(s) ecrit(s) dans Firestore.")

    # Pas d'appel a generer_json(db) : boc_mpr n'est pas concerne par
    # l'archive JSON du site (meme decision que fed_sep / ecb_projections
    # / boe_mpr).

    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(dates), articles_nouveaux=rapports_ecrits)


if __name__ == "__main__":
    cycle()
