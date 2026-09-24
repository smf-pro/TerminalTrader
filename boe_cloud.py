# -*- coding: utf-8 -*-
"""
Script d'extraction - Monetary Policy Report (MPR) de la Banque d'Angleterre
(BoE), version cloud.
--------------------------------------------------------------------------
Collecte le tableau récapitulatif de projections ("Table X.A: Forecast
summary", ou "Summary of scenarios" pour les éditions plus récentes) publié
4 fois par an par la BoE.

Différences volontaires avec fedsep_cloud.py / ecbproj_cloud.py :
- La page du rapport ne contient PAS de <table> HTML standard pour ce
  tableau : le texte apparaît collé sans séparateur (ex :
  "CPI inflation (c)3.0 (3.1)1.7 (2.2)..."). VERIFIE contre le vrai site
  (rapport de fevrier 2026) avant d'ecrire ce script : le texte brut, les
  libelles de ligne et les regex de parsing correspondent exactement.
  C'est la partie la plus fragile du script (travail sur texte plutot que
  sur structure DOM) : parse_forecast_summary() leve ValueError si le
  format ne correspond pas, auquel cas on retente le format "scenarios"
  avant d'abandonner ce rapport (voir plus bas).
- 2 formats de tableau selon l'edition :
  * "Forecast summary" (format standard, la grande majorite des rapports
    depuis 2022) : une projection centrale, avec comparaison entre
    parentheses au rapport precedent.
  * "Summary of scenarios" (nouveau, vu en avril/juillet 2026 lors du choc
    petrolier Moyen-Orient) : plusieurs scenarios nommes (A/B/C ou
    "Central projection"/"Milder scenario"/"Adverse scenario") au lieu
    d'une projection unique, sans comparaison au rapport precedent.
    VERIFIE que ces noms existent bien dans le vrai rapport de juillet
    2026, mais PAS verifie ligne a ligne contre le tableau reel (page
    trop volumineuse) - a surveiller au premier run reel.
- Bank Rate dans le tableau standard = chemin IMPLICITE PAR LES MARCHES
  sur lequel les projections sont conditionnees, PAS une prevision propre
  de la BoE (a la difference de la Fed) - stocke tel quel, l'avertissement
  est juste documente ici pour eviter une mauvaise lecture cote site plus
  tard.
- Certaines valeurs utilisent des fractions unicode (¼, ½, ¾) au lieu de
  decimales - converties en decimal avant stockage.
- doc_id = date du rapport au format "AAAA-MM" (pas de jour precis
  disponible - un seul rapport possible par mois).
- MIN_SUPPORTED_PERIOD = "2022-05" : les rapports plus anciens sont
  bloques par la protection anti-bot du site (Akamai), constat du script
  d'origine - repris tel quel, pas verifie a nouveau ici (limite juste la
  profondeur d'historique, non bloquant).

Le reste de la mecanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, cache local a retention longue, PAS d'appel a
generer_json) est calque a l'identique sur ecbproj_cloud.py.
"""

import html as html_module
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
SITEMAP_URL = "https://www.bankofengland.co.uk/sitemap/monetary-policy-report"
BASE_URL = "https://www.bankofengland.co.uk"

# Contrainte technique du parseur (protection anti-bot sur les rapports
# plus anciens) - reprise du script d'origine, voir en-tete de fichier.
MIN_SUPPORTED_PERIOD = "2022-05"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

COLLECTION = "boe_mpr"
NOM_SOURCE = "boemp"  # identifiant unique de ce script dans pipeline_status

RETENTION_CACHE_JOURS = 3650  # ~10 ans : un rapport BoE deja vu ne revient jamais

# Lien vers un rapport, ex: href="https://www.bankofengland.co.uk/monetary-policy-report/2025/february-2025"
# On exige que le guillemet fermant suive immediatement le mois-annee, pour
# exclure les sous-pages (.../february-2025/annex-...), verifie contre le
# vrai sitemap.
REPORT_LINK_RE = re.compile(
    r'href="(?:https?://www\.bankofengland\.co\.uk)?(/monetary-policy-report/(\d{4})/([a-z]+-\d{4}))"'
)

ROW_LABELS = [
    ("gdp", "GDP"),
    ("cpi_inflation", "CPI inflation"),
    ("unemployment_rate", "Unemployment rate"),
    ("output_gap", "Excess supply/Excess demand"),
    ("bank_rate", "Bank Rate"),
]

# Editions avec plusieurs scenarios (A/B/C ou noms descriptifs) au lieu
# d'une projection centrale unique.
SCENARIO_ROW_LABELS = [
    ("cpi_inflation", "CPI inflation"),
    ("gdp", "GDP"),
    ("output_gap", "Excess supply/Excess demand"),
    ("unemployment_rate", "Unemployment rate"),
    ("bank_rate", "Bank Rate"),
]

SCENARIO_NAME_RE = re.compile(
    r"Scenario\s+[A-Z]\b|Central projection\b|Baseline scenario\b|"
    r"Milder scenario\b|Adverse scenario\b|Upside scenario\b|Downside scenario\b"
)

FRAC_SUFFIX = {"¼": "25", "½": "5", "¾": "75"}
_NUM_CORE = r"(?:\d+\.\d+|\d+[¼½¾]|\d+|[¼½¾])"
NUM_RE = re.compile(rf"([-+]?{_NUM_CORE})(?:\s*\(([-+]?{_NUM_CORE})\))?")
YEAR_Q_RE = re.compile(r"\d{4}\s*Q[1-4]")


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Signature identique aux autres sources (liens_vus = nb de rapports
    vus sur le sitemap, articles_nouveaux = nb de rapports ecrits)."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SCRAPING / PARSING (repris de boe_mpr_scraper.py, verifie contre le vrai site) ----------
def fetch(url):
    reponse = requests.get(url, headers=HEADERS, timeout=30)
    reponse.raise_for_status()
    reponse.encoding = "utf-8"
    return reponse.text


def discover_reports(html):
    """Retourne une liste de (date 'AAAA-MM', url), a partir de
    MIN_SUPPORTED_PERIOD."""
    mois_num = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
    }
    trouves = {}
    for m in REPORT_LINK_RE.finditer(html):
        chemin, annee, mois_annee = m.group(1), m.group(2), m.group(3)
        nom_mois = mois_annee.rsplit("-", 1)[0]
        num_mois = mois_num.get(nom_mois)
        if not num_mois:
            continue
        date = f"{annee}-{num_mois}"
        if date < MIN_SUPPORTED_PERIOD:
            continue
        trouves[date] = (date, BASE_URL + chemin)
    return sorted(trouves.values())


def _to_decimal(token):
    """Convertit un nombre eventuellement signe (+/-) contenant une
    fraction unicode (1/4, 1/2, 3/4), seule ou accolee a un entier
    ("1 1/4" = 1.25), en decimal."""
    sign = ""
    if token and token[0] in "+-":
        sign = "-" if token[0] == "-" else ""
        token = token[1:]

    m = re.match(r"^(\d+)([¼½¾])$", token)
    if m:
        entier, frac = m.groups()
        return f"{sign}{entier}.{FRAC_SUFFIX[frac]}"
    if token in FRAC_SUFFIX:
        return f"{sign}0.{FRAC_SUFFIX[token]}"
    return f"{sign}{token}"


def _diagnostic_page(html, plain):
    """Construit une ligne de diagnostic courte pour comprendre, sans avoir
    a re-uploader le HTML, POURQUOI le texte attendu n'a pas ete trouve :
    page vide/bloquee (taille), contenu jamais charge en JS (nb de <table>
    et de <script> dans le HTML BRUT), ou texte present mais sous une
    forme differente de celle attendue (recherche large, sans le format
    'Table X.Y:' exact)."""
    nb_table_html = len(re.findall(r"<table", html, re.I))
    nb_script_html = len(re.findall(r"<script", html, re.I))
    a_forecast_summary = "forecast summary" in plain.lower()
    a_summary_scenarios = "summary of scenarios" in plain.lower()
    a_bank_rate = "bank rate" in plain.lower()
    return (
        f"[diag] html={len(html)} chars, plain={len(plain)} chars, "
        f"<table> bruts={nb_table_html}, <script> bruts={nb_script_html}, "
        f"'forecast summary' present={a_forecast_summary}, "
        f"'summary of scenarios' present={a_summary_scenarios}, "
        f"'bank rate' present={a_bank_rate}"
    )


def parse_forecast_summary(html):
    """Extrait la table 'Forecast summary' du texte brut de la page (pas
    de <table> HTML standard sur ce site pour ce tableau, verifie contre
    le vrai rapport de fevrier 2026). Retourne un dict variable -> annee ->
    {value, prior}."""
    plain = re.sub(r"<[^>]+>", "", html)
    plain = html_module.unescape(plain)

    start_match = re.search(r"Table\s+\d+\.[A-Z]\s*:\s*(?:Baseline\s+)?Forecast summary", plain, re.I)
    if start_match is None:
        raise ValueError("Table 'Forecast summary' introuvable sur la page. " + _diagnostic_page(html, plain))
    start = start_match.start()

    next_table_match = re.search(r"Table\s+\d+\.[A-Z]\s*:", plain[start_match.end():])
    end_candidates = [plain.find("Footnotes", start)]
    if next_table_match:
        end_candidates.append(start_match.end() + next_table_match.start())
    end_candidates = [c for c in end_candidates if c != -1]
    end = min(end_candidates) if end_candidates else start + 2000
    section = plain[start:end]

    years = [re.sub(r"\s+", " ", y) for y in YEAR_Q_RE.findall(section)]
    years = list(dict.fromkeys(years))
    if not years:
        raise ValueError("Annees introuvables dans la table 'Forecast summary'.")
    n_years = len(years)

    label_positions = []
    for key, label in ROW_LABELS:
        label_pattern_str = r"(?:LFS\s+)?" + re.escape(label) if label == "Unemployment rate" else re.escape(label)
        pattern = re.compile(label_pattern_str + r"\s*(?:\([a-z]\))?", re.I)
        m = pattern.search(section)
        if m:
            label_positions.append((key, m.start(), m.end()))

    if not label_positions:
        raise ValueError("Aucune ligne de la table 'Forecast summary' reconnue.")

    label_positions.sort(key=lambda x: x[1])

    result = {}
    for i, (key, _, data_start) in enumerate(label_positions):
        data_end = label_positions[i + 1][1] if i + 1 < len(label_positions) else len(section)
        chunk = section[data_start:data_end]
        tokens = NUM_RE.findall(chunk)
        if not tokens:
            continue
        tokens = tokens[:n_years]
        entree = {}
        for year, (value, prior) in zip(years, tokens):
            entree[year] = {
                "value": _to_decimal(value),
                "prior": _to_decimal(prior) if prior else None,
            }
        result[key] = entree

    return result


def parse_scenario_titles(html):
    """Extrait une courte description de chaque scenario depuis la table
    'Key assumptions and judgements...', voir logique dans le script
    d'origine. Retourne {} si la correspondance n'est pas fiable plutot
    que de risquer un mauvais alignement."""
    plain = re.sub(r"<[^>]+>", "", html)
    plain = html_module.unescape(plain)

    start_match = re.search(r"Table\s+\d+\.[A-Z]\s*:\s*Key assumptions[^.]{0,100}?scenarios", plain, re.I)
    if start_match is None:
        return {}

    scenario_positions = [(re.sub(r"\s+", " ", m.group(0)).strip(), m.start())
                           for m in SCENARIO_NAME_RE.finditer(plain[start_match.end():start_match.end() + 300])]
    if not scenario_positions:
        return {}
    names = [name for name, _ in scenario_positions]

    after_names = start_match.end() + scenario_positions[-1][1] + len(names[-1])
    row_label_match = re.match(r"\s*([A-Za-z][A-Za-z '\-]{2,40}?)\s+[A-Z]", plain[after_names:])
    if row_label_match is None:
        return {}
    row_label = row_label_match.group(1).strip()
    row_start = after_names + row_label_match.start(1) + len(row_label)

    next_section = plain[row_start:row_start + 3000]
    sentences = re.split(r"(?<=\.)\s+(?=[A-Z])", next_section.strip())

    if len(sentences) < len(names):
        return {}

    descriptions = sentences[:len(names)]
    if any(SCENARIO_NAME_RE.match(d.strip()) for d in descriptions):
        return {}

    return {name: f"{row_label}: {desc.strip()}" for name, desc in zip(names, descriptions)}


def parse_scenarios(html):
    """Extrait le tableau 'Summary of scenarios' des editions qui
    presentent plusieurs scenarios au lieu d'une projection centrale
    unique (ex: avril/juillet 2026). Pas de valeur de comparaison avec le
    rapport precedent dans ce format."""
    plain = re.sub(r"<[^>]+>", "", html)
    plain = html_module.unescape(plain)

    start_match = re.search(
        r"Table\s+\d+\.[A-Z]\s*:\s*Summary of[^.]{0,80}?scenarios", plain, re.I
    )
    if start_match is None:
        raise ValueError("Table 'Summary of scenarios' introuvable sur la page. " + _diagnostic_page(html, plain))

    end = plain.find("Footnotes", start_match.end())
    section = plain[start_match.end():end if end != -1 else start_match.end() + 4000]

    years = [re.sub(r"\s+", " ", y) for y in YEAR_Q_RE.findall(section)]
    years = list(dict.fromkeys(years))
    if not years:
        raise ValueError("Annees introuvables dans le tableau des scenarios.")
    n_years = len(years)

    scenario_positions = [(re.sub(r"\s+", " ", m.group(0)).strip(), m.start())
                           for m in SCENARIO_NAME_RE.finditer(section)]
    if not scenario_positions:
        raise ValueError("Aucun scenario trouve dans le tableau.")

    result = {}
    for i, (name, pos) in enumerate(scenario_positions):
        block_end = scenario_positions[i + 1][1] if i + 1 < len(scenario_positions) else len(section)
        block = section[pos:block_end]

        label_positions = []
        for key, label in SCENARIO_ROW_LABELS:
            m = re.compile(re.escape(label) + r"\s*(?:\([a-z]\))?", re.I).search(block)
            if m:
                label_positions.append((key, m.start(), m.end()))
        label_positions.sort(key=lambda x: x[1])

        scenario_data = {}
        for j, (key, _, data_start) in enumerate(label_positions):
            data_end = label_positions[j + 1][1] if j + 1 < len(label_positions) else len(block)
            chunk = block[data_start:data_end]
            tokens = NUM_RE.findall(chunk)[:n_years]
            entree = {year: _to_decimal(value) for year, (value, _prior) in zip(years, tokens)}
            scenario_data[key] = entree

        if scenario_data:
            result[name] = scenario_data

    return result


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    try:
        html_sitemap = fetch(SITEMAP_URL)
        entries = discover_reports(html_sitemap)
    except Exception as e:
        print(f"Erreur recuperation du sitemap BoE : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(entries)} rapport(s) BoE trouve(s) depuis {MIN_SUPPORTED_PERIOD} : {', '.join(e[0] for e in entries)}")

    cache = charger_cache(NOM_SOURCE)

    rapports_ecrits = 0
    echecs = 0

    # DIAGNOSTIC TEMPORAIRE (a retirer une fois 2026-04/2026-07 verifies) :
    # ces 2 dates utilisent le format scenarios, jamais verifie ligne par
    # ligne contre le vrai site. On force leur re-traitement meme si elles
    # sont deja en cache, juste pour afficher ce qui a ete extrait.
    DATES_A_REVERIFIER = {"2026-04", "2026-07"}

    for date, url in entries:
        doc_id = date  # "AAAA-MM", deja unique (1 rapport max par mois)

        if doc_id in cache and doc_id not in DATES_A_REVERIFIER:
            continue

        try:
            html = fetch(url)
        except requests.HTTPError as e:
            print(f"  ! {date}: page introuvable ou erreur HTTP ({e})")
            echecs += 1
            continue

        forecast_summary = {}
        scenarios = {}
        scenario_titles = {}
        try:
            forecast_summary = parse_forecast_summary(html)
        except ValueError as e_standard:
            try:
                scenarios = parse_scenarios(html)
                scenario_titles = parse_scenario_titles(html)
                print(f"  i {date}: pas de projection centrale, {len(scenarios)} scenario(s) extrait(s) a la place")
            except ValueError as e_scenario:
                print(f"  ! {date}: ni projection centrale ni scenarios extraits "
                      f"(standard: {e_standard} / scenarios: {e_scenario})")
                echecs += 1
                continue

        if date in DATES_A_REVERIFIER:
            print(f"  [diag-scenarios] {date} : forecast_summary vide={not forecast_summary}, "
                  f"scenarios trouves={list(scenarios.keys())}")
            for nom_scenario, valeurs in scenarios.items():
                print(f"      - {nom_scenario!r} -> {valeurs}")
            print(f"      titres: {scenario_titles}")

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": date,
                "url": url,
                "forecast_summary": forecast_summary,
                "scenarios": scenarios,
                "scenario_titles": scenario_titles,
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
                liens_vus=len(entries),
                articles_nouveaux=rapports_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {date} : {e}")
            echecs += 1

        time.sleep(1)  # politesse envers le serveur de la BoE

    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {rapports_ecrits} nouveau(x) rapport(s) ecrit(s) dans Firestore.")

    # Pas d'appel a generer_json(db) : boe_mpr n'est pas concerne par
    # l'archive JSON du site (meme decision que fed_sep / ecb_projections).

    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(entries), articles_nouveaux=rapports_ecrits)


if __name__ == "__main__":
    cycle()
