# -*- coding: utf-8 -*-
"""
Script d'extraction - Outlook for Economic Activity and Prices (BoJ),
version cloud.
--------------------------------------------------------------------------
Collecte les prévisions des membres du Policy Board (PIB réel, CPI hors
produits frais) publiées 4 fois par an par la Banque du Japon (janvier,
avril, juillet, octobre), par année fiscale.

Différence fondamentale avec les 4 autres sources : PAS de tableau HTML
ni de texte de rapport à parser. La BoJ publie une page "Highlights" avec
des infographies (images), et le texte ALTERNATIF de ces images donne les
chiffres exacts en phrases toutes faites, ex (VERIFIE contre le vrai
rapport de juillet 2026) :

  "Actual figures for the year-on-year rate of change in real GDP are
   0.0% for fiscal 2023, +0.5% for fiscal 2024, +0.8% for fiscal 2025.
   Forecasts are +0.6% for fiscal 2026, +0.8% for fiscal 2027, and
   +0.8% for fiscal 2028."

On lit ce texte alternatif directement, sans jamais ouvrir la moindre
image ni le moindre PDF.

Limite connue et acceptee (reprise du script d'origine) : les pages
"Highlights" n'existent que depuis janvier 2023 - la BoJ ne les publie
pas pour les rapports plus anciens (il faudrait alors parser les PDF
complets, hors perimetre ici).

Precaution anti-bot : le script d'origine utilise curl_cffi (imitation
d'empreinte TLS Chrome) en solution PRINCIPALE, avec repli sur requests
standard si la librairie n'est pas installee - signe que l'auteur a
probablement deja rencontre un blocage avec de simples requetes "nues".
Cette precaution est reprise telle quelle ici plutot que retiree, faute
de pouvoir la reproduire/infirmer depuis cet environnement de verification.

Le reste de la mecanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, cache local a retention longue, PAS d'appel a
generer_json) est calque a l'identique sur boc_cloud.py.
"""

import os
import re
import time
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- SESSION HTTP (curl_cffi si dispo, repli sur requests) ----------
# Meme logique defensive que le script d'origine : certains sites
# japonais (comme certains sites suisses/BNS) bloquent parfois les
# requetes HTTP "nues" sur la seule empreinte TLS, meme avec un
# User-Agent de navigateur correct dans les en-tetes. curl_cffi imite
# l'empreinte TLS reelle de Chrome, contournant ce type de blocage.
try:
    from curl_cffi import requests as http_client  # type: ignore
    SESSION = http_client.Session(impersonate="chrome124")
    USING_CURL_CFFI = True
except ImportError:
    import requests as http_client  # type: ignore
    SESSION = http_client.Session()
    SESSION.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    })
    USING_CURL_CFFI = False

REQUEST_TIMEOUT = 30

# ---------- CONFIGURATION ----------
INDEX_URL = "https://www.boj.or.jp/en/mopo/outlook/highlight/index.htm"
REPORT_URL_TMPL = "https://www.boj.or.jp/en/mopo/outlook/highlight/ten{code}.htm"

COLLECTION = "boj_outlook"
NOM_SOURCE = "bojoutlook"  # identifiant unique de ce script dans pipeline_status

RETENTION_CACHE_JOURS = 3650  # ~10 ans : un rapport BoJ deja vu ne revient jamais

REPORT_CODE_RE = re.compile(r"ten(\d{6})\.htm")
PERCENT_YEAR_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)%\s*for fiscal\s*(\d{4})", re.IGNORECASE)


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Signature identique aux autres sources (liens_vus = nb de rapports
    vus sur l'index, articles_nouveaux = nb de rapports ecrits)."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- SCRAPING / PARSING (repris de boj_outlook_scraper.py, verifie contre le vrai site) ----------
def fetch(url):
    reponse = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    reponse.raise_for_status()
    return reponse.text


def discover_report_codes():
    """Retourne les codes 'AAAAMM' de chaque rapport disponible (page
    'Highlights', existe seulement depuis janvier 2023)."""
    html = fetch(INDEX_URL)
    codes = sorted(set(REPORT_CODE_RE.findall(html)))
    return codes


def _parse_percent_year_pairs(alt_text):
    """Decoupe le texte alternatif en 2 groupes (observe / prevu), sur le
    mot-cle 'forecasts are' qui separe toujours les 2 (verifie contre le
    vrai rapport de juillet 2026)."""
    split_idx = alt_text.lower().find("forecasts are")
    actual_part = alt_text[:split_idx] if split_idx != -1 else alt_text
    forecast_part = alt_text[split_idx:] if split_idx != -1 else ""

    actual = {year: value for value, year in PERCENT_YEAR_RE.findall(actual_part)}
    forecast = {year: value for value, year in PERCENT_YEAR_RE.findall(forecast_part)}
    return actual, forecast


def parse_report(html):
    """Extrait les projections PIB reel / CPI depuis le texte alternatif
    des 2 infographies 'Policy Board Members' Forecasts'. Retourne un
    dict annee_fiscale -> {statut, real_gdp, cpi}."""
    soup = BeautifulSoup(html, "html.parser")

    gdp_img = soup.find("img", alt=re.compile(r"real GDP", re.IGNORECASE))
    cpi_img = soup.find("img", alt=re.compile(r"consumer price index", re.IGNORECASE))

    if gdp_img is None or cpi_img is None:
        raise ValueError("Infographie PIB reel ou CPI introuvable sur la page.")

    gdp_actual, gdp_forecast = _parse_percent_year_pairs(gdp_img["alt"])
    cpi_actual, cpi_forecast = _parse_percent_year_pairs(cpi_img["alt"])

    if not gdp_actual and not gdp_forecast:
        raise ValueError("Aucune annee fiscale trouvee dans le texte alternatif du PIB.")

    result = {}
    for status, gdp_dict, cpi_dict in (("actual", gdp_actual, cpi_actual), ("forecast", gdp_forecast, cpi_forecast)):
        for year in set(gdp_dict) | set(cpi_dict):
            result[year] = {
                "status": status,
                "real_gdp": gdp_dict.get(year),
                "cpi": cpi_dict.get(year),
            }

    return result


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    print(f"Methode HTTP : {'curl_cffi (imitation Chrome)' if USING_CURL_CFFI else 'requests standard'}")

    try:
        codes = discover_report_codes()
    except Exception as e:
        print(f"Erreur recuperation de l'index des rapports BoJ : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(codes)} rapport(s) BoJ trouve(s) (depuis janvier 2023) : {', '.join(codes)}")

    cache = charger_cache(NOM_SOURCE)

    rapports_ecrits = 0
    echecs = 0

    for code in codes:
        date = f"{code[:4]}-{code[4:]}"  # "AAAA-MM"
        doc_id = date

        if doc_id in cache:
            continue

        url = REPORT_URL_TMPL.format(code=code)

        try:
            html = fetch(url)
        except Exception as e:
            print(f"  ! {date}: page introuvable ou erreur ({e})")
            echecs += 1
            continue

        try:
            projections = parse_report(html)
        except ValueError as e:
            print(f"  ! {date}: {e}")
            echecs += 1
            continue

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": date,
                "url": url,
                "projections": projections,
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
                liens_vus=len(codes),
                articles_nouveaux=rapports_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {date} : {e}")
            echecs += 1

        time.sleep(1)  # politesse envers le serveur de la BoJ

    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {rapports_ecrits} nouveau(x) rapport(s) ecrit(s) dans Firestore.")

    # Pas d'appel a generer_json(db) : boj_outlook n'est pas concerne par
    # l'archive JSON du site (meme decision que les 4 autres sources
    # projections).

    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(codes), articles_nouveaux=rapports_ecrits)


if __name__ == "__main__":
    cycle()
