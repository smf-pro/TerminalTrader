# -*- coding: utf-8 -*-
"""
Bulletin trimestriel de la BNS (Banque Nationale Suisse), version cloud.
--------------------------------------------------------------------------
Contrairement aux 5 sources precedentes, la BNS ne publie PAS de tableau
HTML ni de texte structure facilement accessible : il faut telecharger le
PDF complet du bulletin trimestriel et en extraire le texte (pdfplumber),
puis y chercher au fil du texte le taux directeur, la croissance du PIB
et la projection d'inflation a 3 ans - approche necessairement plus
heuristique que les 5 autres sources.

VERIFIE contre le vrai PDF du bulletin de mars 2026 (1/2026) avant
d'ecrire ce script, ce qui a permis de trouver et corriger 2 bugs presents
dans le script d'origine (voir ci-dessous) :

1. PROJECTION D'INFLATION A 3 ANS (fiable, reprise telle quelle) :
   toujours introduite par la formule "...puts average annual inflation
   at X% for ANNEE, Y% for ANNEE+1 and Z% for ANNEE+2..." - on cherche
   cette formule puis les paires (%, annee) qui suivent. Verifie exact
   sur mars 2026 : 0.5% (2026), 0.5% (2027), 0.6% (2028).

2. CROISSANCE DU PIB (CORRIGEE) : le script d'origine prenait "les 2
   premiers % de la 1ere phrase contenant 'GDP growth'" en supposant que
   ca correspond toujours a (annee du bulletin, annee+1). Ca fonctionne
   par coincidence sur certains bulletins ("1% for 2026 and 1.5% for
   2027"), mais PAS sur d'autres qui expriment une FOURCHETTE pour une
   seule annee ("1% to 1.5% for 2025 as a whole... 1% to 1.5% for 2026")
   - l'ancienne methode aurait alors associe le haut de fourchette 2025 a
   l'annee 2026 par erreur. Corrige en reprenant la MEME technique fiable
   que l'inflation (declencheur + paires %/annee dans une fenetre de
   texte), qui associe chaque pourcentage a l'annee qui le suit reellement
   dans le texte plutot que de supposer un ordre.

3. TAUX DIRECTEUR (CORRIGE) : le script d'origine cherchait la legende
   d'un graphique ("SNB policy rate X% SNB policy rate Y%"), qui ne
   matche PAS le vrai texte extrait (un texte "Forecast December 2025,"
   s'intercale entre les 2 occurrences, pas juste des espaces). Remplace
   par une formule bien plus fiable et confirmee trouvee dans le texte
   reel, presente une seule fois par bulletin, dans la section "Monetary
   policy decision" : "the SNB decided to leave/leaves/lowers/raises its
   policy rate (unchanged )?(at|to) X%".

Precaution anti-bot (reprise du script d'origine) : le site de la BNS
bloque (403) les requetes HTTP nues -> curl_cffi (imitation TLS Chrome)
en solution principale, repli sur requests standard si absent.

Le reste de la mecanique (init Firestore, enregistrer_statut_pipeline,
cycle() single-pass, cache local a retention longue, PAS d'appel a
generer_json) est calque a l'identique sur les 5 autres sources.
"""

import io
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pdfplumber

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- SESSION HTTP (curl_cffi si dispo, repli sur requests) ----------
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
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.snb.ch/",
    })
    USING_CURL_CFFI = False

REQUEST_TIMEOUT = 30

# ---------- CONFIGURATION ----------
LANG = "en"
COLLECTION = "snb_bulletin"
NOM_SOURCE = "snbbulletin"

RETENTION_CACHE_JOURS = 3650

QUARTER_TO_MONTH = {1: "03", 2: "06", 3: "09", 4: "12"}
YEAR_START = 2023
QUARTERS = [1, 2, 3, 4]


def build_html_url(year, quarter):
    base = f"https://www.snb.ch/{LANG}/publications/quarterly-bulletin/{year}"
    if (year, quarter) >= (2023, 2):
        return f"{base}/quartbul_{year}_{quarter}_komplett"
    month = QUARTER_TO_MONTH[quarter]
    return f"{base}/{month}/quartbul_{year}_{quarter}_komplett"


PERCENT_PATTERN = re.compile(r"[-–]?\s?\d+(?:[.,]\d+)?\s?%")

POLICY_RATE_RE = re.compile(
    r"(?:decided to leave|leaves|lowers|raises)\s+its\s+policy\s+rate\s+"
    r"(?:unchanged\s+)?(?:at|to)\s+([-–]?\s?\d+(?:[.,]\d+)?\s?%)",
    re.IGNORECASE,
)

INFLATION_TRIGGER_RE = re.compile(r"average annual inflation", re.IGNORECASE)
GDP_TRIGGER_RE = re.compile(r"(?:GDP growth of|expects? growth of|anticipates? GDP growth of)", re.IGNORECASE)

PCT_FOR_YEAR_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s?%\s+for\s+(20\d{2})", re.IGNORECASE)
YEAR_THEN_PCT_RE = re.compile(r"for\s+(20\d{2})\D{0,80}?(\d+(?:[.,]\d+)?)\s?%", re.IGNORECASE)
PROJECTION_WINDOW_CHARS = 400


def _extract_percent_year_pairs(flat_text, trigger_re, bulletin_year):
    """Cherche la formule declencheur, puis associe chaque annee au
    pourcentage relie par le mot 'for' - le connecteur toujours present
    dans les 2 sens reellement observes : 'X% for ANNEE' (verifie sur
    l'inflation et le PIB de mars 2026) et 'for ANNEE, ... X%' quand
    l'annee precede (verifie sur le PIB de juin 2025). Passage 1 (le plus
    fiable) prioritaire ; passage 2 comble seulement les annees encore
    manquantes. Une distance brute par nombre de caracteres, sans ce
    connecteur, se revele ambigue quand 3 paires se suivent de pres (ex:
    '0.5% for 2026, 0.5% for 2027 and 0.6% for 2028' - VERIFIE que 'for'
    lave cette ambiguite).
    Limite : si le bulletin donne une FOURCHETTE pour une seule annee
    ("1% to 1.5% for 2025"), on ne garde que la borne directement collee
    a 'for' (pas la fourchette complete) - simplification acceptee
    plutot que de risquer une mauvaise annee."""
    trigger = trigger_re.search(flat_text)
    if not trigger:
        return {}

    window = flat_text[trigger.start():trigger.start() + PROJECTION_WINDOW_CHARS]

    year_to_percent = {}
    for pct, year in PCT_FOR_YEAR_RE.findall(window):
        if int(year) < bulletin_year:
            continue
        year_to_percent.setdefault(year, _normalize_percent(pct + "%"))

    for year, pct in YEAR_THEN_PCT_RE.findall(window):
        if int(year) < bulletin_year:
            continue
        year_to_percent.setdefault(year, _normalize_percent(pct + "%"))

    return year_to_percent


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


def fetch(url):
    reponse = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    reponse.raise_for_status()
    return reponse


def find_pdf_link(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    candidates = [a for a in soup.find_all("a", href=True) if a["href"].lower().endswith(".pdf")]
    if not candidates:
        return None
    for a in candidates:
        text = (a.get_text() or "").lower()
        href = a["href"].lower()
        if "quarterly bulletin" in text or "quartbul" in href:
            return urljoin(page_url, a["href"])
    return urljoin(page_url, candidates[0]["href"])


def extract_pdf_text(pdf_bytes):
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


def _normalize_percent(value):
    return re.sub(r"\s+", "", value).replace(",", ".").replace("–", "-")


def _diagnostic_declencheur(flat_text, trigger_re, nom):
    """Aide au diagnostic : montre OU le declencheur a ete trouve (et un
    apercu du texte autour), ou confirme qu'il est absent du tout."""
    m = trigger_re.search(flat_text)
    if not m:
        return f"[diag-{nom}] declencheur INTROUVABLE dans tout le texte"
    debut = max(0, m.start() - 30)
    apercu = flat_text[debut:m.start() + 200]
    return f"[diag-{nom}] declencheur trouve a la position {m.start()}, apercu: ...{apercu}..."


def parse_bulletin(text, bulletin_year):
    flat = re.sub(r"\s+", " ", text)

    policy_rate_match = POLICY_RATE_RE.search(flat)
    policy_rate = _normalize_percent(policy_rate_match.group(1)) if policy_rate_match else None

    inflation_projection = _extract_percent_year_pairs(flat, INFLATION_TRIGGER_RE, bulletin_year)
    gdp_growth = _extract_percent_year_pairs(flat, GDP_TRIGGER_RE, bulletin_year)

    if not gdp_growth:
        print("  " + _diagnostic_declencheur(flat, GDP_TRIGGER_RE, "PIB"))
    if policy_rate is None:
        print("  " + _diagnostic_declencheur(flat, POLICY_RATE_RE, "taux"))

    if policy_rate is None and not inflation_projection and not gdp_growth:
        raise ValueError("Aucune des 3 donnees (taux, PIB, inflation) n'a ete trouvee.")

    return {
        "policy_rate": policy_rate,
        "gdp_growth": gdp_growth,
        "inflation_projection": inflation_projection,
    }


def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    print(f"Methode HTTP : {'curl_cffi (imitation Chrome)' if USING_CURL_CFFI else 'requests standard'}")

    annee_courante = maintenant.year
    entries = [(year, q) for year in range(YEAR_START, annee_courante + 1) for q in QUARTERS
               if (year, q) <= (annee_courante, 4)]

    print(f"{len(entries)} bulletin(s) potentiel(s) a verifier (depuis {YEAR_START} T1).")

    cache = charger_cache(NOM_SOURCE)

    bulletins_ecrits = 0
    echecs = 0

    # DIAGNOSTIC TEMPORAIRE (a retirer une fois le probleme PIB/taux
    # identifie) : force le re-traitement de 2 bulletins deja en cache.
    DATES_A_REVERIFIER = {"2026-Q2", "2023-Q1"}

    for year, quarter in entries:
        doc_id = f"{year}-Q{quarter}"

        if doc_id in cache and doc_id not in DATES_A_REVERIFIER:
            continue

        page_url = build_html_url(year, quarter)

        try:
            page_response = fetch(page_url)
        except Exception as e:
            print(f"  ! {doc_id}: page introuvable ({e})")
            echecs += 1
            continue

        pdf_url = find_pdf_link(page_response.text, page_url)
        if pdf_url is None:
            print(f"  ! {doc_id}: aucun lien PDF trouve sur la page.")
            echecs += 1
            continue

        try:
            pdf_response = fetch(pdf_url)
            if pdf_response.content[:4] != b"%PDF":
                raise ValueError("le contenu telecharge n'est pas un PDF valide")
            texte = extract_pdf_text(pdf_response.content)
        except Exception as e:
            print(f"  ! {doc_id}: echec telechargement/extraction du PDF ({e})")
            echecs += 1
            continue

        if len(texte) < 500:
            print(f"  ! {doc_id}: texte extrait trop court (PDF scanne/image ?).")
            echecs += 1
            continue

        try:
            donnees = parse_bulletin(texte, year)
        except ValueError as e:
            print(f"  ! {doc_id}: {e}")
            echecs += 1
            continue

        try:
            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "date": doc_id,
                "year": year,
                "quarter": quarter,
                "url": pdf_url,
                "policy_rate": donnees["policy_rate"],
                "gdp_growth": donnees["gdp_growth"],
                "inflation_projection": donnees["inflation_projection"],
                "date_recuperation": maintenant,
            })
            marquer_traite(cache, doc_id)
            bulletins_ecrits += 1
            print(f"  -> {doc_id} : ecrit dans Firestore "
                  f"(taux={donnees['policy_rate']}, PIB={donnees['gdp_growth']}, "
                  f"inflation={donnees['inflation_projection']}).")

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {bulletins_ecrits} bulletin(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(entries),
                articles_nouveaux=bulletins_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {doc_id} : {e}")
            echecs += 1

        time.sleep(1.5)

    sauvegarder_cache(NOM_SOURCE, cache, retention_jours=RETENTION_CACHE_JOURS)

    print(f"\nTermine. {bulletins_ecrits} nouveau(x) bulletin(s) ecrit(s) dans Firestore.")

    statut = "ok_partiel" if echecs > 0 else "ok"
    enregistrer_statut_pipeline(db, statut=statut, liens_vus=len(entries), articles_nouveaux=bulletins_ecrits)


if __name__ == "__main__":
    cycle()
