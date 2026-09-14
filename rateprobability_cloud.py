# -*- coding: utf-8 -*-
"""
rateprobability_cloud.py - Probabilites de taux des banques centrales
(rateprobability.com), version cloud, patron TerminalTrader
------------------------------------------------------------------------------
Contrairement aux 3 autres sources (articles), les donnees ici sont un
INSTANTANE par banque centrale (taux actuel, pricing de la prochaine
decision, tableau meeting-par-meeting) qui se met a jour en continu sur le
site source - pas d'articles individuels a dedupliquer par URL.

Modele de donnees : UN document Firestore par banque centrale (ID fixe :
"fed", "ecb", "boe", "boc", "boj", "rba"), mis a jour (upsert,
set(merge=True)) a CHAQUE cycle. Avec seulement 6 documents, pas besoin de
la logique de cache/diff de data-usd.py (qui existe pour eviter des
milliers d'ecritures inutiles sur des lignes d'historique) : 6 ecritures/
cycle restent tres loin du quota Firestore meme a cadence 5 min.

Particularite technique : contrairement aux autres sources (requests +
BeautifulSoup), les valeurs affichees sur rateprobability.com sont
injectees en JavaScript APRES le chargement initial (verifie : absentes
du HTML brut renvoye par le serveur). Il faut donc un vrai navigateur
(Selenium + Chrome headless) pour recuperer les valeurs reelles. Chrome
est present sur les runners GitHub Actions standards (contrairement au
poste local ou le telechargement automatique du driver etait bloque par
le reseau) - voir l'etape setup-chrome du workflow associe.

Capture d'ecran : en plus des donnees structurees, une capture pleine
page par banque est sauvegardee dans data/rateprobability/{banque}.png,
ECRASEE a chaque cycle (pas d'historique horodate ici, pour eviter de
faire grossir le depot indefiniment a raison d'une capture toutes les
5 minutes).
"""

import os
import re
import time
import base64
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from site_generator import generer_json

# ---------- CONFIGURATION ----------
BANQUES = {
    "fed": ("Federal Reserve", "https://rateprobability.com/fed"),
    "ecb": ("Banque Centrale Europeenne", "https://rateprobability.com/ecb"),
    "boe": ("Bank of England", "https://rateprobability.com/boe"),
    "boc": ("Bank of Canada", "https://rateprobability.com/boc"),
    "boj": ("Bank of Japan", "https://rateprobability.com/boj"),
    "rba": ("Reserve Bank of Australia", "https://rateprobability.com/rba"),
}

COLLECTION = "rate_probabilities"
NOM_SOURCE = "rateprobability"
DOSSIER_CAPTURES = "data/rateprobability"


# ---------- INITIALISATION FIREBASE (identique aux autres scripts) ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Battement de coeur dans 'pipeline_status', a CHAQUE cycle."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


# ---------- NAVIGATEUR (repris du script local copier_page.py) ----------
def _creer_navigateur():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")  # necessaire sur les runners GitHub Actions
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
    )
    return driver


def _accepter_cookies(driver, timeout=5):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    selecteurs = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.XPATH, "//button[contains(translate(text(), 'ACEPT', 'acept'), 'accept')]"),
    ]
    for by, valeur in selecteurs:
        try:
            bouton = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, valeur)))
            bouton.click()
            time.sleep(1)
            return True
        except Exception:
            continue
    return False


def _attendre_page_stable(driver, timeout=20, pause=0.5, stabilite=1.0):
    fin = time.time() + timeout
    derniere_taille = -1
    stable_depuis = time.time()

    while time.time() < fin:
        if driver.execute_script("return document.readyState") == "complete":
            break
        time.sleep(0.2)

    while time.time() < fin:
        taille_actuelle = len(driver.execute_script("return document.body.innerText"))
        if taille_actuelle == derniere_taille:
            if time.time() - stable_depuis >= stabilite:
                return
        else:
            derniere_taille = taille_actuelle
            stable_depuis = time.time()
        time.sleep(pause)


def _masquer_publicites(driver):
    script = """
        const motsCles = ['ad-banner','advertisement','sticky-ad','ad-container',
                           'adsbygoogle','taboola','outbrain','criteo','mediavine',
                           'adthrive','grow-me','grow-banner','growjs','grow-widget',
                           'grow-sticky','grow-unit','grow-native','promo-banner',
                           'sticky-promo','ad-slot','ad-wrapper'];
        function masquer(el) { el.style.setProperty('display', 'none', 'important'); }
        function masquerAvecParents(el, niveaux) {
            let courant = el;
            for (let i = 0; i <= niveaux && courant && courant.tagName !== 'BODY'; i++) {
                masquer(courant);
                courant = courant.parentElement;
            }
        }
        document.querySelectorAll('*').forEach(el => {
            const idClasse = ((el.id || '') + ' ' + (el.className || '')).toLowerCase();
            if (typeof idClasse !== 'string') return;
            const contientGrowSeul = /(^|[^a-z])grow([^a-z]|$)/.test(idClasse) && !idClasse.includes('growth');
            if (motsCles.some(m => idClasse.includes(m)) || contientGrowSeul) {
                masquerAvecParents(el, 3);
            }
        });
        document.querySelectorAll('*').forEach(el => {
            const style = window.getComputedStyle(el);
            if (style.position === 'fixed' || style.position === 'sticky') {
                const z = parseInt(style.zIndex) || 0;
                const rect = el.getBoundingClientRect();
                const collePresDuBord = rect.top < 5 || (window.innerHeight - rect.bottom) < 5;
                const tailleRaisonnable = rect.height > 20 && rect.height < window.innerHeight * 0.5;
                if (z > 100 && !collePresDuBord && tailleRaisonnable) { masquer(el); }
            }
        });
    """
    try:
        driver.execute_script(script)
    except Exception:
        pass


def _masquer_publicites_avec_attente(driver, essais=3, delai=1.5):
    for _ in range(essais):
        _masquer_publicites(driver)
        time.sleep(delai)
    _masquer_publicites(driver)


def _capture_ecran(driver, fichier):
    metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
    content_size = metrics["cssContentSize"]
    resultat = driver.execute_cdp_cmd("Page.captureScreenshot", {
        "format": "png",
        "captureBeyondViewport": True,
        "clip": {"x": 0, "y": 0, "width": content_size["width"], "height": content_size["height"], "scale": 1},
    })
    os.makedirs(os.path.dirname(fichier), exist_ok=True)
    with open(fichier, "wb") as f:
        f.write(base64.b64decode(resultat["data"]))


# ---------- EXTRACTION DES DONNEES ----------
def _valeur_apres_label(texte, label, motif_valeur, fenetre=120):
    """Cherche `label` (insensible a la casse) dans `texte`, puis applique
    `motif_valeur` (regex) sur les `fenetre` caracteres qui suivent.
    Renvoie le 1er groupe capture, ou None si label/motif introuvable.
    Approche volontairement robuste au HTML/CSS exact (pas de dependance
    a une classe ou un id precis), dans l'esprit de trouver_table_history()
    dans data-usd.py."""
    m_label = re.search(re.escape(label), texte, re.IGNORECASE)
    if not m_label:
        return None
    zone = texte[m_label.end():m_label.end() + fenetre]
    m_valeur = re.search(motif_valeur, zone, re.IGNORECASE | re.DOTALL)
    return m_valeur.group(1).strip() if m_valeur else None


def extraire_tableau_meetings(driver):
    """Repere le <table> dont l'en-tete contient 'Meeting' et 'Implied
    Rate', et renvoie ses lignes sous forme de liste de dicts. Robuste
    aux classes CSS (recherche par texte d'en-tete, comme
    trouver_table_history dans data-usd.py)."""
    soup = BeautifulSoup(driver.page_source, "html.parser")

    table_cible = None
    for table in soup.find_all("table"):
        entete = table.find("tr")
        if not entete:
            continue
        texte_entete = entete.get_text(" ", strip=True).lower()
        if "meeting" in texte_entete and "implied" in texte_entete:
            table_cible = table
            break

    if table_cible is None:
        return []

    lignes = table_cible.find_all("tr")
    resultats = []
    for ligne in lignes[1:]:
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < 4:
            continue
        valeurs = [c.get_text(strip=True) for c in cellules]
        resultats.append({
            "meeting": valeurs[0] if len(valeurs) > 0 else "",
            "taux_implique": valeurs[1] if len(valeurs) > 1 else "",
            "probabilite": valeurs[2] if len(valeurs) > 2 else "",
            "nb_hikes_cuts": valeurs[3] if len(valeurs) > 3 else "",
            "delta_vs_actuel_bps": valeurs[4] if len(valeurs) > 4 else "",
        })
    return resultats


def extraire_donnees_banque(driver):
    """Extrait les champs cles de la page (taux actuel, pricing de la
    prochaine decision, outlook 12 mois) par recherche de libelle dans le
    texte visible de la page (document.body.innerText), plus le tableau
    meeting-par-meeting via le DOM. Un champ non trouve reste None plutot
    que de faire planter tout le cycle pour cette banque."""
    texte = driver.execute_script("return document.body.innerText")

    taux_actuel = _valeur_apres_label(texte, "Current Rate", r"([\d.]+%)")
    as_of = _valeur_apres_label(texte, "As of:", r"([\d:]{3,5}\s+[\d/]{6,10})")
    target_band = _valeur_apres_label(texte, "Target Band:", r"([\d.]+[\-–][\d.]+%)")

    prochaine_decision_date = _valeur_apres_label(
        texte, "Next decision in", r"\n\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}[^\n]*)", fenetre=200
    )
    prochaine_decision_pricing = _valeur_apres_label(
        texte, "Next meeting pricing", r"(\d+%\s*(?:HIKE|CUT|HOLD))"
    )
    prochaine_decision_bps = _valeur_apres_label(
        texte, "Next meeting pricing", r"HIKE|CUT|HOLD\)?\s*\n?\s*([+\-][\d.]+\s*bps)", fenetre=60
    )

    outlook_12m_bps = _valeur_apres_label(texte, "12-Month", r"([+\-][\d.]+\s*bps)")
    outlook_12m_texte = _valeur_apres_label(texte, "12-Month", r"(\d+\s*(?:or\s*\d+\s*)?(?:hikes?|cuts?))")

    tableau_meetings = extraire_tableau_meetings(driver)

    return {
        "taux_actuel": taux_actuel,
        "target_band": target_band,
        "as_of_texte": as_of,
        "prochaine_decision_date": prochaine_decision_date,
        "prochaine_decision_pricing": prochaine_decision_pricing,
        "prochaine_decision_bps": prochaine_decision_bps,
        "outlook_12m_bps": outlook_12m_bps,
        "outlook_12m_texte": outlook_12m_texte,
        "tableau_meetings": tableau_meetings,
    }


def traiter_banque(driver, code, nom, url):
    driver.get(url)
    _accepter_cookies(driver)
    _attendre_page_stable(driver, timeout=20)
    _masquer_publicites_avec_attente(driver)

    donnees = extraire_donnees_banque(driver)

    fichier_capture = os.path.join(DOSSIER_CAPTURES, f"{code}.png")
    try:
        _capture_ecran(driver, fichier_capture)
    except Exception as e:
        print(f"  capture d'ecran echouee pour {nom} : {e}")

    return donnees


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    driver = None
    banques_traitees = 0
    try:
        driver = _creer_navigateur()

        for code, (nom, url) in BANQUES.items():
            try:
                donnees = traiter_banque(driver, code, nom, url)

                doc = dict(donnees)
                doc.update({
                    "banque": nom,
                    "code_banque": code,
                    "source_url": url,
                    "date_recuperation": maintenant,
                })
                db.collection(COLLECTION).document(code).set(doc, merge=True)
                banques_traitees += 1
                print(f"OK : {nom} -> taux actuel {donnees.get('taux_actuel')}, "
                      f"prochaine decision {donnees.get('prochaine_decision_pricing')}")

            except ResourceExhausted as e:
                print(f"Quota Firestore depasse sur {nom}, arret du cycle : {e}")
                enregistrer_statut_pipeline(
                    db, statut="erreur", liens_vus=len(BANQUES),
                    articles_nouveaux=banques_traitees,
                    erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
                )
                return
            except Exception as e:
                print(f"Erreur sur {nom} ({url}) : {e}")

    except Exception as e:
        print(f"Erreur navigateur : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return
    finally:
        if driver is not None:
            driver.quit()

    print(f"\nTermine. {banques_traitees}/{len(BANQUES)} banque(s) mise(s) a jour dans Firestore.")

    try:
        generer_json(db)
    except ResourceExhausted as e:
        print(f"Quota Firestore depasse pendant generer_json() : {e}")
        enregistrer_statut_pipeline(
            db, statut="erreur", liens_vus=len(BANQUES),
            articles_nouveaux=banques_traitees,
            erreur="Quota Firestore depasse pendant la generation du JSON",
        )
        return

    enregistrer_statut_pipeline(db, statut="ok", liens_vus=len(BANQUES), articles_nouveaux=banques_traitees)


if __name__ == "__main__":
    cycle()
