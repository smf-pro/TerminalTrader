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

DEUX SOURCES COMPLEMENTAIRES, toutes deux lues comme des TABLEAUX HTML
(pas de regex sur du texte libre, qui s'etait revelee trop fragile) :

1. Page d'accueil (/) - tableau "UPCOMING MEETINGS" : une ligne par
   banque, avec date de prochaine reunion, taux directeur, probabilite,
   hike/cut, delta vs actuel, outcome implicite, outlook 12 mois. C'est
   la source des champs de synthese.
2. Pages par banque (/fed, /ecb, ...) - tableau "PATH OF ... : MARKET
   EXPECTATION" : le detail meeting par meeting (taux implique,
   probabilite, nb de hikes/cuts, delta), stocke dans tableau_meetings.

Particularite technique : les valeurs affichees sur rateprobability.com
sont injectees en JavaScript APRES le chargement initial (absentes du
HTML brut renvoye par le serveur). Il faut donc un vrai navigateur
(Selenium + Chrome headless) plutot que requests + BeautifulSoup comme
les autres sources. Chrome est fourni sur les runners GitHub Actions via
l'etape setup-chrome du workflow associe.

Capture d'ecran : en plus des donnees structurees, une capture pleine
page est sauvegardee dans data/rateprobability/{code}.png (+ accueil.png),
ECRASEE a chaque cycle (pas d'historique horodate, pour eviter de faire
grossir le depot indefiniment).
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
URL_ACCUEIL = "https://rateprobability.com/"

# code interne -> (nom affiche cote site MERIDIAN, url de la page detail)
BANQUES = {
    "fed": ("Federal Reserve", "https://rateprobability.com/fed"),
    "ecb": ("Banque Centrale Europeenne", "https://rateprobability.com/ecb"),
    "boe": ("Bank of England", "https://rateprobability.com/boe"),
    "boc": ("Bank of Canada", "https://rateprobability.com/boc"),
    "boj": ("Bank of Japan", "https://rateprobability.com/boj"),
    "rba": ("Reserve Bank of Australia", "https://rateprobability.com/rba"),
}

# Nom de banque tel qu'ecrit dans le tableau de la page d'accueil -> code
# interne. Permet de rattacher chaque ligne du tableau recapitulatif au
# bon document Firestore sans dependre de l'ordre des lignes.
NOMS_VERS_CODE = {
    "federal reserve": "fed",
    "european central bank": "ecb",
    "bank of england": "boe",
    "bank of canada": "boc",
    "bank of japan": "boj",
    "reserve bank of australia": "rba",
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


# ---------- NAVIGATEUR ----------
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
    """Attend que le DOM soit charge ET que le texte arrete de changer :
    les valeurs de ce site sont injectees en JS apres le rendu initial."""
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
    """Masque les bannieres pub flottantes (widget 'Grow' notamment) qui
    recouvrent le contenu sur les captures d'ecran."""
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
    """Le widget pub se charge en differe : on repasse plusieurs fois."""
    for _ in range(essais):
        _masquer_publicites(driver)
        time.sleep(delai)
    _masquer_publicites(driver)


def _capture_ecran(driver, fichier):
    """Capture pleine page via CDP (pas de redimensionnement de fenetre,
    qui declenchait une reorganisation de la page et un rendu duplique)."""
    metrics = driver.execute_cdp_cmd("Page.getLayoutMetrics", {})
    content_size = metrics["cssContentSize"]
    resultat = driver.execute_cdp_cmd("Page.captureScreenshot", {
        "format": "png",
        "captureBeyondViewport": True,
        "clip": {
            "x": 0, "y": 0,
            "width": content_size["width"],
            "height": content_size["height"],
            "scale": 1,
        },
    })
    os.makedirs(os.path.dirname(fichier), exist_ok=True)
    with open(fichier, "wb") as f:
        f.write(base64.b64decode(resultat["data"]))


def _preparer_page(driver, url):
    """Charge une page et la met en etat d'etre lue (cookies acceptes,
    JS termine, pubs masquees)."""
    driver.get(url)
    _accepter_cookies(driver)
    _attendre_page_stable(driver, timeout=20)
    _masquer_publicites_avec_attente(driver)


# ---------- LECTURE DES TABLEAUX ----------
def _trouver_table(soup, mots_cles_entete):
    """Renvoie le premier <table> dont la ligne d'en-tete contient TOUS
    les mots-cles donnes (insensible a la casse), ou None.

    Recherche par CONTENU d'en-tete plutot que par classe/id CSS : c'est
    ce qui rend l'extraction robuste aux changements de theme du site
    (meme esprit que trouver_table_history() dans data-usd.py)."""
    for table in soup.find_all("table"):
        entete = table.find("tr")
        if not entete:
            continue
        texte_entete = entete.get_text(" ", strip=True).lower()
        if all(mot in texte_entete for mot in mots_cles_entete):
            return table
    return None


def _lignes_table(table, nb_colonnes_min):
    """Renvoie les lignes de donnees (hors en-tete) sous forme de listes
    de chaines, en ignorant les lignes trop courtes (separateurs, lignes
    de mise en page)."""
    lignes = []
    for ligne in table.find_all("tr")[1:]:
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < nb_colonnes_min:
            continue
        lignes.append([c.get_text(strip=True) for c in cellules])
    return lignes


def extraire_synthese_accueil(driver):
    """Lit le tableau recapitulatif de la page d'accueil (une ligne par
    banque) et renvoie {code_banque: {champs de synthese}}.

    Colonnes attendues : Next Meeting | Bank | Policy Rate | Probability |
    Hike/Cut | delta vs Current (bps) | Implied Outcome | 12-Month
    Outlook (bps)."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = _trouver_table(soup, ["next meeting", "bank"])
    if table is None:
        return {}

    syntheses = {}
    for valeurs in _lignes_table(table, nb_colonnes_min=6):
        nom_banque = valeurs[1].strip().lower()
        code = NOMS_VERS_CODE.get(nom_banque)
        if code is None:
            continue  # banque presente sur le site mais hors de notre perimetre
        syntheses[code] = {
            "prochaine_decision_date": valeurs[0],
            "nom_banque_source": valeurs[1],
            "taux_actuel": valeurs[2],
            "probabilite": valeurs[3],
            "sens_mouvement": valeurs[4],
            "delta_vs_actuel_bps": valeurs[5],
            "outcome_implicite": valeurs[6] if len(valeurs) > 6 else "",
            "outlook_12m_bps": valeurs[7] if len(valeurs) > 7 else "",
        }
    return syntheses


def extraire_tableau_meetings(driver):
    """Lit le tableau detaille d'une page banque ("PATH OF ... : MARKET
    EXPECTATION") : une ligne par reunion a venir.

    Colonnes attendues : Meeting | Implied Rate (Post-Meeting) |
    Probability of Hike(Cut) | # of Hikes(Cuts) | delta vs Current (bps).

    FILTRE data-meeting : verifie empiriquement (voir tests) que les
    VRAIES lignes de reunion portent l'attribut data-meeting="..." sur
    le <tr> lui-meme. Le site affiche aussi, sous certaines reunions, une
    ligne technique SANS cet attribut et aux valeurs entre parentheses
    (notation negative) - ce n'est pas une reunion distincte, on l'ignore
    explicitement plutot que de deviner via un dedup par date (une
    premiere tentative de dedup "derniere occurrence" s'est averee
    garder la mauvaise ligne - cette version-ci est basee sur une preuve
    concrete, pas une hypothese)."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = _trouver_table(soup, ["meeting", "implied"])
    if table is None:
        return []

    meetings = []
    for ligne in table.find_all("tr")[1:]:
        if not ligne.has_attr("data-meeting"):
            continue
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < 4:
            continue
        valeurs = [c.get_text(strip=True) for c in cellules]
        meetings.append({
            "meeting": valeurs[0],
            "taux_implique": valeurs[1],
            "probabilite": valeurs[2],
            "nb_hikes_cuts": valeurs[3],
            "delta_vs_actuel_bps": valeurs[4] if len(valeurs) > 4 else "",
        })
    meetings.sort(key=lambda m: _date_ou_infini(m["meeting"]))
    return meetings


def _date_ou_infini(texte_date):
    """Convertit une date texte du style 'Sep 16, 2026' en objet triable ;
    renvoie une valeur "infinie" si illisible, pour l'envoyer en fin de
    liste plutot que de faire planter le tri."""
    from datetime import datetime as _dt
    try:
        return _dt.strptime(texte_date.strip(), "%b %d, %Y")
    except (ValueError, AttributeError):
        return _dt.max


def enregistrer_point_historique(db, code, doc, maintenant):
    """Ajoute un point dans rate_probabilities/{code}/historique/{...},
    SANS jamais ecraser les points precedents (contrairement au document
    principal, qui est un instantane mis a jour). L'historique s'allonge
    tout seul, un point par cycle - c'est ce qui permet aux graphiques
    d'evolution du site de se remplir progressivement avec de vraies
    donnees au fil du temps, au lieu des valeurs generees aleatoirement
    utilisees avant.

    Champs suivis : uniquement ceux qu'on peut reellement mesurer a
    chaque cycle (probabilite/sens de la prochaine reunion, et les 2
    mouvements implicites en pb). Le site source ne donne qu'UNE
    probabilite (pas de decomposition cut/hold/hike separee), donc le
    graphique correspondant sera une seule courbe, pas 3."""
    id_point = maintenant.strftime("%Y%m%dT%H%M%SZ")
    point = {
        "date_recuperation": maintenant,
        "probabilite": doc.get("probabilite"),
        "sens_mouvement": doc.get("sens_mouvement"),
        "outcome_implicite": doc.get("outcome_implicite"),
        "delta_vs_actuel_bps": doc.get("delta_vs_actuel_bps"),
        "outlook_12m_bps": doc.get("outlook_12m_bps"),
    }
    (db.collection(COLLECTION).document(code)
       .collection("historique").document(id_point)
       .set(point))


def _slug_date(texte_date):
    """Convertit une date texte ('Jan 27, 2027') en identifiant de
    document Firestore stable ('2027-01-27'). Repli sur le texte
    nettoye si la date est illisible, plutot que de faire planter
    l'ecriture."""
    d = _date_ou_infini(texte_date)
    if d == datetime.max:
        return re.sub(r"[^a-zA-Z0-9_-]", "-", (texte_date or "inconnue").strip()) or "inconnue"
    return d.strftime("%Y-%m-%d")


def enregistrer_historique_reunions(db, code, meetings, maintenant):
    """Ajoute un point d'historique pour CHAQUE reunion individuelle
    (pas seulement la prochaine), pour que le site puisse afficher un
    graphique d'evolution meme quand on selectionne une reunion lointaine
    plutot que la plus proche.

    Sous-collection : rate_probabilities/{code}/reunions/{slug_date}/historique/{horodatage}.
    Meme principe que enregistrer_point_historique (jamais ecrase, un
    point de plus par cycle), mais un fil d'historique separe par date de
    reunion plutot qu'un seul fil global par banque."""
    id_point = maintenant.strftime("%Y%m%dT%H%M%SZ")
    for m in meetings:
        slug = _slug_date(m.get("meeting"))
        point = {
            "date_recuperation": maintenant,
            "meeting": m.get("meeting"),
            "taux_implique": m.get("taux_implique"),
            "probabilite": m.get("probabilite"),
            "nb_hikes_cuts": m.get("nb_hikes_cuts"),
            "delta_vs_actuel_bps": m.get("delta_vs_actuel_bps"),
        }
        (db.collection(COLLECTION).document(code)
           .collection("reunions").document(slug)
           .collection("historique").document(id_point)
           .set(point))


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    """Chaque page (accueil + 6 pages banque) est visitee avec sa PROPRE
    session de navigateur (creation + fermeture independantes), plutot
    qu'une session unique reutilisee pour naviguer d'une page a l'autre.

    Verifie empiriquement (voir tests) : une session qui enchaine
    plusieurs pages differentes d'affilee declenche la verification
    anti-bot Cloudflare sur les pages /fed, /ecb, etc. (page bloquee sur
    "Just a moment..."), meme avec toutes les autres mesures
    anti-detection deja en place (user-agent, navigator.webdriver masque,
    etc.). Une session fraiche par page passe sans probleme."""
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    banques_ecrites = 0

    # --- 1. Page d'accueil : synthese des 6 banques en un seul tableau ---
    syntheses = {}
    driver = None
    try:
        driver = _creer_navigateur()
        _preparer_page(driver, URL_ACCUEIL)
        syntheses = extraire_synthese_accueil(driver)
        print(f"Synthese accueil : {len(syntheses)} banque(s) lue(s).")
        try:
            _capture_ecran(driver, os.path.join(DOSSIER_CAPTURES, "accueil.png"))
        except Exception as e:
            print(f"  capture accueil echouee : {e}")
    except Exception as e:
        # Non bloquant : on peut encore recuperer le detail par banque.
        print(f"Erreur lecture page d'accueil : {e}")
    finally:
        if driver is not None:
            driver.quit()

    # --- 2. Pages par banque : detail meeting par meeting ---
    for code, (nom, url) in BANQUES.items():
        driver = None
        try:
            driver = _creer_navigateur()
            _preparer_page(driver, url)
            meetings = extraire_tableau_meetings(driver)

            try:
                _capture_ecran(driver, os.path.join(DOSSIER_CAPTURES, f"{code}.png"))
            except Exception as e:
                print(f"  capture {code} echouee : {e}")

            doc = dict(syntheses.get(code, {}))
            doc.update({
                "banque": nom,
                "code_banque": code,
                "source_url": url,
                "tableau_meetings": meetings,
                "date_recuperation": maintenant,
            })

            db.collection(COLLECTION).document(code).set(doc, merge=True)
            enregistrer_point_historique(db, code, doc, maintenant)
            enregistrer_historique_reunions(db, code, meetings, maintenant)
            banques_ecrites += 1
            print(f"OK : {nom} -> taux {doc.get('taux_actuel')}, "
                  f"prochaine decision {doc.get('prochaine_decision_date')} "
                  f"({doc.get('probabilite')} {doc.get('outcome_implicite')}), "
                  f"{len(meetings)} reunion(s) a venir")

        except ResourceExhausted as e:
            print(f"Quota Firestore depasse sur {nom}, arret du cycle : {e}")
            enregistrer_statut_pipeline(
                db, statut="erreur", liens_vus=len(BANQUES),
                articles_nouveaux=banques_ecrites,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {nom} ({url}) : {e}")
        finally:
            if driver is not None:
                driver.quit()

    print(f"\nTermine. {banques_ecrites}/{len(BANQUES)} banque(s) mise(s) a jour dans Firestore.")

    try:
        generer_json(db)
    except ResourceExhausted as e:
        print(f"Quota Firestore depasse pendant generer_json() : {e}")
        enregistrer_statut_pipeline(
            db, statut="erreur", liens_vus=len(BANQUES),
            articles_nouveaux=banques_ecrites,
            erreur="Quota Firestore depasse pendant la generation du JSON",
        )
        return

    enregistrer_statut_pipeline(
        db, statut="ok", liens_vus=len(BANQUES), articles_nouveaux=banques_ecrites
    )


if __name__ == "__main__":
    cycle()
