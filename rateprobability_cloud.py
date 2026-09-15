# -*- coding: utf-8 -*-
"""
test_rateprobability.py - Test d'extraction isole, SANS Firestore.

But : verifier que le parsing des deux tableaux (page d'accueil +
page detail d'une banque) fonctionne sur le vrai site, avant de brancher
quoi que ce soit sur Firestore/le cron.
N'ecrit rien nulle part a part des fichiers de debug locaux.

Usage (GitHub Actions ou local) :
    pip install selenium beautifulsoup4
    python test_rateprobability.py
"""

import os
import time
import base64
import json

from bs4 import BeautifulSoup

URL_ACCUEIL = "https://rateprobability.com/"
URL_DETAIL_TEST = "https://rateprobability.com/fed"

NOMS_VERS_CODE = {
    "federal reserve": "fed",
    "european central bank": "ecb",
    "bank of england": "boe",
    "bank of canada": "boc",
    "bank of japan": "boj",
    "reserve bank of australia": "rba",
}


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
    options.add_argument("--no-sandbox")
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
        "clip": {"x": 0, "y": 0, "width": content_size["width"],
                 "height": content_size["height"], "scale": 1},
    })
    dossier = os.path.dirname(fichier)
    if dossier:
        os.makedirs(dossier, exist_ok=True)
    with open(fichier, "wb") as f:
        f.write(base64.b64decode(resultat["data"]))


def _attendre_verification_cloudflare(driver, timeout=25):
    """Certaines pages du site passent par une page de verification
    Cloudflare ('Just a moment...') avant d'afficher le vrai contenu.
    Elle se resout generalement seule au bout de quelques secondes une
    fois le JS execute - on attend explicitement qu'elle disparaisse
    plutot que de la prendre pour la page finale (son texte est court et
    stable, ce qui faisait sortir _attendre_page_stable() trop tot)."""
    marqueurs = ["just a moment", "performing security verification", "checking your browser"]
    fin = time.time() + timeout
    while time.time() < fin:
        try:
            texte = driver.execute_script("return document.body.innerText").lower()
        except Exception:
            texte = ""
        if not any(m in texte for m in marqueurs):
            return True
        time.sleep(1)
    return False  # toujours bloque a l'expiration du delai


def _preparer_page(driver, url):
    driver.get(url)
    _attendre_verification_cloudflare(driver, timeout=25)
    _accepter_cookies(driver)
    _attendre_page_stable(driver, timeout=20)
    _masquer_publicites_avec_attente(driver)


# ---------- LECTURE DES TABLEAUX ----------
def _trouver_table(soup, mots_cles_entete):
    for table in soup.find_all("table"):
        entete = table.find("tr")
        if not entete:
            continue
        texte_entete = entete.get_text(" ", strip=True).lower()
        if all(mot in texte_entete for mot in mots_cles_entete):
            return table
    return None


def _lignes_table(table, nb_colonnes_min):
    lignes = []
    for ligne in table.find_all("tr")[1:]:
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < nb_colonnes_min:
            continue
        lignes.append([c.get_text(strip=True) for c in cellules])
    return lignes


def extraire_synthese_accueil(driver):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = _trouver_table(soup, ["next meeting", "bank"])
    if table is None:
        return {}

    syntheses = {}
    for valeurs in _lignes_table(table, nb_colonnes_min=6):
        nom_banque = valeurs[1].strip().lower()
        code = NOMS_VERS_CODE.get(nom_banque)
        if code is None:
            continue
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
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = _trouver_table(soup, ["meeting", "implied"])
    if table is None:
        return []

    # Sauvegarde TOUJOURS le HTML brut du tableau (avec ses attributs,
    # pas juste le texte visible) : si des lignes dupliquees ont un
    # attribut distinctif (data-step, data-scenario, timestamp cache...),
    # ce sera visible ici alors que le texte des cellules seul ne le
    # montre pas.
    with open("tableau_fed_brut.html", "w", encoding="utf-8") as f:
        f.write(table.prettify())

    meetings = []
    for ligne in table.find_all("tr")[1:]:
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
            # attributs bruts de la ligne, pour reperer ce qui distingue
            # deux lignes affichant la meme date
            "_attrs_ligne": dict(ligne.attrs),
            "_attrs_cellules": [dict(c.attrs) for c in cellules],
        })
    return meetings


def _lister_tables(driver, etiquette):
    """En cas d'echec, liste les en-tetes de TOUS les tableaux trouves :
    permet de voir tout de suite si le tableau existe sous un autre
    libelle, sans avoir a fouiller le HTML complet."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    tables = soup.find_all("table")
    print(f"  [debug] {len(tables)} tableau(x) trouve(s) sur la page {etiquette} :")
    for i, table in enumerate(tables):
        entete = table.find("tr")
        texte = entete.get_text(" | ", strip=True)[:150] if entete else "(pas de <tr>)"
        print(f"    #{i} : {texte}")


def main():
    # IMPORTANT : chaque page utilise sa PROPRE session de navigateur
    # (creation + fermeture independantes), plutot qu'une seule session
    # reutilisee pour naviguer d'une page a l'autre. C'est exactement ce
    # que fait copier_page.py (qui, lui, passe sans probleme sur GitHub
    # Actions) : une session fraiche qui arrive directement sur une page
    # ressemble moins a un bot qu'une session qui enchaine plusieurs
    # pages differentes d'affilee - probablement ce qui declenchait la
    # verification Cloudflare dans les runs precedents.

    # ---------- 1. PAGE D'ACCUEIL ----------
    print("=" * 70)
    print(f"TEST 1 : tableau de synthese - {URL_ACCUEIL}")
    print("=" * 70)
    syntheses = {}
    driver = _creer_navigateur()
    try:
        _preparer_page(driver, URL_ACCUEIL)
        _capture_ecran(driver, "test_accueil.png")
        syntheses = extraire_synthese_accueil(driver)
        if syntheses:
            print(f"  [OK] {len(syntheses)}/6 banque(s) extraite(s) :\n")
            for code, donnees in syntheses.items():
                print(f"    {code.upper():5} {donnees['nom_banque_source']}")
                print(f"          prochaine decision : {donnees['prochaine_decision_date']}")
                print(f"          taux {donnees['taux_actuel']} | proba {donnees['probabilite']} "
                      f"{donnees['sens_mouvement']} | outcome {donnees['outcome_implicite']}")
                print(f"          delta {donnees['delta_vs_actuel_bps']} bps | "
                      f"outlook 12m {donnees['outlook_12m_bps']} bps")
        else:
            print("  [ECHEC] tableau de synthese introuvable.")
            _lister_tables(driver, "d'accueil")
    finally:
        driver.quit()

    # ---------- 2. PAGE DETAIL (nouvelle session, independante) ----------
    print()
    print("=" * 70)
    print(f"TEST 2 : tableau detaille - {URL_DETAIL_TEST}")
    print("=" * 70)
    meetings = []
    driver = _creer_navigateur()
    try:
        _preparer_page(driver, URL_DETAIL_TEST)
        _capture_ecran(driver, "test_detail_fed.png")

        meetings = extraire_tableau_meetings(driver)
        if meetings:
            print(f"  [OK] {len(meetings)} reunion(s) extraite(s) :\n")
            for m in meetings:
                print(f"    {m['meeting']:16} taux implique {m['taux_implique']:8} "
                      f"proba {m['probabilite']:8} nb {m['nb_hikes_cuts']:6} "
                      f"delta {m['delta_vs_actuel_bps']}")
        else:
            print("  [ECHEC] tableau detaille introuvable.")
            texte_page = driver.execute_script("return document.body.innerText")
            if "just a moment" in texte_page.lower() or "security verification" in texte_page.lower():
                print("  [debug] BLOQUE PAR CLOUDFLARE : la page de verification anti-bot")
                print("          ne s'est pas resolue meme apres l'attente supplementaire.")
            _lister_tables(driver, "detail Fed")
            with open("page_source_fed.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("  [debug] HTML complet sauvegarde dans page_source_fed.html")
    finally:
        driver.quit()

    # ---------- 3. SAUVEGARDE POUR INSPECTION ----------
    resultat = {"synthese_accueil": syntheses, "meetings_fed": meetings}
    with open("resultat_test.json", "w", encoding="utf-8") as f:
        json.dump(resultat, f, ensure_ascii=False, indent=2)

    print()
    print("=" * 70)
    print("Fichiers generes : resultat_test.json, test_accueil.png, test_detail_fed.png")
    succes = bool(syntheses) and bool(meetings)
    print("RESULTAT GLOBAL :", "OK - pret pour Firestore" if succes else "ECHEC - voir [debug] ci-dessus")
    print("=" * 70)


if __name__ == "__main__":
    main()
