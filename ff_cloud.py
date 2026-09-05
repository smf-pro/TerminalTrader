# -*- coding: utf-8 -*-
"""
Script d'extraction de news - ForexFactory News (version cloud)
------------------------------------------------------------------------------
Version adaptée pour tourner sur GitHub Actions (cron).

Différences par rapport à la version locale :
- Pas de boucle infinie : un seul passage (single-pass), c'est GitHub Actions
  qui se charge de relancer le script périodiquement.
- Pas de fichiers .txt locaux ni deja_vus_ff.txt : tout est écrit et vérifié
  dans Firestore (collection "ff_news").
- Dédup : l'ID du document Firestore = hash SHA256 de l'URL.
- Meme filtre anti "page d'erreur" que le scraper Central Banks.
- Après l'écriture dans Firestore, régénère aussi docs/data.json (et
  docs/archive/) via site_generator.generer_json().
- NOUVEAU : à CHAQUE cycle, même sans nouvelle news (ou en cas d'erreur),
  écrit un document dans la collection "pipeline_status" (un battement de
  coeur). Ça permet au site de distinguer "rien de neuf à publier" de "le
  script est en panne", en affichant la dernière fois que le script a
  réellement tourné. Ce script a PLUSIEURS points de sortie anticipée
  (page injoignable, selecteurs casses, aucune news high/medium) : chacun
  ecrit maintenant son propre statut, avec un message different selon le cas.

⚠️ Les sélecteurs CSS (.news-block__item, etc.) n'ont pas pu être testés
contre le vrai HTML en direct. Si les logs affichent "Aucune news trouvée"
lors du premier run, ouvrez https://www.forexfactory.com/news, faites
clic droit > Inspecter sur un titre, et ajustez les sélecteurs dans
recuperer_liens_articles().
"""

import os
import re
import time
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator

import firebase_admin
from firebase_admin import credentials, firestore

from site_generator import generer_json

# ---------- CONFIGURATION ----------
URL_LISTE = "https://www.forexfactory.com/news"
FENETRE_HEURES = 24

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

GENERER_VERSION_FR = True
COLLECTION = "ff_news"
NOM_SOURCE = "forexfactory"  # identifiant unique de ce script dans pipeline_status

# Filtre sur l'impact de la news. Sur ForexFactory, l'icone est une image
# dont l'URL contient "/impact/ff/high.svg" (rouge), "medium.svg" (orange)
# ou "low.svg" (jaune). On garde ici les 3 niveaux renseignes (high, medium,
# low) ; seules les news SANS icone d'impact du tout sont ignorees.
NIVEAUX_IMPACT_GARDES = {"high", "medium", "low"}

MOTIFS_ERREUR = [
    "error 500", "server error", "that's an error", "that's an error",
    "error 404", "page not found", "404 not found", "access denied",
    "forbidden", "too many requests", "rate limit",
]


# ---------- INITIALISATION FIREBASE ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def hash_url(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Ecrit un battement de coeur dans 'pipeline_status', a CHAQUE cycle,
    meme quand aucune nouvelle news n'est trouvee. C'est ce qui permet au
    site de savoir quand ce script a tourne pour la derniere fois, sans
    confondre 'rien de neuf a publier' et 'le script est en panne'."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


def page_erreur(titre, contenu):
    texte = f"{titre or ''} {contenu or ''}".lower()
    return any(motif in texte for motif in MOTIFS_ERREUR)


# ---------- SCRAPING (identique à la version locale) ----------
def parser_date_relative(texte, maintenant):
    if not texte:
        return None
    texte = texte.strip().lower()

    m = re.fullmatch(r"(\d+)\s*hr\s*(\d+)\s*min\s*ago", texte)
    if m:
        heures, minutes = int(m.group(1)), int(m.group(2))
        return maintenant - timedelta(hours=heures, minutes=minutes)

    m = re.fullmatch(r"(\d+)\s*hr\s*ago", texte)
    if m:
        return maintenant - timedelta(hours=int(m.group(1)))

    m = re.fullmatch(r"(\d+)\s*min\s*ago", texte)
    if m:
        return maintenant - timedelta(minutes=int(m.group(1)))

    m = re.fullmatch(r"(\d+)\s*d(ay|ays)?\s*ago", texte)
    if m:
        return maintenant - timedelta(days=int(m.group(1)))

    return None


def extraire_date_publication(element, maintenant):
    details = element.select_one(".news-block__details")
    if not details:
        return None
    date_span = details.select_one("span.nowrap")
    if date_span:
        return parser_date_relative(date_span.get_text(strip=True), maintenant)
    return None


def extraire_impact(element):
    """
    Determine le niveau d'impact d'une news a partir de l'icone presente
    dans le bloc .news-block__details, ex :
    <img src="https://www.forexfactory.com/resources/svg/images/impact/ff/high.svg">
    Retourne "high", "medium", "low", ou None si aucune icone d'impact
    n'est presente (certaines news n'en ont pas).
    """
    details = element.select_one(".news-block__details")
    if not details:
        return None
    icone = details.select_one("img[src*='/impact/ff/']")
    if not icone or not icone.get("src"):
        return None
    src = icone["src"].lower()
    for niveau in ("high", "medium", "low"):
        if f"/impact/ff/{niveau}" in src:
            return niveau
    return None


def recuperer_liens_articles(maintenant):
    reponse = requests.get(URL_LISTE, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    candidats = soup.select(".news-block__item")
    total_brut = len(candidats)
    resultats = []

    for element in candidats:
        if "news-block__item--comment" in element.get("class", []):
            continue

        titre_tag = element.select_one(".news-block__title a")
        if not titre_tag:
            continue
        titre = titre_tag.get_text(strip=True)
        if not titre:
            continue

        href = titre_tag.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.forexfactory.com" + href

        details = element.select_one(".news-block__details")
        source_tag = details.select_one("a") if details else None
        source = source_tag.get_text(strip=True) if source_tag else "Inconnue"
        source = re.sub(r"^from\s+", "", source, flags=re.IGNORECASE)

        preview_tag = element.select_one(".news-block__preview")
        extrait = preview_tag.get_text(strip=True) if preview_tag else ""

        date_pub = extraire_date_publication(element, maintenant)
        impact = extraire_impact(element)

        # On ne garde que les news avec impact rouge (high) ou orange
        # (medium). Les autres (low / sans icone) sont ignorees ici, avant
        # meme d'entrer dans la logique de fenetre 24h / deja_vus.
        if impact not in NIVEAUX_IMPACT_GARDES:
            continue

        resultats.append({
            "titre": titre,
            "url": href,
            "source": source,
            "extrait": extrait,
            "date_pub": date_pub,
            "impact": impact,
        })

    return resultats, total_brut


def decouper_texte(texte, limite=4500):
    morceaux = []
    reste = texte
    while len(reste) > limite:
        coupe = reste.rfind(". ", 0, limite)
        if coupe == -1:
            coupe = limite
        morceaux.append(reste[:coupe].strip())
        reste = reste[coupe:].strip()
    if reste:
        morceaux.append(reste)
    return morceaux


SIGNATURES_ERREUR_GOOGLE = ["!!1500", "that's an error", "that\u2019s an error"]


def _est_page_erreur_traduction(texte):
    texte_lower = texte.lower()
    return any(sig.lower() in texte_lower for sig in SIGNATURES_ERREUR_GOOGLE)


def traduire_texte(texte, langue_dest="fr"):
    if not texte:
        return texte
    try:
        traducteur = GoogleTranslator(source="auto", target=langue_dest)
        morceaux_traduits = [traducteur.translate(m) for m in decouper_texte(texte)]
        resultat = " ".join(morceaux_traduits)
        if _est_page_erreur_traduction(resultat):
            print(" -> Page d'erreur Google Translate detectee, version originale affichee en attendant")
            return None
        time.sleep(0.3)
        return resultat
    except Exception as e:
        print(f" -> Erreur de traduction, version originale affichee en attendant : {e}")
        return None


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    debut_fenetre = maintenant - timedelta(hours=FENETRE_HEURES)

    try:
        toutes_les_news, total_brut = recuperer_liens_articles(maintenant)
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la recuperation de la page news : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    if total_brut == 0:
        message = (
            "Aucun bloc de news trouve du tout. Les selecteurs CSS doivent "
            "probablement etre ajustes (voir la note en tete du fichier), "
            "ou le contenu est charge en JavaScript."
        )
        print(message)
        enregistrer_statut_pipeline(db, statut="erreur", erreur=message)
        return

    print(f"{total_brut} bloc(s) de news au total sur la page.")
    print(f"{len(toutes_les_news)} apres filtre d'impact (high/medium uniquement).")

    if not toutes_les_news:
        print("Aucune news high/medium sur la page pour le moment.")
        # On regenere quand meme le JSON : rien de nouveau cote ff_news,
        # mais les autres collections ont pu changer depuis la derniere
        # generation. Le cycle s'est bien deroule (statut "ok"), juste
        # sans rien de neuf a publier.
        generer_json(db)
        enregistrer_statut_pipeline(db, statut="ok", liens_vus=total_brut, articles_nouveaux=0)
        return

    news_ecrites = 0
    for news in toutes_les_news:
        doc_id = hash_url(news["url"])
        doc_ref = db.collection(COLLECTION).document(doc_id)

        if doc_ref.get().exists:
            continue

        if page_erreur(news["titre"], news["extrait"]):
            print(f"Page d'erreur detectee, ignore : {news['url']}")
            doc_ref.set({"url": news["url"], "ignore": True, "raison": "page_erreur"})
            continue

        if news["date_pub"] is None:
            print(f"Date introuvable, ignoree : {news['titre']}")
            doc_ref.set({"url": news["url"], "ignore": True, "raison": "date_introuvable"})
            continue

        if news["date_pub"] < debut_fenetre:
            doc_ref.set({"url": news["url"], "ignore": True, "raison": "hors_fenetre"})
            continue

        titre_fr = None
        extrait_fr = None
        if GENERER_VERSION_FR:
            titre_fr = traduire_texte(news["titre"])
            extrait_fr = traduire_texte(news["extrait"]) if news["extrait"] else "(Pas d'extrait disponible)"

        doc_ref.set({
            "url": news["url"],
            "titre": news["titre"],
            "titre_fr": titre_fr,
            "source": news["source"],
            "impact": news["impact"],
            "extrait": news["extrait"] if news["extrait"] else "(Pas d'extrait disponible)",
            "extrait_fr": extrait_fr,
            "date_publication": news["date_pub"],
            "date_recuperation": maintenant,
            "ignore": False,
        })
        news_ecrites += 1
        print(f"Ecrit dans Firestore : {news['titre']}")

    print(f"\nTermine. {news_ecrites} nouvelle(s) news ecrite(s) dans Firestore.")

    # On régénère docs/data.json avec les données à jour des collections,
    # lu ensuite par votre site.
    generer_json(db)

    # Battement de coeur : ce cycle s'est termine normalement, meme si
    # news_ecrites vaut 0 (tout etait deja connu ou hors fenetre).
    enregistrer_statut_pipeline(
        db, statut="ok",
        liens_vus=len(toutes_les_news),
        articles_nouveaux=news_ecrites,
    )


if __name__ == "__main__":
    cycle()
