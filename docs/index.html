# -*- coding: utf-8 -*-
"""
Script d'extraction d'articles - investingLive Central Banks (version cloud)
------------------------------------------------------------------------------
Version adaptée pour tourner sur GitHub Actions (cron toutes les 5 min).

Différences par rapport à la version locale :
- Pas de boucle infinie : un seul passage (single-pass), c'est GitHub Actions
  qui se charge de relancer le script périodiquement.
- Pas de fichiers .txt locaux ni deja_vus.txt : tout est écrit et vérifié
  dans Firestore (collection "cb_articles"), pour que la donnée survive
  entre deux exécutions et soit consultable par la page web en temps réel.
- Dédup : l'ID du document Firestore = hash SHA256 de l'URL. Avant de
  traiter un article, on vérifie s'il existe déjà -> s'il existe, on saute.
- NOUVEAU : après l'écriture dans Firestore, régénère aussi docs/index.html
  (site statique publié sur GitHub Pages) via site_generator.generer_site().
"""

import os
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
URL_LISTE = "https://investinglive.com/CentralBanks/"
FENETRE_HEURES = 24  # on ne garde que les articles publiés dans les dernières 24h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

GENERER_VERSION_FR = True
LIMITE_CARACTERES_TRADUCTION = 4500
COLLECTION = "cb_articles"


# ---------- INITIALISATION FIREBASE ----------
# La clé de service Firebase est fournie via la variable d'environnement
# GOOGLE_APPLICATION_CREDENTIALS_JSON (contenu brut du fichier JSON),
# injectée par le secret GitHub Actions FIREBASE_SERVICE_ACCOUNT.
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def hash_url(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


# ---------- SCRAPING (identique à la version locale) ----------
def recuperer_liens_articles():
    reponse = requests.get(URL_LISTE, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    liens = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/central-banks/" in href.lower() and href.rstrip("/").lower() != "https://investinglive.com/central-banks":
            if href.startswith("/"):
                href = "https://investinglive.com" + href
            if href.startswith("https://investinglive.com/central-banks/"):
                liens.add(href.split("?")[0])

    return sorted(liens)


def extraire_meilleur_bloc_de_texte(soup):
    meilleur_conteneur = None
    meilleur_score = 0

    for conteneur in soup.find_all(["div", "article", "section"]):
        paragraphes = conteneur.find_all("p", recursive=False)
        texte = " ".join(p.get_text(strip=True) for p in paragraphes)
        score = len(texte)
        if score > meilleur_score:
            meilleur_score = score
            meilleur_conteneur = conteneur

    if meilleur_conteneur is None:
        return ""

    paragraphes = meilleur_conteneur.find_all("p", recursive=False)
    return "\n\n".join(p.get_text(strip=True) for p in paragraphes if p.get_text(strip=True))


def extraire_date_publication(soup):
    balise = soup.find("meta", {"property": "article:published_time"})
    if balise and balise.get("content"):
        try:
            texte_date = balise["content"].replace("Z", "+00:00")
            return datetime.fromisoformat(texte_date)
        except ValueError:
            return None
    return None


MOTIFS_ERREUR = [
    "error 500", "server error", "that's an error", "that's an error",
    "error 404", "page not found", "404 not found", "access denied",
    "forbidden", "too many requests", "rate limit",
]


def page_erreur(titre, contenu):
    """Detecte si le contenu recupere est en fait une page d'erreur
    (site source temporairement indisponible, lien casse, blocage, etc.)
    plutot qu'un vrai article."""
    texte = f"{titre or ''} {contenu or ''}".lower()
    return any(motif in texte for motif in MOTIFS_ERREUR)


def extraire_article(url):
    reponse = requests.get(url, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    titre_tag = soup.find("h1")
    titre = titre_tag.get_text(strip=True) if titre_tag else "Sans titre"

    date_pub = extraire_date_publication(soup)
    contenu = extraire_meilleur_bloc_de_texte(soup)

    return titre, date_pub, contenu


def decouper_texte(texte, limite=LIMITE_CARACTERES_TRADUCTION):
    morceaux = []
    reste = texte
    while len(reste) > limite:
        coupe = reste.rfind("\n\n", 0, limite)
        if coupe == -1:
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
        morceaux_traduits = []
        for morceau in decouper_texte(texte):
            morceaux_traduits.append(traducteur.translate(morceau))
            time.sleep(0.3)
        resultat = "\n\n".join(morceaux_traduits)
        if _est_page_erreur_traduction(resultat):
            print(" -> Page d'erreur Google Translate detectee, version originale affichee en attendant")
            return None
        return resultat
    except Exception as e:
        print(f" -> Erreur de traduction, version originale affichee en attendant : {e}")
        return None


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    debut_fenetre = maintenant - timedelta(hours=FENETRE_HEURES)

    liens = recuperer_liens_articles()
    print(f"{len(liens)} lien(s) trouve(s) sur la page liste.")

    articles_ecrits = 0
    for url in liens:
        doc_id = hash_url(url)
        doc_ref = db.collection(COLLECTION).document(doc_id)

        # Dédup : si le document existe déjà, on saute cet article
        if doc_ref.get().exists:
            continue

        try:
            titre, date_pub, contenu = extraire_article(url)

            if page_erreur(titre, contenu):
                print(f"Page d'erreur detectee, ignore : {url}")
                doc_ref.set({"url": url, "ignore": True, "raison": "page_erreur"})
                continue

            if date_pub is None:
                print(f"Date introuvable, ignore : {titre}")
                # On marque quand meme comme traite pour ne pas re-tenter en boucle
                doc_ref.set({"url": url, "ignore": True, "raison": "date_introuvable"})
                continue

            if date_pub < debut_fenetre:
                doc_ref.set({"url": url, "ignore": True, "raison": "hors_fenetre"})
                continue

            titre_fr = None
            contenu_fr = None
            if GENERER_VERSION_FR:
                titre_fr = traduire_texte(titre)
                contenu_fr = traduire_texte(contenu) if contenu else "(Contenu non trouve)"

            doc_ref.set({
                "url": url,
                "titre": titre,
                "titre_fr": titre_fr,
                "contenu": contenu if contenu else "(Contenu non trouve)",
                "contenu_fr": contenu_fr,
                "date_publication": date_pub,
                "date_recuperation": maintenant,
                "ignore": False,
            })
            articles_ecrits += 1
            print(f"Ecrit dans Firestore : {titre}")

        except Exception as e:
            print(f"Erreur sur {url} : {e}")

        time.sleep(1)

    print(f"\nTermine. {articles_ecrits} nouvel(aux) article(s) ecrit(s) dans Firestore.")

    # NOUVEAU : on régénère docs/data.json avec les données à jour des
    # deux collections (ff_news + cb_articles), lu ensuite par votre site.
    generer_json(db)


if __name__ == "__main__":
    cycle()
