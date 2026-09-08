# -*- coding: utf-8 -*-
"""
Script de RECUPERATION SEMI-MANUELLE - ForexFactory
------------------------------------------------------------------------------
Complement a ff_cloud.py, pour les news qui echappent au scraping normal
(page /news trop chargee, fenetre 24h ratee, etc.).

Fonctionnement :
1. Lit le fichier a_recuperer.txt (un lien ForexFactory par ligne).
2. Pour chaque lien, va chercher la page ARTICLE INDIVIDUELLE (pas la page
   liste /news) et en extrait titre / date / impact / extrait.
3. Ecrit dans Firestore (meme collection "ff_news", meme hash_url() que
   ff_cloud.py -> AUCUN risque de doublon, meme si l'article existe deja).
4. Si l'extraction a reussi, retire la ligne du fichier a_recuperer.txt.
   Si elle a echoue, LAISSE la ligne (elle sera retentee au prochain
   declenchement du workflow).

ATTENTION SELECTEURS : comme pour ff_cloud.py au moment de sa creation,
je n'ai pas pu tester ces selecteurs contre le vrai HTML brut (je n'ai vu
que du texte rendu, sans les classes CSS). Pour limiter la casse en cas de
structure differente d'un article a l'autre, ce script utilise volontairement
des methodes plus robustes que des selecteurs CSS precis :
  - Titre : balise <h1> (quasi jamais absente) + repli sur la meta og:title
  - Impact : recherche du motif "impact/ff/(high|medium|low)" n'importe ou
    dans le HTML brut (deja valide sur la page liste dans ff_cloud.py)
  - Date : recherche par regex d'un motif du type "Sep 7, 2026 4:10am"
    n'importe ou dans le texte de la page

Si les logs indiquent "date introuvable" ou "titre introuvable" de facon
repetee, c'est le signal qu'il faut inspecter le HTML reel d'une page
article et ajuster les fonctions extraire_* ci-dessous.

A lancer via GitHub Actions, declenche sur push du fichier a_recuperer.txt
(voir workflow associe), pas en cron -- inutile de tourner en continu pour
un outil utilise seulement quand on repere une news manquante a l'oeil.
"""

import os
import re
import hashlib
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator

import firebase_admin
from firebase_admin import credentials, firestore

# ---------- CONFIGURATION ----------
FICHIER_A_RECUPERER = "a_recuperer.txt"
COLLECTION = "ff_news"
GENERER_VERSION_FR = True

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",  # anglais : evite de recevoir des
    # dates/textes localises qui casseraient le parsing (voir le bug corrige
    # dans ff_cloud.py -- on ne prend pas le risque une deuxieme fois)
}

# Motif du type "Sep 7, 2026 4:10am" -- observe sur la page article a cote
# du tweet source. ATTENTION : le fuseau horaire exact affiche par
# ForexFactory sur cette page n'a pas pu etre confirme avec certitude.
# A verifier/ajuster si les dates recuperees semblent decalees de quelques
# heures par rapport a la realite.
MOTIF_DATE = re.compile(
    r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:am|pm))",
    re.IGNORECASE,
)
MOTIF_IMPACT = re.compile(r"impact/ff/(high|medium|low)", re.IGNORECASE)


def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def hash_url(url):
    # IDENTIQUE a ff_cloud.py : meme fonction = meme doc_id pour la meme URL
    # = pas de doublon possible dans Firestore, meme en cas de retraitement.
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def lire_urls(chemin):
    if not os.path.exists(chemin):
        return []
    with open(chemin, "r", encoding="utf-8") as f:
        lignes = [l.strip() for l in f.readlines()]
    return [l for l in lignes if l and not l.startswith("#")]


def reecrire_urls(chemin, urls_restantes):
    with open(chemin, "w", encoding="utf-8") as f:
        for url in urls_restantes:
            f.write(url + "\n")


def extraire_titre(soup, html_brut):
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    meta = soup.find("meta", attrs={"property": "og:title"})
    if meta and meta.get("content"):
        return meta["content"].strip()
    return None


def extraire_extrait(soup):
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        texte = meta["content"].strip()
        # La meta-description de ForexFactory se termine souvent par "..."
        # -- on la garde telle quelle, comme extrait court (meme esprit que
        # l'"extrait" scrape sur la page liste dans ff_cloud.py).
        return texte
    return "(Pas d'extrait disponible)"


def extraire_impact(html_brut):
    m = MOTIF_IMPACT.search(html_brut)
    return m.group(1).lower() if m else None


def extraire_date_publication(html_brut):
    m = MOTIF_DATE.search(html_brut)
    if not m:
        return None
    texte = m.group(1).strip()
    try:
        dt = datetime.strptime(texte, "%b %d, %Y %I:%M%p")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def traduire_texte(texte, langue_dest="fr"):
    if not texte:
        return texte
    try:
        return GoogleTranslator(source="auto", target=langue_dest).translate(texte)
    except Exception as e:
        print(f"  -> Traduction echouee, version originale gardee en attendant : {e}")
        return None


def traiter_url(db, url):
    print(f"\nTraitement : {url}")
    try:
        reponse = requests.get(url, headers=HEADERS, timeout=15)
        reponse.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  -> Erreur de recuperation de la page, on reessaiera plus tard : {e}")
        return False

    html_brut = reponse.text
    soup = BeautifulSoup(html_brut, "html.parser")

    titre = extraire_titre(soup, html_brut)
    if not titre:
        print("  -> Titre introuvable, on reessaiera plus tard (verifier le selecteur h1/og:title)")
        return False

    date_pub = extraire_date_publication(html_brut)
    if date_pub is None:
        print("  -> Date introuvable, on reessaiera plus tard (verifier le motif MOTIF_DATE)")
        return False

    impact = extraire_impact(html_brut) or "high"  # a defaut : on suppose que
    # si tu as juge utile de la recuperer a la main, elle merite d'etre visible
    # (mieux vaut "high" par defaut que la perdre silencieusement)

    extrait = extraire_extrait(soup)

    titre_fr = None
    extrait_fr = None
    if GENERER_VERSION_FR:
        titre_fr = traduire_texte(titre)
        extrait_fr = traduire_texte(extrait) if extrait else "(Pas d'extrait disponible)"

    doc_id = hash_url(url)
    maintenant = datetime.now(timezone.utc)

    doc = {
        "url": url,
        "titre": titre,
        "titre_fr": titre_fr,
        "source": "Recuperation manuelle",
        "impact": impact,
        "extrait": extrait,
        "extrait_fr": extrait_fr,
        "date_publication": date_pub,
        "date_recuperation": maintenant,
        "ignore": False,
    }

    db.collection(COLLECTION).document(doc_id).set(doc)
    print(f"  -> Ecrit dans Firestore : {titre}")
    return True


def executer():
    urls = lire_urls(FICHIER_A_RECUPERER)
    if not urls:
        print("Aucune URL a traiter dans a_recuperer.txt.")
        return

    print(f"{len(urls)} URL(s) a traiter.")
    db = init_firestore()

    urls_restantes = []
    for url in urls:
        succes = traiter_url(db, url)
        if not succes:
            urls_restantes.append(url)  # on la garde pour la prochaine fois

    reecrire_urls(FICHIER_A_RECUPERER, urls_restantes)
    print(f"\nTermine. {len(urls) - len(urls_restantes)} traitee(s), {len(urls_restantes)} restante(s) (echec, a reessayer).")


if __name__ == "__main__":
    executer()
