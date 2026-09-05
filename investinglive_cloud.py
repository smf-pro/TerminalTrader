# -*- coding: utf-8 -*-
"""
Script d'extraction d'articles - investingLive (version cloud, multi-sources)
------------------------------------------------------------------------------
Version adaptée pour tourner sur GitHub Actions (cron toutes les 5 min).
Calquée sur centralbanks_cloud.py, avec une différence majeure :

- Contrairement à centralbanks_cloud.py qui ne scrape qu'UNE seule page
  liste (/CentralBanks/), ce script scrape PLUSIEURS pages liste
  InvestingLive (des pages /Tag/xxx/ + la page /forex/), définies dans
  SOURCES ci-dessous. Chaque page liste peut contenir des liens vers des
  articles de catégories différentes (news, centralbanks, commodities,
  stocks, technical-analysis, education...), donc on ne peut plus filtrer
  les liens sur un motif fixe comme "/central-banks/" : on utilise à la
  place un filtre générique (voir recuperer_liens_articles).

- Un même article peut apparaître sur plusieurs pages /Tag/xxx/ (ex : un
  article tagué à la fois "usd" et "cad"). On collecte donc TOUS les liens
  de TOUTES les sources dans un seul set avant de les traiter, pour ne
  jamais traduire/écrire le même article deux fois dans la même exécution.

- Pas de boucle infinie : un seul passage (single-pass), c'est GitHub
  Actions qui se charge de relancer le script périodiquement.
- Dédup entre exécutions : l'ID du document Firestore = hash SHA256 de
  l'URL. Avant de traiter un article, on vérifie s'il existe déjà.
- Après l'écriture dans Firestore, régénère docs/data.json (et
  docs/archive/) via site_generator.generer_json().
- NOUVEAU : à CHAQUE cycle, même sans nouvel article, écrit un document
  dans la collection "pipeline_status" (un battement de coeur). Ça permet
  au site de distinguer "rien de neuf à publier" de "le script est en
  panne", en affichant la dernière fois que le script a réellement tourné.
"""

import os
import time
import hashlib
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from site_generator import generer_json
from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- CONFIGURATION ----------
# Liste des pages "liste d'articles" InvestingLive à scraper. On peut en
# ajouter/retirer librement : chaque page est traitée avec la même logique
# générique de détection de liens d'articles (voir recuperer_liens_articles).
SOURCES = [
    {"nom": "market-overview", "url": "https://investinglive.com/Tag/market-overview/"},
    {"nom": "ukraine", "url": "https://investinglive.com/Tag/ukraine/"},
    {"nom": "politics", "url": "https://investinglive.com/Tag/politics/"},
    {"nom": "forex", "url": "https://investinglive.com/forex/"},
    {"nom": "cad", "url": "https://investinglive.com/Tag/cad/"},
    {"nom": "mxn", "url": "https://investinglive.com/Tag/mxn/"},
    {"nom": "krw", "url": "https://investinglive.com/Tag/krw/"},
    {"nom": "bonds", "url": "https://investinglive.com/Tag/bonds/"},
    {"nom": "oil", "url": "https://investinglive.com/Tag/oil/"},
    {"nom": "usd", "url": "https://investinglive.com/Tag/usd/"},
    {"nom": "eur", "url": "https://investinglive.com/Tag/eur/"},
    {"nom": "gbp", "url": "https://investinglive.com/Tag/gbp/"},
    {"nom": "chf", "url": "https://investinglive.com/Tag/chf/"},
    {"nom": "jpy", "url": "https://investinglive.com/Tag/jpy/"},
    {"nom": "aud", "url": "https://investinglive.com/Tag/aud/"},
    {"nom": "nzd", "url": "https://investinglive.com/Tag/nzd/"},
]

DOMAINE = "investinglive.com"

# Premier segment de chemin à exclure : ce ne sont jamais des articles,
# mais des pages de navigation, de tag, d'auteur, légales, etc.
SEGMENTS_EXCLUS = {
    "tag", "author", "directory", "live-feed", "premium", "about",
    "contact-us", "signup", "terms-of-use", "cookies", "privacy",
    "forexbrokers", "economiccalendar", "livecharts", "livequotes",
    "rss", "page", "education-center",
}

FENETRE_HEURES = 24  # on ne garde que les articles publiés dans les dernières 24h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

GENERER_VERSION_FR = True
LIMITE_CARACTERES_TRADUCTION = 4500
COLLECTION = "il_articles"
NOM_SOURCE = "investinglive"  # identifiant unique de ce script dans pipeline_status


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


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Ecrit un battement de coeur dans 'pipeline_status', a CHAQUE cycle,
    meme quand aucun nouvel article n'est trouve. C'est ce qui permet au
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


# ---------- SCRAPING ----------
def recuperer_liens_articles(url_liste):
    """Récupère les liens d'articles sur une page liste InvestingLive.

    Contrairement à centralbanks_cloud.py (qui filtre sur le motif fixe
    "/central-banks/"), ce filtre est générique : un lien est considéré
    comme un article s'il pointe vers investinglive.com avec EXACTEMENT
    2 segments de chemin (ex: /news/mon-article/, /centralbanks/xxx/,
    /commodities/xxx/), et que le premier segment n'est pas une page de
    navigation connue (Tag, author, directory...). Ça permet de gérer
    n'importe quelle page liste (/Tag/xxx/, /forex/, etc.) sans logique
    dédiée par source.
    """
    reponse = requests.get(url_liste, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    liens = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]

        if href.startswith("/"):
            href = "https://" + DOMAINE + href

        parsed = urlparse(href)
        if DOMAINE not in parsed.netloc.lower():
            continue

        segments = [s for s in parsed.path.strip("/").split("/") if s]
        if len(segments) != 2:
            continue

        categorie, slug = segments
        if categorie.lower() in SEGMENTS_EXCLUS or not slug:
            continue

        lien_normalise = f"https://{DOMAINE}/{categorie}/{slug}/"
        liens.add(lien_normalise)

    return liens


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

    # On collecte les liens de TOUTES les sources dans un seul set avant
    # de traiter quoi que ce soit, pour ne jamais traiter deux fois le
    # même article s'il apparaît sur plusieurs pages /Tag/xxx/.
    # Chaque source a son propre try/except : si une source echoue (site
    # bloque, timeout...), on continue avec les autres au lieu de tout
    # annuler. On garde le compte des sources en erreur pour le statut.
    tous_les_liens = set()
    sources_en_erreur = 0
    for source in SOURCES:
        try:
            liens_source = recuperer_liens_articles(source["url"])
            print(f"{source['nom']} : {len(liens_source)} lien(s) trouve(s).")
            tous_les_liens |= liens_source
        except Exception as e:
            print(f"Erreur en listant la source {source['nom']} ({source['url']}) : {e}")
            sources_en_erreur += 1
        time.sleep(1)

    print(f"\n{len(tous_les_liens)} lien(s) unique(s) au total sur {len(SOURCES)} source(s).")

    # Si TOUTES les sources ont echoue, c'est un vrai probleme (site
    # source injoignable, changement de structure...) : on le signale
    # comme une erreur plutot que comme un cycle "ok" avec 0 lien.
    if sources_en_erreur == len(SOURCES):
        enregistrer_statut_pipeline(
            db, statut="erreur",
            erreur=f"Les {len(SOURCES)} sources ont echoue"
        )
        return

    # Cache local de deduplication (remplace les lectures Firestore
    # doc_ref.get() qui epuisaient le quota gratuit - voir cache_dedup.py).
    cache = charger_cache(NOM_SOURCE)

    articles_ecrits = 0
    for url in sorted(tous_les_liens):
        doc_id = hash_url(url)

        # Dédup : verification LOCALE (fichier cache/investinglive.json),
        # aucune lecture Firestore. Remplace l'ancien doc_ref.get().
        if doc_id in cache:
            continue

        try:
            titre, date_pub, contenu = extraire_article(url)

            if page_erreur(titre, contenu):
                print(f"Page d'erreur detectee, ignore : {url}")
                marquer_traite(cache, doc_id)
                continue

            if date_pub is None:
                print(f"Date introuvable, ignore : {titre}")
                # On marque quand meme comme traite pour ne pas re-tenter en boucle
                marquer_traite(cache, doc_id)
                continue

            if date_pub < debut_fenetre:
                marquer_traite(cache, doc_id)
                continue

            titre_fr = None
            contenu_fr = None
            if GENERER_VERSION_FR:
                titre_fr = traduire_texte(titre)
                contenu_fr = traduire_texte(contenu) if contenu else "(Contenu non trouve)"

            # Catégorie déduite de l'URL (ex: "news", "centralbanks",
            # "commodities", "forex"...) — utile plus tard si on veut
            # sous-diviser la colonne INVESTINGLIVE par type de contenu.
            categorie = urlparse(url).path.strip("/").split("/")[0].lower()

            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "url": url,
                "categorie": categorie,
                "titre": titre,
                "titre_fr": titre_fr,
                "contenu": contenu if contenu else "(Contenu non trouve)",
                "contenu_fr": contenu_fr,
                "date_publication": date_pub,
                "date_recuperation": maintenant,
                "ignore": False,
            })
            marquer_traite(cache, doc_id)
            articles_ecrits += 1
            print(f"Ecrit dans Firestore : {titre}")

        except ResourceExhausted as e:
            # Quota d'ECRITURE Firestore depasse (rare, mais possible si
            # beaucoup de nouveaux articles ce cycle). On arrete la
            # boucle : les tentatives suivantes echoueraient pareil.
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {articles_ecrits} article(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(tous_les_liens),
                articles_nouveaux=articles_ecrits,
                erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
            )
            return
        except Exception as e:
            print(f"Erreur sur {url} : {e}")

        time.sleep(1)

    # On sauvegarde le cache local a jour (nouveaux hash vus ce cycle),
    # pour que le prochain run n'ait pas besoin de retraiter ces liens.
    sauvegarder_cache(NOM_SOURCE, cache)

    print(f"\nTermine. {articles_ecrits} nouvel(aux) article(s) ecrit(s) dans Firestore.")

    # On régénère docs/data.json avec les données à jour de toutes les
    # collections (ff_news + cb_articles + il_articles), lu ensuite par le site.
    try:
        generer_json(db)
    except ResourceExhausted as e:
        print(f"Quota Firestore depasse pendant generer_json() : {e}")
        enregistrer_statut_pipeline(
            db, statut="erreur",
            liens_vus=len(tous_les_liens),
            articles_nouveaux=articles_ecrits,
            erreur="Quota Firestore depasse pendant la generation du JSON",
        )
        return

    # Battement de coeur : ce cycle s'est termine normalement, meme si
    # articles_ecrits vaut 0 (rien de neuf a publier). Si certaines
    # sources (mais pas toutes) ont echoue, on le note quand meme "ok"
    # mais avec un statut partiel dans le message d'erreur, a titre indicatif.
    statut = "ok" if sources_en_erreur == 0 else "ok_partiel"
    enregistrer_statut_pipeline(
        db, statut=statut,
        liens_vus=len(tous_les_liens),
        articles_nouveaux=articles_ecrits,
        erreur=f"{sources_en_erreur} source(s) en erreur ce cycle" if sources_en_erreur else None,
    )


if __name__ == "__main__":
    cycle()
