# -*- coding: utf-8 -*-
"""
Script d'extraction d'articles - investingLive Central Banks (version cloud)
------------------------------------------------------------------------------
Version adaptée pour tourner sur GitHub Actions (cron toutes les 5 min).

- Pas de boucle infinie : un seul passage (single-pass), c'est GitHub Actions
  qui se charge de relancer le script périodiquement.
- Pas de fichiers .txt locaux ni deja_vus.txt : tout est écrit et vérifié
  dans Firestore (collection "cb_articles"), pour que la donnée survive
  entre deux exécutions et soit consultable par la page web en temps réel.
- Dédup : LOCALE via cache_dedup.py (cache/centralbanks.json), jamais de
  lecture Firestore (doc_ref.get()) pour vérifier l'existence d'un article.
- Après l'écriture dans Firestore, régénère aussi docs/archive/ via
  site_generator.generer_json().
- A CHAQUE cycle, même sans nouvel article, écrit un document dans la
  collection "pipeline_status" (battement de coeur), pour que le site
  distingue "rien de neuf à publier" de "le script est en panne".

MODIF (sept. 2026) :
- L'article n'est plus traduit en français : on ne stocke plus que la
  langue d'origine du site source (titre, contenu). Champs "titre_fr" et
  "contenu_fr" retirés.
- Nouveau champ "tags" : liste des tags/thèmes affichés sur la page
  source (ex: ["RBA", "AUD"]), utile pour du filtrage côté site plus tard.
- Extraction du contenu réécrite : l'ancienne version ne prenait que les
  <p> qui étaient des ENFANTS DIRECTS d'un même conteneur, ce qui ratait
  les paragraphes du corps de l'article quand ils étaient nichés dans des
  sous-<div> (cas fréquent sur ce site). La nouvelle version parcourt le
  DOM dans l'ordre de lecture à partir du <h1>, et garde tout paragraphe/
  puce réel en ignorant les blocs majoritairement composés de liens (nav,
  bloc "Tags", articles liés, CTA "Add as a preferred source"...) ainsi
  que tout ce qui suit un marqueur de fin de contenu éditorial ("Must
  Read", avertissements légaux, etc.).
"""

import os
import time
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from site_generator import generer_json
from cache_dedup import charger_cache, sauvegarder_cache, marquer_traite

# ---------- CONFIGURATION ----------
URL_LISTE = "https://investinglive.com/CentralBanks/"
FENETRE_HEURES = 24  # on ne garde que les articles publiés dans les dernières 24h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

COLLECTION = "cb_articles"
NOM_SOURCE = "centralbanks"  # identifiant unique de ce script dans pipeline_status

# Seuil au-dessus duquel un bloc est considéré comme "surtout des liens"
# (nav, listes d'articles liés, bloc Tags, CTA...) et donc ignoré.
SEUIL_DENSITE_LIEN = 0.6

# Marqueurs de titre/texte qui signalent qu'on est sorti du contenu
# éditorial (tout ce qui suit est ignoré).
MARQUEURS_ARRET = [
    "must read", "featured videos", "best in", "related articles",
    "high risk warning", "advisory warning", "disclaimer",
    "subscribe to our", "follow us", "manage cookies",
]

# Libellés courts à ignorer telles quels s'ils apparaissent seuls.
LIBELLES_A_IGNORER = {
    "tags", "share", "print", "advertisement - continue reading below",
}


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


def densite_lien(tag):
    """Proportion du texte d'un element qui provient de liens <a>.
    Pres de 1.0 -> l'element est surtout une liste de liens (nav, tags,
    articles lies...), pas du contenu editorial."""
    texte_total = tag.get_text(strip=True)
    if not texte_total:
        return 1.0
    texte_liens = "".join(a.get_text(strip=True) for a in tag.find_all("a"))
    return len(texte_liens) / max(len(texte_total), 1)


def est_marqueur_arret(texte):
    texte_lower = texte.strip().lower()
    return any(motif in texte_lower for motif in MARQUEURS_ARRET)


def extraire_contenu_complet(titre_tag):
    """Parcourt le DOM dans l'ordre de lecture a partir du <h1> du titre,
    et collecte le texte de chaque paragraphe/puce reel de l'article.

    Remplace l'ancienne approche par "meilleur conteneur" (recursive=
    False sur les <p>), qui ratait les paragraphes du corps de l'article
    quand ils etaient niches dans des sous-<div> (cas frequent ici).

    Est ignore :
    - tout bloc majoritairement compose de liens (nav, bloc "Tags",
      articles lies, CTA "Add as a preferred source"...),
    - tout ce qui suit un marqueur de fin de contenu editorial (heading
      "Must Read", avertissements legaux, etc.),
    - les legendes courtes finissant par ":" (ex: "Earlier weight on
      AUD:"), qui introduisent generalement une liste de liens deja
      filtree juste apres.
    """
    if titre_tag is None:
        return ""

    morceaux = []
    for element in titre_tag.find_all_next(["h1", "h2", "h3", "h4", "p", "li"]):
        texte = element.get_text(strip=True)
        if not texte:
            continue

        if est_marqueur_arret(texte):
            break

        if texte.lower() in LIBELLES_A_IGNORER:
            continue

        if element.name in ("h1", "h2", "h3", "h4"):
            morceaux.append(texte)
            continue

        if densite_lien(element) > SEUIL_DENSITE_LIEN:
            continue

        if texte.endswith(":") and len(texte) < 80:
            continue

        morceaux.append(texte)

    return "\n\n".join(morceaux)


def extraire_tags(soup):
    """Recupere les tags/themes de l'article (liens de la forme
    /Tag/nom-du-tag/ sur ce site), dans l'ordre d'apparition, sans
    doublon."""
    tags = []
    vus = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/tag/" in href.lower():
            texte = a.get_text(strip=True)
            if texte and texte.lower() not in vus:
                vus.add(texte.lower())
                tags.append(texte)
    return tags


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
    contenu = extraire_contenu_complet(titre_tag)
    tags = extraire_tags(soup)

    return titre, date_pub, contenu, tags


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    debut_fenetre = maintenant - timedelta(hours=FENETRE_HEURES)

    try:
        liens = recuperer_liens_articles()
    except Exception as e:
        print(f"Erreur recuperation des liens : {e}")
        enregistrer_statut_pipeline(db, statut="erreur", erreur=e)
        return

    print(f"{len(liens)} lien(s) trouve(s) sur la page liste.")

    # Cache local de deduplication (remplace les lectures Firestore
    # doc_ref.get() qui epuisaient le quota gratuit - voir cache_dedup.py).
    cache = charger_cache(NOM_SOURCE)

    articles_ecrits = 0
    for url in liens:
        doc_id = hash_url(url)

        # Dédup : verification LOCALE (fichier cache/centralbanks.json),
        # aucune lecture Firestore. Remplace l'ancien doc_ref.get().
        if doc_id in cache:
            continue

        try:
            titre, date_pub, contenu, tags = extraire_article(url)

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

            doc_ref = db.collection(COLLECTION).document(doc_id)
            doc_ref.set({
                "url": url,
                "titre": titre,
                "contenu": contenu if contenu else "(Contenu non trouve)",
                "tags": tags,
                "date_publication": date_pub,
                "date_recuperation": maintenant,
                "ignore": False,
            })
            marquer_traite(cache, doc_id)
            articles_ecrits += 1
            print(f"Ecrit dans Firestore : {titre} (tags: {tags})")

        except ResourceExhausted as e:
            # Quota d'ECRITURE Firestore depasse (rare). On arrete la
            # boucle : les tentatives suivantes echoueraient pareil.
            print(f"Quota Firestore depasse, arret du cycle en cours (traite {articles_ecrits} article(s) avant l'arret) : {e}")
            sauvegarder_cache(NOM_SOURCE, cache)
            enregistrer_statut_pipeline(
                db, statut="erreur",
                liens_vus=len(liens),
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

    # On régénère docs/archive/ avec les données à jour des collections,
    # lu ensuite par votre site.
    try:
        generer_json(db)
    except ResourceExhausted as e:
        print(f"Quota Firestore depasse pendant generer_json() : {e}")
        enregistrer_statut_pipeline(
            db, statut="erreur",
            liens_vus=len(liens),
            articles_nouveaux=articles_ecrits,
            erreur="Quota Firestore depasse pendant la generation du JSON",
        )
        return

    # Battement de coeur : ce cycle s'est termine normalement, meme si
    # articles_ecrits vaut 0 (rien de neuf a publier).
    enregistrer_statut_pipeline(db, statut="ok", liens_vus=len(liens), articles_nouveaux=articles_ecrits)


if __name__ == "__main__":
    cycle()
