# -*- coding: utf-8 -*-
"""
data-eur.py - Script d'extraction de l'HISTORIQUE des indicateurs
economiques EUR - ForexFactory Calendar (version cloud, patron TerminalTrader)
----------------------------------------------------------------------
Reprend la logique de scraping du script local `ffus.py` (tableau
"History" de chaque page calendrier ForexFactory, ex :
https://www.forexfactory.com/calendar/12-ez-main-refinancing-rate) mais l'adapte au
patron cloud des autres scripts du projet (ff_cloud.py, centralbanks_cloud.py,
investinglive_cloud.py) :

- Pas de boucle infinie ni d'ecriture Excel : un seul passage (single-pass),
  c'est GitHub Actions (cron horaire) qui relance le script periodiquement.
- Ecriture dans Firestore, collection "ff_indicator_history", au lieu d'un
  classeur .xlsx local.
- PAS de doc_ref.get() : la comparaison "cette ligne a-t-elle deja ete vue,
  a-t-elle change ?" se fait via un cache LOCAL dedie
  (cache/ffhistory.json), jamais via une lecture Firestore.

Difference importante avec cache_dedup.py (utilise par les 3 autres
scripts) : ce module partage ne stocke qu'une date "vu le" par hash, ce
qui suffit pour du "deja vu / pas deja vu" sur des articles qui
n'apparaissent qu'une fois. Ici, une meme ligne d'historique (meme
indicateur + meme date de publication) peut revenir a chaque cycle ET
voir sa valeur "Previous" REVISEE apres coup (revision economique).
Il faut donc memoriser les VALEURS elles-memes (actual/forecast/previous/
position), pas juste une date de premiere vue, pour detecter un
changement. D'ou un cache dedie (fonctions ci-dessous), stocke dans le
meme dossier cache/ que les autres (aucune purge par anciennete : le
tableau "History" de ForexFactory n'affiche de toute facon qu'un nombre
limite de lignes recentes par indicateur, le fichier reste petit).

Hypothese a verifier a l'usage : le tableau "History" liste les
publications de la plus recente en haut a la plus ancienne en bas. Le
champ "position" (0 = premiere ligne du tableau lors du scrape) sert a
trier l'affichage cote site sans avoir a parser la date texte (souvent
incomplete, ex "Aug 13" sans annee). Si l'ordre reel s'avere inverse,
il suffira d'inverser le sens de tri cote docs/index.html - aucune
migration Firestore necessaire.
"""

import os
import re
import time
import json
import hashlib
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

# ---------- CONFIGURATION ----------

# Liens ForexFactory a suivre (repris de ffus.py). Ajoute / supprime /
# commente une ligne pour modifier la liste des indicateurs suivis.
# Liens ForexFactory a suivre pour l'EUR. Ajoute / supprime / commente
# une ligne pour modifier la liste des indicateurs suivis.
LIENS = [
    # Taux
    "https://www.forexfactory.com/calendar/12-ez-main-refinancing-rate",
    # Inflation
    "https://www.forexfactory.com/calendar/168-ez-cpi-flash-estimate-yy",
    "https://www.forexfactory.com/calendar/166-ez-final-cpi-yy",
    "https://www.forexfactory.com/calendar/593-ez-core-cpi-flash-estimate-yy",
    "https://www.forexfactory.com/calendar/167-ez-final-core-cpi-yy",
    # Emploi
    "https://www.forexfactory.com/calendar/59-ez-unemployment-rate",
    "https://www.forexfactory.com/calendar/808-ez-flash-employment-change-qq",
    # Croissance
    "https://www.forexfactory.com/calendar/41-ez-flash-gdp-qq",
    "https://www.forexfactory.com/calendar/638-ez-prelim-flash-gdp-qq",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

COLLECTION = "ff_indicator_history"
NOM_SOURCE = "data-eur"  # identifiant unique de ce script dans pipeline_status
DOSSIER_CACHE = "cache"

# Acronymes a garder en majuscules dans le nom lisible d'un indicateur
ACRONYMES = {
    "us", "cpi", "ppi", "pce", "gdp", "ism", "pmi", "jolts", "adp",
    "yy", "mm", "qq", "fomc", "ecb", "boe", "boj",
}


# ---------- INITIALISATION FIREBASE (identique aux autres scripts) ----------
def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def enregistrer_statut_pipeline(db, statut, liens_vus=0, articles_nouveaux=0, erreur=None):
    """Battement de coeur dans 'pipeline_status', a CHAQUE cycle, meme
    sans aucune ligne nouvelle ou revisee (permet au site de distinguer
    'rien de neuf' de 'le script est en panne')."""
    doc = {
        "derniere_execution": firestore.SERVER_TIMESTAMP,
        "liens_vus": liens_vus,
        "articles_nouveaux": articles_nouveaux,
        "statut": statut,
    }
    if erreur:
        doc["derniere_erreur"] = str(erreur)[:300]
    db.collection("pipeline_status").document(NOM_SOURCE).set(doc, merge=True)


def hash_ligne(indicateur_slug, cle_ligne):
    """ID de document Firestore = hash de (indicateur + cle unique de la
    ligne). cle_ligne est l'identifiant "detail=" extrait du lien quand il
    existe (toujours le cas normalement), la date texte en repli sinon."""
    brut = f"{indicateur_slug}|{cle_ligne}"
    return hashlib.sha256(brut.encode("utf-8")).hexdigest()


# ---------- CACHE LOCAL DEDIE (valeurs, pas juste une date) ----------
def _chemin_cache_historique():
    return os.path.join(DOSSIER_CACHE, f"{NOM_SOURCE}.json")


def charger_cache_historique():
    """Retourne {doc_id: {"actual":..., "forecast":..., "previous":...,
    "position": int}} tel que vu au dernier cycle. Dict vide si le fichier
    n'existe pas encore ou est corrompu."""
    chemin = _chemin_cache_historique()
    if not os.path.exists(chemin):
        return {}
    try:
        with open(chemin, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def sauvegarder_cache_historique(cache):
    """Ecrit le cache sur disque. Pas de purge par anciennete ici : le
    tableau History de ForexFactory ne montre qu'un nombre borne de
    lignes recentes par indicateur, le fichier reste petit naturellement."""
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(_chemin_cache_historique(), "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ---------- EXTRACTION (identique a ffus.py) ----------
def trouver_table_history(soup):
    """Parcourt tous les <table> de la page et renvoie celui dont la ligne
    d'en-tete contient "Actual", "Forecast" ET "Previous" (insensible a la
    casse) - robuste aux changements de classes CSS de ForexFactory."""
    for table in soup.find_all("table"):
        entete = table.find("tr")
        if not entete:
            continue
        texte_entete = entete.get_text(" ", strip=True).lower()
        if "actual" in texte_entete and "forecast" in texte_entete and "previous" in texte_entete:
            return table
    return None


def nettoyer_cellule(cellule):
    return cellule.get_text(strip=True) if cellule else ""


def extraire_historique(url):
    """Renvoie une liste de dicts {"date_texte", "cle_ligne", "actual",
    "forecast", "previous", "previous_revise", "url_detail"}, dans l'ordre
    d'apparition du tableau (position 0 = premiere ligne).

    "cle_ligne" = identifiant unique de la publication (extrait du lien,
    ex: "153362" pour ".../calendar?day=aug11.2026#detail=153362"). C'est
    CE champ qui sert de cle d'unicite, PAS "date_texte" seul : ForexFactory
    peut afficher deux publications differentes sous la meme date texte
    (ex: rattrapage d'une publication sautee la semaine precedente, deux
    lignes "Aug 11, 2026" avec un detail= different chacune). Utiliser la
    date seule comme cle fait collisionner ces deux lignes distinctes et
    declenche une fausse "revision".

    "previous_revise" = True si ForexFactory affiche l'icone officielle de
    revision a cote de la valeur "Previous" (une <img> dans la cellule).
    C'est plus fiable que de deduire une revision en comparant deux
    scrapes successifs : ForexFactory marque lui-meme la revision, meme
    des le tout premier scrape d'une ligne."""
    reponse = requests.get(url, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    table = trouver_table_history(soup)
    if table is None:
        return []

    lignes = table.find_all("tr")
    resultats = []

    for ligne in lignes[1:]:  # on saute la ligne d'en-tete
        cellules = ligne.find_all(["td", "th"])
        if len(cellules) < 4:
            continue

        lien_date = cellules[0].find("a")
        date_texte = nettoyer_cellule(lien_date) if lien_date else nettoyer_cellule(cellules[0])
        url_detail = lien_date.get("href") if lien_date else ""
        if url_detail and url_detail.startswith("/"):
            url_detail = "https://www.forexfactory.com" + url_detail

        m_detail = re.search(r"detail=(\d+)", url_detail)
        cle_ligne = m_detail.group(1) if m_detail else date_texte

        actual = nettoyer_cellule(cellules[1])
        forecast = nettoyer_cellule(cellules[2])
        cellule_previous = cellules[3]
        previous = nettoyer_cellule(cellule_previous)
        previous_revise = cellule_previous.find("img") is not None

        if not date_texte:
            continue

        resultats.append({
            "date_texte": date_texte,
            "cle_ligne": cle_ligne,
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "previous_revise": previous_revise,
            "url_detail": url_detail,
        })

    return resultats


def slug_depuis_url(url):
    m = re.search(r"/calendar/(\d+-)?([a-z0-9-]+)$", url)
    return m.group(2) if m else re.sub(r"[^a-z0-9-]", "-", url.lower())


def nom_lisible_depuis_slug(slug):
    mots = slug.split("-")
    mots_formes = [m.upper() if m.lower() in ACRONYMES else m.capitalize() for m in mots]
    return " ".join(mots_formes)


# ---------- PROGRAMME PRINCIPAL (single-pass) ----------
def cycle():
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)

    cache = charger_cache_historique()

    liens_vus = 0
    lignes_ecrites = 0  # nouvelles lignes OU lignes revisees/repositionnees

    for url in LIENS:
        liens_vus += 1
        slug = slug_depuis_url(url)
        nom = nom_lisible_depuis_slug(slug)

        try:
            historique = extraire_historique(url)
        except requests.exceptions.RequestException as e:
            print(f"Erreur reseau sur {nom} ({url}) : {e}")
            continue

        if not historique:
            print(f"Aucun tableau History trouve pour {nom} - ignore ce cycle.")
            continue

        for position, ligne in enumerate(historique):
            doc_id = hash_ligne(slug, ligne["cle_ligne"])
            snapshot_actuel = {
                "actual": ligne["actual"],
                "forecast": ligne["forecast"],
                "previous": ligne["previous"],
                "previous_revise": ligne["previous_revise"],
                "position": position,
            }
            snapshot_cache = cache.get(doc_id)

            if snapshot_cache is None:
                # Nouvelle ligne jamais vue.
                try:
                    db.collection(COLLECTION).document(doc_id).set({
                        "indicateur_slug": slug,
                        "indicateur_nom": nom,
                        "url_indicateur": url,
                        "date_publication_texte": ligne["date_texte"],
                        "actual": ligne["actual"],
                        "forecast": ligne["forecast"],
                        "previous": ligne["previous"],
                        "previous_revise": ligne["previous_revise"],
                        "url_detail": ligne["url_detail"],
                        "position": position,
                        "date_ajout": maintenant,
                        "date_recuperation": maintenant,
                        "derniere_revision": maintenant if ligne["previous_revise"] else None,
                    })
                    cache[doc_id] = snapshot_actuel
                    lignes_ecrites += 1
                    if ligne["previous_revise"]:
                        print(f"Revision (marqueur ForexFactory) : {nom} ({ligne['date_texte']})")
                except ResourceExhausted as e:
                    print(f"Quota Firestore depasse (ecriture), arret du cycle : {e}")
                    sauvegarder_cache_historique(cache)
                    enregistrer_statut_pipeline(
                        db, statut="erreur", liens_vus=liens_vus,
                        articles_nouveaux=lignes_ecrites,
                        erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
                    )
                    return

            elif snapshot_cache != snapshot_actuel:
                # La revision "vient d'apparaitre" si le marqueur officiel
                # n'etait pas present au cycle precedent et l'est maintenant.
                nouvelle_revision = ligne["previous_revise"] and not snapshot_cache.get("previous_revise")
                maj = {
                    "actual": ligne["actual"],
                    "forecast": ligne["forecast"],
                    "previous": ligne["previous"],
                    "previous_revise": ligne["previous_revise"],
                    "position": position,
                    "date_recuperation": maintenant,
                }
                if nouvelle_revision:
                    maj["derniere_revision"] = maintenant
                    maj["valeur_avant_revision"] = {
                        "actual": snapshot_cache.get("actual"),
                        "forecast": snapshot_cache.get("forecast"),
                        "previous": snapshot_cache.get("previous"),
                    }
                    print(f"Revision (marqueur ForexFactory) : {nom} ({ligne['date_texte']})")

                try:
                    db.collection(COLLECTION).document(doc_id).set(maj, merge=True)
                    cache[doc_id] = snapshot_actuel
                    lignes_ecrites += 1
                except ResourceExhausted as e:
                    print(f"Quota Firestore depasse (ecriture), arret du cycle : {e}")
                    sauvegarder_cache_historique(cache)
                    enregistrer_statut_pipeline(
                        db, statut="erreur", liens_vus=liens_vus,
                        articles_nouveaux=lignes_ecrites,
                        erreur="Quota Firestore depasse (ResourceExhausted), cycle interrompu",
                    )
                    return
            # sinon : ligne identique au dernier cycle, rien a faire.

        time.sleep(1)  # pause polie entre deux pages d'indicateur

    sauvegarder_cache_historique(cache)

    print(f"\nTermine. {lignes_ecrites} ligne(s) ecrite(s)/mise(s) a jour dans Firestore sur {liens_vus} indicateur(s).")

    enregistrer_statut_pipeline(db, statut="ok", liens_vus=liens_vus, articles_nouveaux=lignes_ecrites)


if __name__ == "__main__":
    cycle()
