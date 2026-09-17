# -*- coding: utf-8 -*-
"""
snb_rbnz_cloud.py - Probabilites de taux SNB et RBNZ (centralbank.watch),
version cloud, meme schema Firestore que rateprobability_cloud.py
------------------------------------------------------------------------------
SCRIPT VOLONTAIREMENT SEPARE de rateprobability_cloud.py : fichier propre,
workflow GitHub Actions propre, aucun import ni modification du script
existant. Les deux scripts ecrivent dans la MEME collection Firestore
(rate_probabilities) mais des documents differents (snb/rbnz vs
fed/ecb/boe/boc/boj/rba) - aucun risque d'ecrasement croise.

DIFFERENCE MAJEURE avec rateprobability_cloud.py : centralbank.watch rend
ses valeurs directement dans le HTML renvoye par le serveur (verifie via
Ctrl+U, pas juste via l'outil de fetch - la meme prudence qui avait
revele que rateprobability.com necessitait Selenium a ici confirme le
contraire). Donc requests + BeautifulSoup suffisent, PAS de Selenium :
plus simple, plus rapide, aucun risque de blocage Cloudflare.

CAS PARTICULIER RBNZ : verifie au Ctrl+U que le tableau de probabilites
par reunion (.cbw-row) est VIDE cote serveur pour cette banque
("No upcoming meetings with probability data available" - correspond a
ce que la page dit elle-meme ailleurs : "live RBNZ probabilities are not
published yet"). Decision actee avec l'utilisateur : ne PAS inventer de
donnee de repli, laisser les champs de probabilite vides pour RBNZ tant
que sa vraie table n'est pas publiee par le site source. taux_actuel et
prochaine_decision_date restent disponibles et sont donc remplis.

STRUCTURE HTML EXPLOITEE (verifiee sur le vrai code source, cf. session) :
- .metric-card / .metric-label / .metric-value : taux directeur actuel
- .bank-quick-facts : premiere <strong> = date de la prochaine reunion
- .cbw-row (repete) : une ligne par reunion, avec
    .cbw-row-date        -> date de la reunion
    .cbw-move-pct         -> probabilite du mouvement "frais" a CETTE reunion
    .cbw-move-step        -> mouvement frais en pb (ex: "+1.7 bp")
    .cbw-exp-big           -> taux implique CUMULE a cette date (ex: "0.02%")
    .cbw-chip-up/flat/down -> repartition higher/same/lower (%), donnee
                              bonus qu'on n'a pas pour les 6 autres banques
"""

import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import ResourceExhausted

from site_generator import generer_json

# ---------- CONFIGURATION ----------
BANQUES = {
    "snb": ("Banque Nationale Suisse (SNB)", "https://centralbank.watch/swiss-national-bank/", "CHF"),
    "rbnz": ("Reserve Bank of New Zealand (RBNZ)", "https://centralbank.watch/reserve-bank-of-new-zealand/", "NZD"),
}

COLLECTION = "rate_probabilities"  # meme collection que rateprobability_cloud.py
NOM_SOURCE = "snb_rbnz"  # identifiant distinct dans pipeline_status

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


# ---------- INITIALISATION FIREBASE (identique aux autres scripts) ----------
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


# ---------- EXTRACTION ----------
def _nombre(texte):
    """Extrait le premier nombre (eventuellement negatif/decimal) d'un
    texte du style '6.9%', '+1.7 bp', '0.02%'. Renvoie None si rien
    d'exploitable - jamais 0 par defaut, pour distinguer une vraie valeur
    nulle d'une donnee absente."""
    if not texte:
        return None
    m = re.search(r"-?\d+(\.\d+)?", texte)
    return float(m.group()) if m else None


def extraire_taux_actuel(soup):
    for carte in soup.select(".metric-card"):
        label = carte.select_one(".metric-label")
        valeur = carte.select_one(".metric-value")
        if not label or not valeur:
            continue
        texte_label = label.get_text(strip=True).lower()
        if "policy rate" in texte_label or "cash rate" in texte_label or texte_label == "ocr":
            return valeur.get_text(strip=True)
    return None


def extraire_prochaine_date(soup):
    """La date de la prochaine reunion est dans le premier <strong> du
    paragraphe .bank-quick-facts (ex: '...scheduled for <strong>24
    September 2026</strong>...')."""
    p = soup.select_one(".bank-quick-facts")
    if not p:
        return None
    premier_strong = p.find("strong")
    return premier_strong.get_text(strip=True) if premier_strong else None


def extraire_reunions(soup):
    """Lit chaque bloc .cbw-row (une reunion). Renvoie une liste vide,
    sans erreur, si le site n'a pas encore publie ce tableau pour cette
    banque (cas RBNZ actuellement - voir note en tete de fichier)."""
    reunions = []
    for ligne in soup.select(".cbw-row"):
        date_el = ligne.select_one(".cbw-row-date")
        pct_el = ligne.select_one(".cbw-move-pct")
        if not date_el or not pct_el:
            continue

        step_el = ligne.select_one(".cbw-move-step")
        exp_el = ligne.select_one(".cbw-exp-big")

        repartition = {}
        for chip in ligne.select(".cbw-chip"):
            morceaux = chip.get_text(strip=True).split()
            if len(morceaux) >= 2:
                repartition[morceaux[0]] = morceaux[1]  # {"higher": "6.9%", "same": "93.1%", "lower": "0.0%"}

        reunions.append({
            "meeting": date_el.get_text(strip=True),
            "probabilite_mouvement": pct_el.get_text(strip=True),
            "delta_bp_reunion": step_el.get_text(strip=True) if step_el else "",
            "taux_implique_cumulatif": exp_el.get_text(strip=True) if exp_el else "",
            "higher": repartition.get("higher", ""),
            "same": repartition.get("same", ""),
            "lower": repartition.get("lower", ""),
        })
    return reunions


def _sens_dominant(reunion):
    """Determine le sens (hike/hold/cut) de la reunion : celui des 3
    pourcentages higher/same/lower qui est le plus eleve. Renvoie
    (sens, probabilite) ou (None, None) si rien d'exploitable."""
    valeurs = {
        "hike": _nombre(reunion.get("higher")),
        "hold": _nombre(reunion.get("same")),
        "cut": _nombre(reunion.get("lower")),
    }
    valeurs = {k: v for k, v in valeurs.items() if v is not None}
    if not valeurs:
        return None, None
    sens = max(valeurs, key=valeurs.get)
    return sens, valeurs[sens]


def _bps_cumules(reunion, taux_actuel_pct):
    """Convertit le taux implique cumule (ex: '0.02%') en mouvement en pb
    par rapport au taux actuel, coherent avec la convention deja utilisee
    par rateprobability_cloud.py (Δ vs Current, cumulatif)."""
    taux_implique = _nombre(reunion.get("taux_implique_cumulatif"))
    if taux_implique is None or taux_actuel_pct is None:
        return None
    return round((taux_implique - taux_actuel_pct) * 100, 1)


def extraire_banque(url):
    reponse = requests.get(url, headers=HEADERS, timeout=15)
    reponse.raise_for_status()
    soup = BeautifulSoup(reponse.text, "html.parser")

    taux_actuel_texte = extraire_taux_actuel(soup)
    taux_actuel_pct = _nombre(taux_actuel_texte)
    prochaine_date = extraire_prochaine_date(soup)
    reunions = extraire_reunions(soup)

    resultat = {
        "taux_actuel": taux_actuel_texte,
        "prochaine_decision_date": prochaine_date,
        "probabilite": None,
        "sens_mouvement": None,
        "outcome_implicite": None,
        "delta_vs_actuel_bps": None,
        "outlook_12m_bps": None,
        "tableau_meetings": [],
    }

    if reunions:
        premiere = reunions[0]
        sens, prob = _sens_dominant(premiere)
        resultat["probabilite"] = f"{prob}%" if prob is not None else None
        resultat["sens_mouvement"] = sens
        resultat["outcome_implicite"] = sens.upper() if sens else None
        resultat["delta_vs_actuel_bps"] = _bps_cumules(premiere, taux_actuel_pct)

        # Reunion la plus proche de 12 mois (ou la derniere disponible si
        # aucune n'atteint 12 mois - cas RBNZ meme si son tableau se
        # remplit un jour, tres peu de reunions listees).
        reunion_12m = None
        meilleur_ecart = None
        for r in reunions:
            d = _date_ou_infini(r["meeting"])
            if d == datetime.max:
                continue
            ecart = abs((d - datetime.now()).days - 365)
            if meilleur_ecart is None or ecart < meilleur_ecart:
                meilleur_ecart = ecart
                reunion_12m = r
        if reunion_12m is not None and meilleur_ecart is not None and meilleur_ecart <= 60:
            # Ecart de moins de 2 mois par rapport a 12 mois exactement :
            # assez proche pour etre appele "outlook 12 mois" sans induire
            # en erreur. Au-dela, on laisse vide (cas RBNZ : donnees
            # honnetes plutot qu'une approximation etiquetee a tort).
            resultat["outlook_12m_bps"] = _bps_cumules(reunion_12m, taux_actuel_pct)

        resultat["tableau_meetings"] = [
            {
                "meeting": r["meeting"],
                "taux_implique": r["taux_implique_cumulatif"],
                "probabilite": r["probabilite_mouvement"],
                "nb_hikes_cuts": "",  # non fourni par cette source (voir note methodologie)
                "delta_vs_actuel_bps": str(_bps_cumules(r, taux_actuel_pct) or ""),
                "repartition_higher": r["higher"],
                "repartition_same": r["same"],
                "repartition_lower": r["lower"],
            }
            for r in reunions
        ]

    return resultat


def _date_ou_infini(texte_date):
    """Convertit une date texte du style '24 September 2026' (format
    centralbank.watch, jour avant le mois - different du format 'Sep 16,
    2026' de rateprobability.com) en objet triable."""
    try:
        return datetime.strptime(texte_date.strip(), "%d %B %Y")
    except (ValueError, AttributeError):
        try:
            return datetime.strptime(texte_date.strip(), "%B %d, %Y")
        except (ValueError, AttributeError):
            return datetime.max


# ---------- ECRITURE FIRESTORE (meme schema que rateprobability_cloud.py) ----------
def enregistrer_point_historique(db, code, doc, maintenant):
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
    d = _date_ou_infini(texte_date)
    if d == datetime.max:
        return re.sub(r"[^a-zA-Z0-9_-]", "-", (texte_date or "inconnue").strip()) or "inconnue"
    return d.strftime("%Y-%m-%d")


def enregistrer_historique_reunions(db, code, meetings, maintenant):
    if not meetings:
        return  # rien a faire pour RBNZ tant que sa table est vide
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
    db = init_firestore()
    maintenant = datetime.now(timezone.utc)
    banques_ecrites = 0

    for code, (nom, url, ccy) in BANQUES.items():
        try:
            donnees = extraire_banque(url)

            doc = dict(donnees)
            doc.update({
                "banque": nom,
                "code_banque": code,
                "devise": ccy,
                "source_url": url,
                "date_recuperation": maintenant,
            })

            db.collection(COLLECTION).document(code).set(doc, merge=True)
            enregistrer_point_historique(db, code, doc, maintenant)
            enregistrer_historique_reunions(db, code, donnees["tableau_meetings"], maintenant)
            banques_ecrites += 1

            if donnees["tableau_meetings"]:
                print(f"OK : {nom} -> taux {donnees['taux_actuel']}, "
                      f"prochaine decision {donnees['prochaine_decision_date']} "
                      f"({donnees['probabilite']} {donnees['outcome_implicite']}), "
                      f"{len(donnees['tableau_meetings'])} reunion(s)")
            else:
                print(f"OK (partiel) : {nom} -> taux {donnees['taux_actuel']}, "
                      f"prochaine decision {donnees['prochaine_decision_date']} "
                      f"- tableau de probabilites pas encore publie par le site source")

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

        time.sleep(1)

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
