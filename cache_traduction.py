# -*- coding: utf-8 -*-
"""
Cache local de PROGRESSION pour translate_news.py.

Contrairement a cache_dedup.py (qui retient un ENSEMBLE de hash "deja
vus"), ici on retient un CURSEUR par collection : la date_publication du
dernier document deja traite (traduit ou constate deja traduit). A
chaque run, translate_news.py ne relit dans Firestore que les documents
publies APRES ce curseur, trie par date_publication croissante - meme
principe que le curseur incremental de site_generator.py
(_generer_archive_jour_courant), applique ici a la traduction plutot
qu'a la generation de l'archive.

Avantage : un seul et meme mecanisme couvre a la fois le RATTRAPAGE de
tout l'historique existant (le curseur part d'une date volontairement
ancienne) ET le traitement des nouvelles news au fil de l'eau (une fois
le retard rattrape, le curseur reste proche de "maintenant" et chaque
run ne traite que les tout derniers articles).

Fichier : cache/traduction_progression.json, ex:
{
  "ff_news": "2026-01-01T00:00:00+00:00",
  "cb_articles": "2026-01-01T00:00:00+00:00"
}
"""

import os
import json

DOSSIER_CACHE = "cache"
FICHIER = os.path.join(DOSSIER_CACHE, "traduction_progression.json")

# Curseur de depart si aucun cache n'existe encore pour une collection :
# assez ancien pour couvrir tout l'historique du projet (le pipeline
# TerminalTrader a demarre courant 2026). A ajuster si besoin de
# retraduire depuis une date differente.
CURSEUR_INITIAL = "2026-01-01T00:00:00+00:00"


def charger_progression():
    """Charge le fichier de progression. Retourne un dict vide si le
    fichier n'existe pas encore (premier run) ou est corrompu."""
    if not os.path.exists(FICHIER):
        return {}
    try:
        with open(FICHIER, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def obtenir_curseur(progression, collection):
    """Retourne le curseur (chaine ISO 8601) pour une collection, ou le
    curseur initial si cette collection n'a pas encore ete traitee."""
    return progression.get(collection, CURSEUR_INITIAL)


def avancer_curseur(progression, collection, nouvelle_date_iso):
    """Met a jour en memoire le curseur d'une collection. N'ecrit rien
    sur disque - appeler sauvegarder_progression() pour persister."""
    progression[collection] = nouvelle_date_iso


def sauvegarder_progression(progression):
    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(FICHIER, "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False, indent=2)
