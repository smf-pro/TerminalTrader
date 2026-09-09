# -*- coding: utf-8 -*-
"""
Cache de deduplication LOCAL pour le calendrier economique.

Pourquoi ce module est SEPARE de cache_dedup.py :
cache_dedup.py part du principe qu'un lien, une fois traite, ne doit plus
JAMAIS etre retraite (un article de news ne change pas apres publication).
Un evenement du calendrier economique, lui, EST modifie dans le temps :
- d'abord publie avec seulement `forecast`/`previous` renseignes,
- puis, a l'heure reelle de publication de la statistique, le champ
  `actual` se remplit.
Si on utilisait cache_dedup.py tel quel, le premier passage marquerait
l'evenement comme "deja vu" et on ne verrait JAMAIS la valeur reelle
publiee plus tard. Ce module garde donc un SNAPSHOT des valeurs par
evenement plutot qu'un simple "vu/pas vu", et ne dit d'ecrire dans
Firestore que si quelque chose a reellement change depuis le dernier
cycle (economise le quota d'ecriture, comme cache_dedup.py le fait pour
la dedup classique).

IMPORTANT (choix valide avec l'utilisateur) : on ne supprime JAMAIS les
evenements de Firestore, meme une fois passes depuis longtemps. Ce
module ne fait que purger le fichier cache LOCAL (pour qu'il ne grossisse
pas indefiniment) - la purge locale ne supprime rien cote Firestore, elle
sert seulement a ne plus suivre localement des evenements qui de toute
facon ne reapparaitront plus dans le flux "thisweek".
"""

import os
import json
from datetime import datetime, timedelta, timezone

DOSSIER_CACHE = "cache"
DUREE_RETENTION_JOURS = 10  # un peu plus qu'une semaine, marge de securite


def _chemin_cache(nom_source):
    return os.path.join(DOSSIER_CACHE, f"{nom_source}.json")


def charger_cache(nom_source):
    """Retourne un dict {event_id: {date_evenement, forecast, previous,
    actual}}. Dict vide si le fichier n'existe pas encore ou est
    corrompu (au pire on reecrit dans Firestore des evenements deja a
    jour au prochain cycle, sans consequence grave car l'ID de document
    est deterministe)."""
    chemin = _chemin_cache(nom_source)
    if not os.path.exists(chemin):
        return {}
    try:
        with open(chemin, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def a_change(cache, event_id, forecast, previous, actual):
    """True si l'evenement est nouveau OU si une des 3 valeurs a change
    depuis le dernier cycle connu localement (typiquement : `actual` qui
    passe de vide a rempli apres l'heure de publication reelle)."""
    entree = cache.get(event_id)
    if entree is None:
        return True
    return (
        entree.get("forecast") != forecast
        or entree.get("previous") != previous
        or entree.get("actual") != actual
    )


def marquer_traite(cache, event_id, date_evenement_brute, forecast, previous, actual):
    """Met a jour le snapshot EN MEMOIRE pour cet evenement (rien n'est
    ecrit sur disque ici - sauvegarder_cache() s'en charge une seule fois
    en fin de cycle)."""
    cache[event_id] = {
        "date_evenement": date_evenement_brute,
        "forecast": forecast,
        "previous": previous,
        "actual": actual,
    }


def sauvegarder_cache(nom_source, cache):
    """Purge les entrees dont l'evenement date de plus de
    DUREE_RETENTION_JOURS puis ecrit le fichier sur disque. Ne touche
    JAMAIS Firestore : purge locale uniquement (voir docstring du
    module)."""
    seuil = datetime.now(timezone.utc) - timedelta(days=DUREE_RETENTION_JOURS)
    cache_purge = {}
    for event_id, entree in cache.items():
        date_brute = entree.get("date_evenement")
        try:
            date_evenement = datetime.fromisoformat(date_brute)
        except (ValueError, TypeError):
            # Date illisible : on garde l'entree par prudence plutot que
            # de perdre le suivi d'un evenement valide.
            cache_purge[event_id] = entree
            continue
        if date_evenement >= seuil:
            cache_purge[event_id] = entree

    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(_chemin_cache(nom_source), "w", encoding="utf-8") as f:
        json.dump(cache_purge, f, ensure_ascii=False, indent=2)
