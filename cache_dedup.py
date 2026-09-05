# -*- coding: utf-8 -*-
"""
Cache de deduplication LOCAL, en remplacement des lectures Firestore
(doc_ref.get()) pour verifier si un article a deja ete traite.

Pourquoi ce module existe :
Avant, chaque script (ff_cloud.py, centralbanks_cloud.py,
investinglive_cloud.py) faisait UNE lecture Firestore par lien trouve sur
les pages sources, a CHAQUE cycle, juste pour savoir si l'article avait
deja ete traite (ex: 225 lectures/cycle pour investinglive_cloud.py,
toutes les 5 minutes). Ca epuisait le quota gratuit Firestore (50 000
lectures/jour, plan Spark) en quelques heures, faisant planter les
scripts (google.api_core.exceptions.ResourceExhausted).

Principe :
Chaque script garde un petit fichier JSON versionne dans le depot
(cache/{nom_source}.json : cache/forexfactory.json, cache/centralbanks.json,
cache/investinglive.json), qui liste les hash d'URL deja vus (que
l'article ait fini par etre ecrit dans Firestore normalement, ou ignore
- peu importe, dans les deux cas il ne doit plus etre retraite/re-fetch).

Ce fichier est commit par le workflow GitHub Actions a chaque run, comme
docs/ (voir `git add cache/` a ajouter dans les .yml). Au demarrage du
cycle, chaque script le charge en memoire (gratuit, fichier local) et
l'utilise a la place de doc_ref.get(). Un fichier par script evite tout
conflit de fusion puisque les 3 scripts tournent en parallele mais
n'ecrivent jamais dans le meme fichier cache.

Purge automatique :
Les entrees plus vieilles que DUREE_RETENTION_JOURS sont supprimees a
chaque sauvegarde, pour que le fichier ne grossisse pas indefiniment. Les
pages liste des sources ne remontent de toute facon que les articles
recents : un hash vieux de plusieurs jours ne sera plus jamais revu, pas
la peine de le garder.
"""

import os
import json
from datetime import datetime, timedelta, timezone

DOSSIER_CACHE = "cache"
DUREE_RETENTION_JOURS = 7


def _chemin_cache(nom_source):
    return os.path.join(DOSSIER_CACHE, f"{nom_source}.json")


def charger_cache(nom_source):
    """Retourne un dict {hash_url: date_iso_ajout} des articles deja vus
    par ce script. Dict vide si le fichier n'existe pas encore (tout
    premier run) ou est corrompu (on repart d'un cache vide plutot que de
    planter ; au pire on retraite quelques articles deja connus, sans
    consequence grave car Firestore fait toujours foi en dernier ressort
    via l'ID de document deterministe = hash de l'URL)."""
    chemin = _chemin_cache(nom_source)
    if not os.path.exists(chemin):
        return {}
    try:
        with open(chemin, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def sauvegarder_cache(nom_source, cache):
    """Purge les entrees plus vieilles que DUREE_RETENTION_JOURS puis
    ecrit le fichier sur disque. A appeler en fin de cycle()."""
    seuil = datetime.now(timezone.utc) - timedelta(days=DUREE_RETENTION_JOURS)
    cache_purge = {}
    for doc_id, date_iso in cache.items():
        try:
            date_ajout = datetime.fromisoformat(date_iso)
        except (ValueError, TypeError):
            continue  # entree corrompue, on la laisse tomber silencieusement
        if date_ajout >= seuil:
            cache_purge[doc_id] = date_iso

    os.makedirs(DOSSIER_CACHE, exist_ok=True)
    with open(_chemin_cache(nom_source), "w", encoding="utf-8") as f:
        json.dump(cache_purge, f, ensure_ascii=False, indent=2)


def marquer_traite(cache, doc_id):
    """Ajoute un hash au cache EN MEMOIRE (rien n'est ecrit sur disque
    ici - sauvegarder_cache() s'en charge une seule fois en fin de
    cycle, pour eviter une ecriture disque par article)."""
    cache[doc_id] = datetime.now(timezone.utc).isoformat()
