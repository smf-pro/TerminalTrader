# -*- coding: utf-8 -*-
"""
Exporte les données de Firestore (collections "ff_news", "cb_articles" et
"il_articles") vers des fichiers JSON statiques, lus ensuite en JavaScript
par le site (HTML/CSS/JS) via fetch(). Aucune base de données côté site :
juste des fichiers texte, servis par GitHub Pages comme n'importe quel
fichier statique.

Deux niveaux d'export, pour concilier "site rapide au chargement" et
"aucune donnee jamais perdue" :

1. docs/data.json
   Fenetre recente (FENETRE_HEURES_SITE), regeneree a chaque run.
   C'est ce que le site charge en premier, au demarrage.

2. docs/archive/AAAA-MM-JJ.json
   Un fichier par jour, contenant TOUS les articles (ff_news + cb_articles +
   il_articles) publies ce jour-la (hors documents ignore=True). Aucune limite de duree :
   l'historique complet reste disponible, juste charge a la demande cote
   site (bouton "Charger les articles plus anciens").
   docs/archive/index.json liste les dates disponibles, du plus recent au
   plus ancien, pour que le site sache quels fichiers existent.

Pour eviter de relire tout l'historique Firestore a chaque run (couteux en
lectures a mesure que le volume grossit), chaque requete d'archive est
bornee a une seule journee :
- Le jour courant est regenere a chaque run (necessaire, il n'est pas
  encore complet).
- La veille n'est regeneree qu'une seule fois, au premier run de la
  nouvelle journee (marqueur .finalise), pour capturer les tout derniers
  articles de la veille sans avoir a la relire ensuite indefiniment.
- Les jours plus anciens ne sont plus jamais requetes : leur fichier
  archive, une fois finalise, ne change plus.

La sauvegarde dans Firebase/Firestore continue de fonctionner exactement
comme avant — ce module ne fait que RELIRE les données déjà écrites pour
en produire des copies exportables.

Appelé à la fin du cycle() de ff_cloud.py, centralbanks_cloud.py ET
investinglive_cloud.py.

NOTE INDEX COMPOSITE : comme pour ff_news et cb_articles, la requête
combinant ignore == False + tri/filtre sur date_publication nécessite un
index composite Firestore sur il_articles. Il n'existe pas encore lors du
premier déploiement de investinglive_cloud.py : la première exécution
plantera avec une erreur Firestore donnant un lien direct pour le créer
en un clic — c'est normal, il suffit de suivre ce lien une fois.
"""

import os
import json
from datetime import datetime, timedelta, timezone

# Fenetre affichee au chargement initial du site (plus large que la fenetre
# de collecte des scripts, pour ne pas vider le site trop vite entre deux
# runs). L'historique complet, lui, reste toujours accessible via l'archive.
FENETRE_HEURES_SITE = 48

DOSSIER_SITE = "docs"
FICHIER_JSON = os.path.join(DOSSIER_SITE, "data.json")
DOSSIER_ARCHIVE = os.path.join(DOSSIER_SITE, "archive")
FICHIER_INDEX_ARCHIVE = os.path.join(DOSSIER_ARCHIVE, "index.json")


def _recuperer_documents_periode(db, collection, debut, fin=None):
    """Lit dans Firestore les documents non ignores publies entre `debut`
    (inclus) et `fin` (exclu ; si None, pas de borne haute), tries du plus
    recent au plus ancien.

    NOTE : cette requete combine un filtre d'egalite (ignore == False) et
    un filtre d'inegalite + tri sur date_publication. Firestore exige un
    index composite pour ce type de requete (deja cree pour ff_news et
    cb_articles lors de la mise en place initiale).
    """
    requete = (
        db.collection(collection)
        .where("ignore", "==", False)
        .where("date_publication", ">=", debut)
    )
    if fin is not None:
        requete = requete.where("date_publication", "<", fin)
    docs = requete.order_by("date_publication", direction="DESCENDING").stream()
    return [d.to_dict() for d in docs]


def _serialiser(doc):
    """Convertit les champs non-JSON (dates Firestore) en texte ISO 8601,
    pour que json.dump() ne plante pas dessus."""
    resultat = {}
    for cle, valeur in doc.items():
        if isinstance(valeur, datetime):
            resultat[cle] = valeur.astimezone(timezone.utc).isoformat()
        else:
            resultat[cle] = valeur
    return resultat


def _ecrire_json(chemin, contenu):
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=2)


def _lire_index_archive():
    if not os.path.exists(FICHIER_INDEX_ARCHIVE):
        return []
    try:
        with open(FICHIER_INDEX_ARCHIVE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _mettre_a_jour_index_archive(date_str):
    """Ajoute date_str a archive/index.json si absente, trie du plus
    recent au plus ancien."""
    dates = _lire_index_archive()
    if date_str not in dates:
        dates.append(date_str)
    dates.sort(reverse=True)
    _ecrire_json(FICHIER_INDEX_ARCHIVE, dates)


def _generer_archive_jour(db, debut_jour, fin_jour, date_str):
    """Regenere le fichier d'archive d'une seule journee (requete bornee,
    jamais tout l'historique)."""
    news_ff = [_serialiser(d) for d in _recuperer_documents_periode(db, "ff_news", debut_jour, fin_jour)]
    articles_cb = [_serialiser(d) for d in _recuperer_documents_periode(db, "cb_articles", debut_jour, fin_jour)]
    articles_il = [_serialiser(d) for d in _recuperer_documents_periode(db, "il_articles", debut_jour, fin_jour)]

    if not news_ff and not articles_cb and not articles_il:
        return

    contenu = {
        "date": date_str,
        "ff_news": news_ff,
        "cb_articles": articles_cb,
        "il_articles": articles_il,
    }
    chemin = os.path.join(DOSSIER_ARCHIVE, f"{date_str}.json")
    _ecrire_json(chemin, contenu)
    _mettre_a_jour_index_archive(date_str)


def generer_json(db):
    """Regenere docs/data.json (fenetre recente) + l'archive du jour
    courant, et finalise l'archive de la veille une seule fois par jour.
    À appeler à la fin de cycle() dans ff_cloud.py ET centralbanks_cloud.py."""
    os.makedirs(DOSSIER_SITE, exist_ok=True)
    maintenant = datetime.now(timezone.utc)

    # ---- 1) data.json : fenetre recente, comme avant ----
    debut_fenetre = maintenant - timedelta(hours=FENETRE_HEURES_SITE)
    news_ff = [_serialiser(d) for d in _recuperer_documents_periode(db, "ff_news", debut_fenetre)]
    articles_cb = [_serialiser(d) for d in _recuperer_documents_periode(db, "cb_articles", debut_fenetre)]
    articles_il = [_serialiser(d) for d in _recuperer_documents_periode(db, "il_articles", debut_fenetre)]

    contenu = {
        "generated_at": maintenant.isoformat(),
        "ff_news": news_ff,
        "cb_articles": articles_cb,
        "il_articles": articles_il,
    }
    _ecrire_json(FICHIER_JSON, contenu)

    # ---- 2) archive du jour courant : requete bornee a aujourd'hui ----
    debut_jour = maintenant.replace(hour=0, minute=0, second=0, microsecond=0)
    date_str_jour = debut_jour.strftime("%Y-%m-%d")
    _generer_archive_jour(db, debut_jour, None, date_str_jour)

    # ---- 3) finalisation de la veille, une seule fois par jour ----
    debut_hier = debut_jour - timedelta(days=1)
    date_str_hier = debut_hier.strftime("%Y-%m-%d")
    marqueur_hier = os.path.join(DOSSIER_ARCHIVE, f".{date_str_hier}.finalise")
    if not os.path.exists(marqueur_hier):
        _generer_archive_jour(db, debut_hier, debut_jour, date_str_hier)
        os.makedirs(DOSSIER_ARCHIVE, exist_ok=True)
        with open(marqueur_hier, "w", encoding="utf-8") as f:
            f.write("ok")

    print(f"JSON genere : {FICHIER_JSON} ({len(news_ff)} news, {len(articles_cb)} articles cb, {len(articles_il)} articles il)")
