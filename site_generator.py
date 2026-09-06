# -*- coding: utf-8 -*-
"""
Exporte les articles de Firestore (collections "ff_news", "cb_articles" et
"il_articles") vers l'archive JSON statique du site (docs/archive/), lue
ensuite en JavaScript par le site (bouton "Charger les articles plus
anciens"). Aucune base de données côté site pour l'archive : juste des
fichiers texte, servis par GitHub Pages comme n'importe quel fichier
statique.

HISTORIQUE - CE QUI A CHANGE :
Ce module generait aussi docs/data.json (fenetre glissante de 48h), lu
par le site au chargement initial. Depuis la bascule du site vers la
lecture Firestore en temps reel (onSnapshot cote navigateur, voir
docs/index.html), plus aucun `fetch('data.json')` n'existe dans le site
-> ce fichier n'etait plus lu par personne, seulement genere pour rien a
chaque cycle. Sa generation a ete retiree (economise 3 lectures
Firestore completes sur une fenetre de 48h, a chaque appel).

docs/archive/AAAA-MM-JJ.json reste necessaire : contient TOUS les
articles (ff_news + cb_articles + il_articles) publies un jour donne
(hors documents ignore=True), pour l'historique complet accessible a la
demande. docs/archive/index.json liste les dates disponibles, du plus
recent au plus ancien.

Pour eviter de relire tout l'historique Firestore a chaque run (couteux
en lectures a mesure que le volume grossit), chaque requete d'archive est
bornee a une seule journee :
- Le jour courant est regenere (necessaire, il n'est pas encore complet).
- La veille n'est regeneree qu'une seule fois, au premier run de la
  nouvelle journee (marqueur .finalise), pour capturer les tout derniers
  articles de la veille sans avoir a la relire ensuite indefiniment.
- Les jours plus anciens ne sont plus jamais requetes : leur fichier
  archive, une fois finalise, ne change plus.

THROTTLE ANTI-REDONDANCE :
generer_json() est appelee en fin de cycle() par LES TROIS scripts
(ff_cloud.py, centralbanks_cloud.py, investinglive_cloud.py), qui
tournent tous les 5 minutes. Sans precaution, ca fait 3 lectures
completes de l'archive du jour par vague de 5 minutes, alors qu'une
seule suffirait (rien n'a change entre les 3 appels, ils se suivent a
quelques secondes/minutes d'intervalle). Un marqueur local
(docs/archive/.derniere_generation, avec un timestamp) fait qu'un seul
des 3 appels par vague fait vraiment le travail : les autres, arrivant
moins de THROTTLE_MINUTES plus tard, sont sautes sans toucher Firestore.
Peu importe LEQUEL des 3 scripts arrive en premier a chaque vague -> si
l'un des 3 tombe en panne, les 2 autres prennent quand meme le relais
normalement, pas de dependance a un script en particulier.

La sauvegarde dans Firebase/Firestore continue de fonctionner exactement
comme avant — ce module ne fait que RELIRE les données déjà écrites pour
en produire des copies exportables.

NOTE INDEX COMPOSITE : la requête combinant ignore == False + tri/filtre
sur date_publication nécessite un index composite Firestore, pour
chacune des 3 collections (deja cree pour les 3 lors de la mise en
place initiale).
"""

import os
import json
from datetime import datetime, timedelta, timezone

DOSSIER_SITE = "docs"
DOSSIER_ARCHIVE = os.path.join(DOSSIER_SITE, "archive")
FICHIER_INDEX_ARCHIVE = os.path.join(DOSSIER_ARCHIVE, "index.json")
FICHIER_THROTTLE = os.path.join(DOSSIER_ARCHIVE, ".derniere_generation")

# Ecart minimum entre deux generations reelles de l'archive. Les 3
# scripts tournent toutes les 5 minutes : un throttle de 4 minutes
# garantit au maximum 1 generation reelle par vague, quel que soit
# l'ordre d'arrivee des 3 scripts, tout en laissant une marge si un
# script demarre un peu en avance/retard par rapport aux autres.
THROTTLE_MINUTES = 4


def _recuperer_documents_periode(db, collection, debut, fin=None):
    """Lit dans Firestore les documents non ignores publies entre `debut`
    (inclus) et `fin` (exclu ; si None, pas de borne haute), tries du plus
    recent au plus ancien.

    NOTE : cette requete combine un filtre d'egalite (ignore == False) et
    un filtre d'inegalite + tri sur date_publication. Firestore exige un
    index composite pour ce type de requete (deja cree pour les 3
    collections lors de la mise en place initiale).
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

    return len(news_ff), len(articles_cb), len(articles_il)


def _throttle_actif():
    """Retourne True si l'archive a deja ete regeneree il y a moins de
    THROTTLE_MINUTES : dans ce cas, l'appelant doit sauter la generation
    (aucune lecture Firestore), un autre script s'en est deja charge."""
    if not os.path.exists(FICHIER_THROTTLE):
        return False
    try:
        with open(FICHIER_THROTTLE, "r", encoding="utf-8") as f:
            derniere = datetime.fromisoformat(f.read().strip())
    except (ValueError, OSError):
        return False
    return (datetime.now(timezone.utc) - derniere) < timedelta(minutes=THROTTLE_MINUTES)


def _marquer_generation():
    os.makedirs(DOSSIER_ARCHIVE, exist_ok=True)
    with open(FICHIER_THROTTLE, "w", encoding="utf-8") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def generer_json(db):
    """Regenere l'archive du jour courant (docs/archive/AAAA-MM-JJ.json),
    et finalise l'archive de la veille une seule fois par jour.

    Protegee par un throttle (voir _throttle_actif) : appelee par les 3
    scripts a chaque cycle, mais ne fait le travail reel qu'une fois par
    fenetre de THROTTLE_MINUTES, peu importe lequel des 3 scripts arrive
    en premier. A appeler a la fin de cycle() dans ff_cloud.py,
    centralbanks_cloud.py ET investinglive_cloud.py."""
    os.makedirs(DOSSIER_SITE, exist_ok=True)

    if _throttle_actif():
        print("Archive deja regeneree recemment par un autre script de ce cycle, on saute (throttle).")
        return

    maintenant = datetime.now(timezone.utc)

    # ---- archive du jour courant : requete bornee a aujourd'hui ----
    debut_jour = maintenant.replace(hour=0, minute=0, second=0, microsecond=0)
    date_str_jour = debut_jour.strftime("%Y-%m-%d")
    compte_jour = _generer_archive_jour(db, debut_jour, None, date_str_jour)

    # ---- finalisation de la veille, une seule fois par jour ----
    debut_hier = debut_jour - timedelta(days=1)
    date_str_hier = debut_hier.strftime("%Y-%m-%d")
    marqueur_hier = os.path.join(DOSSIER_ARCHIVE, f".{date_str_hier}.finalise")
    if not os.path.exists(marqueur_hier):
        _generer_archive_jour(db, debut_hier, debut_jour, date_str_hier)
        os.makedirs(DOSSIER_ARCHIVE, exist_ok=True)
        with open(marqueur_hier, "w", encoding="utf-8") as f:
            f.write("ok")

    _marquer_generation()

    if compte_jour:
        news_ff, articles_cb, articles_il = compte_jour
        print(f"Archive du jour regeneree : {news_ff} news ff, {articles_cb} articles cb, {articles_il} articles il")
    else:
        print("Archive du jour regeneree (aucun article non-ignore pour l'instant).")
