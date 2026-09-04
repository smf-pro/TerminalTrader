# -*- coding: utf-8 -*-
"""
Exporte les données de Firestore (collections "ff_news" et "cb_articles")
vers un fichier JSON statique : docs/data.json

Ce fichier est ensuite lu directement en JavaScript par le site (HTML/CSS/JS)
via fetch("data.json"). Aucune base de données côté site : juste un fichier
texte, servi par GitHub Pages comme n'importe quel autre fichier statique.

La sauvegarde dans Firebase/Firestore continue de fonctionner exactement
comme avant — ce module ne fait que RELIRE les données déjà écrites pour
en produire une copie exportable.

Appelé à la fin du cycle() de ff_cloud.py ET de extract2_cloud.py.
"""

import os
import json
from datetime import datetime, timedelta, timezone

# Fenêtre affichée sur le site (un peu plus large que la fenêtre de
# collecte des scripts, pour ne pas vider le site trop vite entre deux runs)
FENETRE_HEURES_SITE = 48

DOSSIER_SITE = "docs"
FICHIER_JSON = os.path.join(DOSSIER_SITE, "data.json")


def _recuperer_documents(db, collection, limite_heures):
    """Lit dans Firestore les documents non ignorés et publiés dans la
    fenêtre demandée, triés du plus récent au plus ancien.

    NOTE : cette requête combine un filtre d'égalité (ignore == False) et
    un filtre d'inégalité + tri sur date_publication. Firestore exige un
    index composite pour ce type de requête. Au premier lancement, les
    logs GitHub Actions afficheront une erreur contenant un lien direct
    pour créer cet index automatiquement (à cliquer une seule fois).
    """
    debut = datetime.now(timezone.utc) - timedelta(hours=limite_heures)
    docs = (
        db.collection(collection)
        .where("ignore", "==", False)
        .where("date_publication", ">=", debut)
        .order_by("date_publication", direction="DESCENDING")
        .stream()
    )
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


def generer_json(db):
    """Régénère docs/data.json à partir des données actuelles de Firestore.
    À appeler à la fin de cycle() dans ff_cloud.py ET extract2_cloud.py."""
    os.makedirs(DOSSIER_SITE, exist_ok=True)

    news_ff = [_serialiser(d) for d in _recuperer_documents(db, "ff_news", FENETRE_HEURES_SITE)]
    articles_cb = [_serialiser(d) for d in _recuperer_documents(db, "cb_articles", FENETRE_HEURES_SITE)]

    contenu = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ff_news": news_ff,
        "cb_articles": articles_cb,
    }

    with open(FICHIER_JSON, "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=2)

    print(f"JSON genere : {FICHIER_JSON} ({len(news_ff)} news, {len(articles_cb)} articles)")
