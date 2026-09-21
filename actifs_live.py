# -*- coding: utf-8 -*-
"""
Voie rapide des prix — dernier prix de chaque actif, toutes les ~minutes
-----------------------------------------------------------------------
Complète actifs_cloud.py (historique horaire, cron horaire) : ce script-ci ne
touche PAS à l'historique. Il télécharge les dernières bougies d'UNE MINUTE
(yfinance, interval="1m") et écrit un seul petit document Firestore :

  asset_latest/live = {
      actifs: { SLUG: {prix, ts} },   # ts = horodatage UNIX (secondes) de la bougie 1m
      derniere_maj, statut, actifs_recuperes, actifs_totaux, duree_s, [derniere_erreur]
  }

Le site fusionne ce prix dans la bougie en cours (clôture = dernier prix,
plus haut / plus bas étendus) ; c'est ce qui fait "arriver le prix vite".

Ce que ce script NE peut PAS accélérer : le délai propre à Yahoo. Forex et
cryptos sont quasi en direct ; certaines places (indices européens, futures)
sont retardées de 10 à 15 minutes côté Yahoo, quelle que soit notre cadence.
Le champ "ts" dit toujours l'âge réel du prix : le site s'en sert.

Coût : 1 écriture par exécution (~1 440/jour à la minute), zéro lecture, aucun
commit git (donc aucun conflit avec les autres scrapers).

Robustesse :
  - set(merge=True) sur la map "actifs" : si un lot échoue, les autres actifs
    gardent leur dernier prix connu, rien n'est effacé.
  - 2e passe (période 5 jours) pour les actifs sans bougie aujourd'hui
    (week-end, jour férié, marché pas encore ouvert) : ils gardent ainsi leur
    dernier prix connu avec son vrai horodatage.
  - La liste d'actifs vient de actifs_cloud.py (une seule source de vérité).
"""

import time
from datetime import datetime, timezone

import yfinance as yf

from firebase_admin import firestore
from google.api_core.exceptions import ResourceExhausted

import actifs_cloud as base   # ACTIFS, init_firestore, extraire_barres...

DOC_LIVE = "live"
TAILLE_LOT_TICKERS = 34        # 102 actifs -> 3 requêtes
PAUSE_ENTRE_LOTS_SECONDES = 0.5
NB_TENTATIVES = 2


def telecharger_1m(tickers, periode):
    """Bougies 1 minute d'un lot de tickers. Retourne le DataFrame, ou None."""
    for tentative in range(1, NB_TENTATIVES + 1):
        try:
            df = yf.download(
                tickers, period=periode, interval="1m",
                auto_adjust=False, progress=False, group_by="ticker", threads=False,
            )
            if df is not None and not df.empty:
                return df
            print(f"⚠️  Lot vide ({periode}, tentative {tentative}/{NB_TENTATIVES}).")
        except Exception as e:
            print(f"⚠️  Échec du téléchargement ({periode}, tentative {tentative}/{NB_TENTATIVES}) : {e}")
        if tentative < NB_TENTATIVES:
            time.sleep(2)
    return None


def dernier_prix_par_actif(actifs, periode):
    """Retourne ({slug: (ts_unix, prix)}, [slugs sans donnée]) pour cette période."""
    trouves = {}
    manquants = []
    lots = [actifs[i:i + TAILLE_LOT_TICKERS] for i in range(0, len(actifs), TAILLE_LOT_TICKERS)]
    for numero, lot in enumerate(lots, start=1):
        tickers = [a["ticker"] for a in lot]
        df = telecharger_1m(tickers, periode)
        for a in lot:
            barres = base.extraire_barres(df, a["ticker"], len(tickers)) if df is not None else []
            if barres:
                ts, _o, _h, _l, c = barres[-1]
                trouves[a["slug"]] = (ts, c)
            else:
                manquants.append(a["slug"])
        if numero < len(lots):
            time.sleep(PAUSE_ENTRE_LOTS_SECONDES)
    return trouves, manquants


def cycle():
    debut = time.time()
    db = base.init_firestore()
    actifs = base.ACTIFS
    par_slug = {a["slug"]: a for a in actifs}

    # 1re passe : séance du jour (léger). 2e passe : 5 jours, seulement pour les manquants.
    prix, manquants = dernier_prix_par_actif(actifs, "1d")
    if manquants:
        print(f"{len(manquants)} actif(s) sans bougie aujourd'hui, 2e passe sur 5 jours...")
        reste = [par_slug[s] for s in manquants]
        prix2, manquants = dernier_prix_par_actif(reste, "5d")
        prix.update(prix2)

    total = len(actifs)
    statut = "erreur" if not prix else ("ok" if len(prix) == total else "ok_partiel")
    doc = {
        "derniere_maj": firestore.SERVER_TIMESTAMP,
        "statut": statut,
        "actifs_recuperes": len(prix),
        "actifs_totaux": total,
        "duree_s": round(time.time() - debut, 1),
        # merge=True conserve les anciens champs : DELETE_FIELD efface l'ancien message d'erreur.
        "derniere_erreur": ("Aucun prix récupéré (Yahoo injoignable ou limité ?)" if not prix
                            else firestore.DELETE_FIELD),
    }
    if prix:
        doc["actifs"] = {slug: {"prix": c, "ts": ts} for slug, (ts, c) in prix.items()}

    try:
        db.collection(base.COLLECTION_DERNIER).document(DOC_LIVE).set(doc, merge=True)
    except ResourceExhausted as e:
        print(f"Quota Firestore dépassé : {e}")
        return

    ages = [time.time() - ts for ts, _c in prix.values()]
    print(f"Terminé en {doc['duree_s']} s : statut={statut}, {len(prix)}/{total} prix"
          + (f", prix le plus récent il y a {int(min(ages))} s" if ages else "")
          + (f", sans donnée : {manquants}" if manquants else ""))


if __name__ == "__main__":
    cycle()
