# -*- coding: utf-8 -*-
"""
nettoyer_doublons_indicateurs.py - Script PONCTUEL (a lancer une seule
fois, pas en cron) pour supprimer les documents orphelins laisses dans
Firestore par la toute premiere version de data-usd.py, avant la
correction du bug de collision de cle.

Contexte :
La toute premiere version de data-usd.py ecrivait un champ "revise"
(booleen) et utilisait un ID de document base uniquement sur la date de
publication. Une fois le bug corrige, le nouveau schema utilise un champ
"previous_revise" et un ID de document base sur l'identifiant unique
"detail=" du lien ForexFactory. Les anciens documents (ID different) ne
sont donc jamais ecrases par les runs suivants et restent orphelins dans
Firestore, provoquant un doublon visible sur le site (2 lignes pour la
meme date, memes valeurs).

Ce script :
1. Parcourt TOUTE la collection ff_indicator_history.
2. Identifie les documents de l'ANCIEN schema : ceux qui ont un champ
   "revise" (regarde sa simple presence, peu importe sa valeur - les
   documents du nouveau schema n'ont jamais ce champ, ils ont
   "previous_revise" a la place).
3. Les supprime.

Usage : lancer une seule fois (workflow_dispatch manuel), puis on peut
supprimer ce script et son workflow - il n'a plus d'utilite une fois le
nettoyage fait, tant que le bug ne se reproduit pas ailleurs.
"""

import os

import firebase_admin
from firebase_admin import credentials, firestore

COLLECTION = "ff_indicator_history"


def init_firestore():
    if not firebase_admin._apps:
        chemin_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
        cred = credentials.Certificate(chemin_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def nettoyer():
    db = init_firestore()
    docs = db.collection(COLLECTION).stream()

    total_vus = 0
    total_supprimes = 0
    lot = db.batch()
    compteur_lot = 0

    for doc in docs:
        total_vus += 1
        data = doc.to_dict()
        if "revise" in data:
            print(f"Suppression (ancien schema) : {data.get('indicateur_slug')} - "
                  f"{data.get('date_publication_texte')} [{doc.id}]")
            lot.delete(doc.reference)
            compteur_lot += 1
            total_supprimes += 1
            # Firestore limite un batch a 500 operations.
            if compteur_lot >= 400:
                lot.commit()
                lot = db.batch()
                compteur_lot = 0

    if compteur_lot > 0:
        lot.commit()

    print(f"\nTermine. {total_vus} document(s) inspecte(s), "
          f"{total_supprimes} document(s) orphelin(s) supprime(s).")


if __name__ == "__main__":
    nettoyer()
