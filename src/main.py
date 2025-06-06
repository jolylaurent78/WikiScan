import argparse
import os
import glob
import time
import re
from typing import List, Optional

import sys
sys.ps1 = ">>> "
sys.ps2 = "... "

from src.wikiDataLoader import logger
from src.wikiDataLoader_Etape1 import BatchProcessingTitresExtraction
from src.wikiDataLoader_Etape2 import BatchProcessingQidDepuisWikipedia
from src.wikiDataLoader_Etape3 import BatchProcessingCoordonnees
from src.wikiDataLoader_Etape4 import BatchProcessingResumeDescription
from src.wikiDataLoader_Etape5 import BatchProcessingInsertionBD


REPERTOIRES_PAR_ETAPE = {
    1: "data/step1_backlinks/",
    2: "data/step2_qid/",
    3: "data/step3_coord/",
    4: "data/step4_semantics/",
    5: "data/step5_types/"
}


# ───────────────────────────────────────
# Fonctions de traitement (par étape ou par logique)
# ───────────────────────────────────────

def listener(run_id: str, step: int, pause: float):
    dossier_source = REPERTOIRES_PAR_ETAPE[step - 1]
    print(f"[Etape {step} 👂 Listener actif] Étape {step} – Surveillance du répertoire : {dossier_source}")

    nom_stop = f"{run_id}_STOP"

    while True:
        fichiers_tous = sorted(os.listdir(dossier_source))  # Inclut STOP et .json
        fichiers_json = sorted(
            [f for f in fichiers_tous if f.endswith(".json")],
            key=lambda f: int(re.findall(r"batch_(\d+)", os.path.basename(f))[0])
        )

        for fichier in fichiers_json:
            chemin_complet = os.path.join(dossier_source, fichier)
            print(f"[Etape {step} 📥 ] Nouveau fichier détecté] {chemin_complet}")

            if step == 2:
                traiterQidDepuisWikipedia(runId=run_id, fichierInput=chemin_complet, pause=pause)
            elif step == 3:
                traiterCoordonnees(runId=run_id, fichierInput=chemin_complet, pause=pause)
            elif step == 4:
                traiterResumeDescription(runId=run_id, fichierInput=chemin_complet, pause=pause)
            elif step == 5:
                insertionBase(runId=run_id, fichierInput=chemin_complet)

            # 📦 Archivage des fichiers JSON
            dossier_done = os.path.join(dossier_source, "Done")
            os.makedirs(dossier_done, exist_ok=True)
            destination = os.path.join(dossier_done, os.path.basename(fichier))
            if os.path.exists(destination):
                os.remove(destination)
            os.rename(chemin_complet, destination)
            print(f"[Etape {step} 📦 Archivé] {chemin_complet} → {destination}")


        # 🛑 Gestion standard des autres étapes
        if nom_stop in fichiers_tous:

            print(f"[Etape {step} ✅] Tous les fichiers traités. Fichier STOP détecté : {nom_stop}")

            # 📦 Déplacement vers l’étape suivante si applicable
            if step < len(REPERTOIRES_PAR_ETAPE):
                dossier_suivant = REPERTOIRES_PAR_ETAPE[step]
                os.makedirs(dossier_suivant, exist_ok=True)
                source_stop = os.path.join(dossier_source, nom_stop)
                dest_stop = os.path.join(dossier_suivant, nom_stop)

                if os.path.exists(dest_stop):
                    os.remove(dest_stop)
                os.rename(source_stop, dest_stop)

                print(f"[Etape {step} ➡️] Fichier STOP déplacé vers {dossier_suivant}")

            break  # ✅ Fin du listener

        time.sleep(5)



# ───────────────────────────────────────
# Fonctions de traitement (par étape ou par logique)
# ───────────────────────────────────────
def lister_titres(runId: str):
    print(f"[Étape 0] Lister titres Run: {runId}")
    processor = BatchProcessingTitresExtraction(
        runId=runId,
        dossierSortie="dummy"
    )
    processor.afficherTitres()

def traiter_extraction_titres(runId: str, pause: float = 0.1, max_lignes: Optional[int] = None):
    print(f"[Étape 1] Extraction par backlink – Run: {runId} | max={max_lignes}")
    processor = BatchProcessingTitresExtraction(
        runId=runId,
        dossierSortie=REPERTOIRES_PAR_ETAPE[1],
        pause=pause,
        max_lignes=max_lignes
    )
    processor.executer()


def traiterQidDepuisWikipedia(runId: str, fichierInput: str, pause: float):
    print(f"[Étape 2] Extraction des QID – Run: {runId} | Input file: {fichierInput}")
    batch = BatchProcessingQidDepuisWikipedia(
        runId=runId,
        dossierSortie=REPERTOIRES_PAR_ETAPE[2],
        fichierInput=fichierInput,
        pause=pause)
    batch.executer()


def traiterCoordonnees(runId: str, fichierInput: str, pause: float = 0.1):
    processor = BatchProcessingCoordonnees(
        runId=runId,
        fichierInput=fichierInput,
        dossierSortie=REPERTOIRES_PAR_ETAPE[3],
        pause=pause
    )
    processor.executer()
    print(f"[Étape 3 ✅] Traitement terminé pour : {fichierInput}")


def traiterResumeDescription(runId: str, fichierInput: str, pause: float = 0.1):
    processor = BatchProcessingResumeDescription(
        runId=runId,
        fichierInput=fichierInput,
        dossierSortie=REPERTOIRES_PAR_ETAPE[4],
        pause=pause
    )
    processor.executer()
    print(f"[Etape 4 ✅] Traitement terminé pour : {fichierInput}")


def insertionBase(runId: str, fichierInput: str):
    batch = BatchProcessingInsertionBD(
        runId = runId,
        fichierInput = fichierInput,
        db = "../shared-db/WikiCarto.db")
    batch.executer()



# ───────────────────────────────────────
# 5. Main logique
# ───────────────────────────────────────
def main(runId:str, step:int, pause:int = 0.1, maxLignes:int = None):

    # 🔁 Scan automatique du répertoire (listener actif)
    if step == 0:
        lister_titres(runId=runId)
# 🔁 Scan automatique du répertoire (listener actif)
    elif step == 1:
        traiter_extraction_titres(
            runId=runId,
            pause=pause,
            max_lignes=maxLignes
        )

    elif step in [2,3,4,5]:
        listener(run_id=runId, step=step, pause=pause)

    else:
        print(f"[ERREUR] Étape {args.step} non encore implémentée.")

# ───────────────────────────────────────
# 6. Entrée du programme
# ───────────────────────────────────────
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Pipeline base historique géolocalisée")
    parser.add_argument("--runId", required=True, help="Identifiant du run")
    parser.add_argument("--step", type=int, required=True, choices=range(0, 6))
    parser.add_argument("--pause", type=float, default=0.1, help="Pause entre requêtes en secondes (anti-timeout)")
    parser.add_argument("--maxLignes", type=int, default=None, help="Nombre maximum de lignes à traiter (debug/test uniquement)")
    args = parser.parse_args()
    main(args.runId, args.step, args.pause, args.maxLignes)

