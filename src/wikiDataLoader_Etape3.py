import time
import requests
from typing import List, Optional

from src.wikiDataLoader import BatchProcessing, BatchWriterJSON, BatchReaderJSON, logger
from src.wikiDataLoader import EntreeHistorique, LigneProcess


class BatchProcessingCoordonnees(BatchProcessing):
    def __init__(self, runId: str, fichierInput: str, dossierSortie: str, pause: float = 0.5):
        super().__init__(runId=runId, etape=3, nbLignesBatch = 20)
        self.reader = BatchReaderJSON(fichierInput)
        self.writer = BatchWriterJSON(
            dossier_sortie = dossierSortie,
            runId = runId,
            etape=3,
            fichierSortie=fichierInput.replace("Step2", "Step3")
        )
        self.pause = pause
        self.batch = []

    def chargerEntrees(self) -> List[EntreeHistorique]:
        lignes = self.reader.loadLignes()
        print(f"[Etape 3 ✅] {len(lignes)} lignes chargées depuis {self.reader.fichierSource}")
        return lignes


    def recupererInfosWikidataBatchREST(self, liste_qids: List[str]) -> dict:
        """
        Interroge l’API REST Wikidata pour un lot de QID (max 50).
        Retourne un dictionnaire {qid: {lat, lon, p31, nbLangues}}
        """
        if not liste_qids:
            return {}

        ids = "|".join(liste_qids)
        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": ids,
            "props": "claims|sitelinks",
        }

        data = self.requeteWikiMedia("https://www.wikidata.org/w/api.php", params=params)
        if not data or "entities" not in data:
            logger.warning("[⚠️ REST] Réponse invalide")
            return {}

        resultats = {}

        for qid, entity in data["entities"].items():
            claims = entity.get("claims", {})
            coord_claim = claims.get("P625")
            type_claim = claims.get("P31")
            if not coord_claim:
                continue

            coord_data = coord_claim[0].get("mainsnak", {}).get("datavalue", {}).get("value")
            if not coord_data:
                continue

            lat = coord_data.get("latitude")
            lon = coord_data.get("longitude")

            type_id = None
            if type_claim:
                type_id = type_claim[0].get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")

            # Compte grossier des langues : via le nombre de sitelinks
            nbLangues = len(entity.get("sitelinks", {}))

            resultats[qid] = {
                "lat": lat,
                "lon": lon,
                "p31": type_id,
                "nbLangues": nbLangues,
            }

        return resultats



    def traiterBatch(self, lignes: List[EntreeHistorique]):
            qids = [ligne.qid for ligne in lignes if ligne.qid]
            infos_batch = self.recupererInfosWikidataBatchREST(qids)

            for ligne in lignes:
                infos = infos_batch.get(ligne.qid)
                if not infos:
                    continue

                ligne.lat = infos["lat"]
                ligne.lon = infos["lon"]
                ligne.p31 = infos["p31"]
                ligne.nbLangues = infos["nbLangues"]

                if not ligne.estGeolocaliseeEnFrance():
                    logger.warning(f"[🌍 Coordonnées hors France] {ligne.titre} ignoré")
                    return  # ou continue dans une boucle

                ligne.convertirLambert93()
                ligne.calculerNote()


                self.writer.ajouter(ligne)
