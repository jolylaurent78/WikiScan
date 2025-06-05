import time
import requests
from typing import List, Optional

from src.wikiDataLoader import BatchProcessing, BatchWriterJSON, BatchReaderJSON, logger
from src.wikiDataLoader import EntreeHistorique, LigneProcess


class BatchProcessingQidDepuisWikipedia(BatchProcessing):
    def __init__(self, runId: str, fichierInput: str, dossierSortie: str, pause: float = 0.5):
        super().__init__(runId=runId, etape=2, nbLignesBatch = 20)

        self.reader = BatchReaderJSON(fichierInput)
        self.writer = BatchWriterJSON(
            dossier_sortie = dossierSortie,
            runId = runId,
            fichierSortie=fichierInput.replace("Step1", "Step2")
        )
        self.pause = pause

    def chargerEntrees(self) -> List[EntreeHistorique]:
        lignes = self.reader.loadLignes()
        print(f"✅ {len(lignes)} lignes chargées depuis {self.reader.fichierSource}")
        return lignes

    def traiterBatch(self, lignes: List[EntreeHistorique]):
        titres = [ligne.titre for ligne in lignes if ligne.titre]
        qids_par_titre = self.recupererQidDepuisWikipedia(titres)

        for ligne in lignes:
            qid = qids_par_titre.get(ligne.titre)
            if not qid:
                logger.warning(f"[❌ QID manquant] {ligne.titre}")
                continue
            ligne.qid = qid
            self.writer.ajouter(ligne)

    def recupererQidDepuisWikipedia(self, titres: List[str]) -> dict:
        """
        Envoie une requête batch à l’API Wikipedia pour obtenir les QID des titres.
        Retourne un dictionnaire {titre: qid}
        """
        resultats = {}
        if not titres:
            return resultats

        titres_concat = "|".join(titres)
        url = "https://fr.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "prop": "pageprops",
            "format": "json",
            "titles": titres_concat
        }

        time.sleep(self.pause)
        data = self.requeteWikiMedia(url, params=params)
        if not data:
            logger.warning("[⚠️ QID Batch] Échec de la requête Wikipedia")
            return resultats

        try:
            pages = data.get("query", {}).get("pages", {})
            for page in pages.values():
                titre = page.get("title")
                qid = page.get("pageprops", {}).get("wikibase_item")
                if titre and qid:
                    resultats[titre] = qid
        except Exception as e:
            logger.error(f"[⛔ Parsing QID Batch] Erreur lors du parsing : {e}")

        return resultats
