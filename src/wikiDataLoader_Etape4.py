import time

from typing import List, Optional

from src.wikiDataLoader import BatchProcessing, BatchWriterJSON, BatchReaderJSON, logger
from src.wikiDataLoader import EntreeHistorique, LigneProcess



class BatchProcessingResumeDescription(BatchProcessing):
    def __init__(self, runId: str, fichierInput: str, dossierSortie: str, pause: float = 0.5):
        super().__init__(runId=runId, etape=4, nbLignesBatch = 20)
        self.reader = BatchReaderJSON(fichierInput)
        self.writer = BatchWriterJSON(
            dossier_sortie = dossierSortie,
            runId = runId,
            fichierSortie=fichierInput.replace("Step3", "Step4")
        )
        self.pause = pause

    def chargerEntrees(self) -> List[EntreeHistorique]:
        lignes = self.reader.loadLignes()
        print(f"✅ {len(lignes)} lignes chargées depuis {self.reader.fichierSource}")
        return lignes

    def traiterBatch(self, lignes: List[EntreeHistorique]):
        for ligne in lignes:
            time.sleep(self.pause)
            resume, description = self.recupererResumeEtDescription(ligne.titre)
            if resume:
                ligne.resume = resume
            if description:
                ligne.description = description
            self.writer.ajouter(ligne)

    def recupererResumeEtDescription(self, titre: str) -> (Optional[str], Optional[str]):
        url = f"https://fr.wikipedia.org/api/rest_v1/page/summary/{titre.replace(' ', '_')}"
        try:
            data = self.requeteWikiMedia(url)
            resume = data.get("extract")
            description = data.get("description")
            return resume, description
        except Exception as e:
            logger.warning(f"[⚠️ Wikipedia REST] Erreur pour {titre} : {e}")
            return None, None