from typing import List, Optional

from src.wikiDataLoader import BatchProcessing, BatchWriterSQLite, BatchReaderJSON, logger
from src.wikiDataLoader import EntreeHistorique, LigneProcess


class BatchProcessingInsertionBD(BatchProcessing):
    def __init__(self, runId: str,fichierInput : str, db: str):
        super().__init__(runId=runId, etape=5, nbLignesBatch = 1)
        self.nom_process = "InsertionBD"
        self.reader = BatchReaderJSON(fichierInput)
        self.writer = BatchWriterSQLite(db)
        self.p31Connus = set()

    def chargerEntrees(self) -> List[EntreeHistorique]:
        lignes = self.reader.loadLignes()
        print(f"[Etape 5 ✅ ] {len(lignes)} lignes chargées depuis {self.reader.fichierSource}")

        # On charge aussi la table P31Classification
        cursor = self.writer.conn.cursor()
        cursor.execute("SELECT p31 FROM P31Classification")
        self.p31Connus = {row[0] for row in cursor.fetchall()}

        # Extraire tous les P31 présents dans les lignes à traiter
        p31DansBatch = set()
        for entree in lignes:
            if entree.p31 is not None:
                p31DansBatch.update(entree.p31)
        nouveauxP31 = p31DansBatch - self.p31Connus
        # Affichage du nombre de nouveaux P31 à insérer
        print(f"[Etape 5 📊 ] {len(nouveauxP31)} nouveaux P31 à insérer dans P31Classification.")

        return lignes


    def recupererLabelDepuisAPI(self, qid: str) -> Optional[str]:
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
        data = self.requeteWikiMedia(url, raw_url=True)

        if not data:
            logger.warning(f"[⚠️] Aucune donnée récupérée pour {qid}")
            return None

        try:
            label = data["entities"][qid]["labels"]["fr"]["value"]
            return label
        except KeyError:
            logger.warning(f"[⚠️] Label FR introuvable pour {qid}")
            return None


    def traiterLigne(self, ligne):
        """
        Ici, l'entrée est déjà un objet EntreeHistorique.
        Aucune transformation métier n’est nécessaire.
        """

        # Ajout automatique dans SourceBacklink si absent
        cursor = self.writer.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM SourceBacklink WHERE source_backlink = ?", (ligne.source_backlink,))
        count = cursor.fetchone()[0]

        if count == 0 and ligne.source_backlink:
            cursor.execute("""
                INSERT INTO SourceBacklink (source_backlink, url, couleur, visible)
                VALUES (?, ?, ?, ?)
            """, (
                ligne.source_backlink,
                ligne.url,         # Par défaut, on utilise l'URL de l’entrée
                "(0,0,0)",         # Couleur par défaut
                1                  # Visible = True
            ))
            print(f"[Etape 5 ➕ ] SourceBacklink ajoutée : {ligne.source_backlink}")


        if not ligne.p31 or not ligne.p31.startswith("Q"):
            logger.warning(f"[Etape 5 ⚠️] P31 invalide ou manquant pour {ligne.qid} → {ligne.p31}")
            return ligne  # on ignore l'entrée sans planter

        if ligne.p31 not in self.p31Connus:
            label = self.recupererLabelDepuisAPI(ligne.p31)  # → appel Wikidata REST
            cursor.execute(
                "INSERT INTO P31Classification (p31, label, statut) VALUES (?, ?, ?)",
                (ligne.p31, label, "non_defini")
            )
            self.p31Connus.add(ligne.p31)

        self.writer.ajouter(ligne)
        return ligne

    def finTraitement(self):
        """
        Rien à faire ici dans ce cas simple.
        """
        pass

    def flushBatch(self):
        """
        Rien à faire ici non plus dans le cas d’un batch unique (tout est géré en fin de process).
        """
        pass
