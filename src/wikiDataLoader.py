# ───────────────────────────────────────
# Imports
# ───────────────────────────────────────
import json
import time
import os
import sqlite3
import requests

from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional
from abc import ABC, abstractmethod

# Pour la conversion GP->Lambert
from pyproj import Transformer
transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)


# ───────────────────────────────────────
# Objets métier : LigneProcess, EntreeHistorique
# ───────────────────────────────────────
@dataclass
class LigneProcess:
    run_id: str
    etape: int
    retry: int = 0

    def to_dict(self):
        data = {
            "run_id": self.run_id,
            "etape": self.etape,
}
        return {k: v for k, v in data.items() if v is not None}


@dataclass
class EntreeHistorique:
    titre: str
    url: str
    qid: Optional[str] = None
    source_backlink: Optional[str] = None
    crossReference: Optional[bool] = None
    process: LigneProcess = None
    resume: Optional[str] = None
    description: Optional[str] = None
    p31: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    x_l93: Optional[float] = None
    y_l93: Optional[float] = None
    nbLangues: Optional[int] = None
    notoriete: Optional[int] = None

    def to_dict(self):
        result = {
            "titre": self.titre,
            "url": self.url,
            "qid": self.qid,
            "source_backlink": self.source_backlink,
            "crossReference": self.crossReference,
            "process": self.process.to_dict(),
            "resume": self.resume,
            "description": self.description,
            "p31": self.p31,
            "lat": self.lat,
            "lon": self.lon,
            "x_l93": self.x_l93,
            "y_l93": self.y_l93,
            "nbLangues": self.nbLangues,
            "notoriete": self.notoriete
        }
        # Supprimer les champs à valeur None pour plus de lisibilité
        return {k: v for k, v in result.items() if v is not None}


    @classmethod
    def fromDict(cls, data: dict):
        process_data = data.get("process", {})
        process = LigneProcess(
            run_id=process_data.get("run_id", ""),
            etape=process_data.get("etape", 0),
)

        return cls(
            titre=data.get("titre"),
            url=data.get("url"),
            qid=data.get("qid"),
            source_backlink = data.get("source_backlink"),
            crossReference=data.get("crossReference"),
            process=process,
            resume=data.get("resume"),
            description=data.get("description"),
            p31=data.get("p31"),
            lat=data.get("lat"),
            lon=data.get("lon"),
            x_l93=data.get("x_l93"),
            y_l93=data.get("y_l93"),
            nbLangues=data.get("nbLangues"),
            notoriete=data.get("notoriete")
        )



    def convertirLambert93(self):
        if self.lat is not None and self.lon is not None:
            self.x_l93, self.y_l93 = transformer.transform(self.lon, self.lat)

    def calculerNote(self):
        if self.nbLangues is None:
            self.notoriete = None
        elif self.nbLangues >= 50:
            self.notoriete = 10
        elif self.nbLangues >= 30:
            self.notoriete = 8
        elif self.nbLangues >= 15:
            self.notoriete = 5
        elif self.nbLangues >= 5:
            self.notoriete = 3
        else:
            self.notoriete = 1


    def estGeolocaliseeEnFrance(self):
        if self.lat is None or self.lon is None:
            return False
        return 40.0 <= self.lat <= 51.0 and -6.0 <= self.lon <= 11.0


# ───────────────────────────────────────
# Objet Batch Processing
# ───────────────────────────────────────



class BatchProcessing(ABC):
    """
    Classe de base pour le traitement par batchs.
    Définit le squelette du pipeline, à spécialiser par héritage.
    """

    def __init__(self, runId: str, etape: int, nbLignesBatch: int = 1):
        self.runId = runId
        self.etape = etape
        self.resultats = []  # Liste d’objets métier valides
        self.discardes = []  # Liste d’objets en échec (si retry > max)

        # Reader et Writer de la structure métier
        self.reader = None
        self.writer = None

        # Mode par ligne ou mode batch
        self.nbLignesBatch = nbLignesBatch
        self.batch = []


    def executer(self):
        start = time.time()
        total = 0

        lignes = self.chargerEntrees()
        self.taggerLignes(lignes)

        for ligne in lignes:
            if self.nbLignesBatch > 1:
                self.batch.append(ligne)
                if len(self.batch) >= self.nbLignesBatch:
                    self.traiterBatch(self.batch)
                    self.batch.clear()
            else:
                resultat = self.traiterLigne(ligne)
                if resultat is not None:
                    self.writer.ajouter(resultat)
                else:
                    self.gerer_echec(ligne)
            total += 1

        if self.nbLignesBatch > 1 and self.batch:
            self.traiterBatch(self.batch)
            self.batch.clear()

        if hasattr(self, "finTraitement"):
            self.finTraitement()

        if self.writer.besoinSauvegarder():
            self.writer._sauvegarder_batch()
        duree = time.time() - start
        logger.info(f"[⏱️ Perf] {total} lignes traitées en {duree:.2f} secondes")



    def taggerLignes(self, entrees: List[EntreeHistorique]):
        for entree in entrees:
            entree.process = LigneProcess(
                run_id=self.runId,
                etape=self.etape
            )


    def chargerEntrees(self) -> List[EntreeHistorique]:
        return None


    def traiterLigne(self, ligne: EntreeHistorique) -> EntreeHistorique:
        return ligne


    def traiterBatch(self, lignes: List[EntreeHistorique]):
        """
        Par défaut : applique traiterLigne() à chaque ligne du batch.
        Peut être redéfini dans les sous-classes pour traitement groupé.
        """
        for ligne in lignes:
            resultat = self.traiterLigne(ligne)
            if resultat is not None:
                self.writer.ajouter(resultat)
            else:
                self.gerer_echec(ligne)

    def gerer_echec(self, ligne):
        """Par défaut, ajoute à la liste des lignes rejetées"""
        self.discardes.append(ligne)

    # ───────────────────────────────────────
    # Gestion des APIs Wikipedia
    # ───────────────────────────────────────
    def requeteWikiMedia(self, url, params=None, raw_url=False):
        try:
            t0 = time.time()
            headers = {"User-Agent": "ChouetteBot/1.0"}

            if raw_url:
                response = requests.get(url, timeout=10, headers=headers)
            else:
                response = requests.get(url, params=params, timeout=10, headers=headers)

            dt = time.time() - t0

            if response.status_code != 200:
                logger.error(f"[❌ Erreur] {response.status_code} pour {url}")
            elif dt > 1:
                logger.warning(f"[⚠️ Lent] Requête vers {url} a pris {dt:.2f}s")

            return response.json()
        except Exception as e:
            logger.exception(f"[❌ Exception] Requête échouée pour {url} : {e}")
            return None



    def requeteSPARQL(self, titre: str, query: str, max_retries: int = 3, pause: float = 0.8) -> Optional[dict]:

        url = "https://query.wikidata.org/sparql"
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "ChouetteBot/1.0 (https://jolylaurent78@gmail.com)"
        }

        for tentative in range(1, max_retries + 1):
            try:
                logging.debug(f"[🔍 SPARQL] Tentative {tentative} – Pause {pause:.2f}s")
                time.sleep(pause)

                response = requests.get(url, params={"query": query}, headers=headers)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    logging.warning(f"[⚠️ SPARQL] Requête refusée (429) – Attente {retry_after}s")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                reponse = response.json()

                # Vérification du contenu
                if not reponse or "results" not in reponse or "bindings" not in reponse["results"]:
                    logging.warning(f"[⚠️ Aucun résultat SPARQL] Réponse invalide ou incomplète pour pour {titre}.")
                    return None

                bindings = reponse["results"]["bindings"]
                if not bindings:
                    logging.warning(f"[⚠️ Aucun résultat SPARQL] Bindings vide pour {titre}.")
                    return None

                return reponse

            except Exception as e:
                logging.error(f"⛔ Erreur SPARQL (tentative {tentative}) : {e}")
                pause *= 2  # Exponentiel backoff

        return None



# ───────────────────────────────────────
# Objet Batch Writer
# ───────────────────────────────────────


class BaseWriter(ABC):
    @abstractmethod
    def ajouter(self, entree):
        """
        Ajoute une entrée au writer. À implémenter dans chaque Writer concret.
        """
        pass

    @abstractmethod
    def besoinSauvegarder(self):
        pass

    @abstractmethod
    def _sauvegarder_batch(self):
        """
        Sauvegarde ou commit final du batch. Peut inclure une fermeture de fichier ou de connexion.
        """
        pass

class BatchWriterJSON(BaseWriter):
    def __init__(self, dossier_sortie, fichierSortie, runId, taille_batch=None):
        self.dossier_sortie = dossier_sortie
        self.fichierSortie = fichierSortie
        self.nom_entree = os.path.splitext(os.path.basename(fichierSortie))[0]
        self.runId = runId
        self.taille_batch = taille_batch
        self.batch_unique = self.taille_batch is None
        self.lignes = []
        self.compteur_fichier = 1
        self.buffer = []

        os.makedirs(dossier_sortie, exist_ok=True)

    def ajouter(self, ligne):
        self.lignes.append(ligne)
        if not self.batch_unique and len(self.lignes) >= self.taille_batch:
            self._sauvegarder_batch()

    def besoinSauvegarder(self):
        return self.lignes

    def _sauvegarder_batch(self):
        if self.batch_unique:
            nom_fichier = f"{self.nom_entree}.json"
        else:
            nom_fichier = f"{self.nom_entree}_batch_{self.compteur_fichier:03d}.json"

        chemin = os.path.join(self.dossier_sortie, nom_fichier)
        with open(chemin, "w", encoding="utf-8") as f:
            for ligne in self.lignes:
                f.write(json.dumps(ligne.to_dict(), ensure_ascii=False) + "\n")

        print(f"[💾] Batch {self.compteur_fichier} sauvegardé avec {len(self.lignes)} lignes")
        self.lignes = []

        if not self.batch_unique:
            self.compteur_fichier += 1

    def creerFichierStop(self):
        # Création du fichier STOP
        stopPath = os.path.join(self.dossier_sortie, f"{self.runId}_STOP")
        with open(stopPath, "w", encoding="utf-8") as f:
            f.write("Fin de génération du batch.")
        print(f"[✔️] Fichier STOP créé : {stopPath}")



class BatchWriterSQLite(BaseWriter):
    def __init__(self, chemin_db):
        self.chemin_db = chemin_db
        self.conn = None
        self.cursor = None
        self.nb_inserts = 0
        self._ouvrir_connexion()
        self.batch_id = None

    def _ouvrir_connexion(self):
        if not os.path.exists(self.chemin_db):
            logging.warning(f"[⚠️] La base de données '{self.chemin_db}' n'existe pas encore. Elle sera créée automatiquement.")
        self.conn = sqlite3.connect(self.chemin_db)
        self.cursor = self.conn.cursor()


    def ajouter(self, entree):

        # On rajoute une entrée dans HistoriqueInsertion pour tracabilité
        if self.batch_id is None:
            self.cursor.execute("""
                INSERT INTO HistoriqueInsertion (source_backlink, date_insertion)
                VALUES (?, ?)
            """, (entree.source_backlink, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

            self.batch_id = self.cursor.lastrowid  # ← à injecter dans chaque EntreeHistorique ensuite


        self.cursor.execute("""
            INSERT OR IGNORE INTO EntreeHistorique (
                qid, titre, lat, lon, lambert_x, lambert_y, p31, summary, description, source_backlink, url, crossReference, batch_id, nbLangues, notoriete ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entree.qid,
            entree.titre,
            entree.lat,
            entree.lon,
            entree.x_l93,
            entree.y_l93,
            entree.p31,
            entree.resume,
            entree.description,
            entree.source_backlink,
            entree.url,
            entree.crossReference,
            self.batch_id,
            entree.nbLangues,
            entree.notoriete
        ))
        self.nb_inserts += 1

    def besoinSauvegarder(self):
        return True

    def _sauvegarder_batch(self):
        try:
            if self.conn:
                if self.batch_id is not None:
                    self.cursor.execute("""
                        UPDATE HistoriqueInsertion
                        SET nb_entrees = ?
                        WHERE id = ?
                    """, (self.nb_inserts, self.batch_id))


                self.conn.commit()
                self.conn.close()
                logging.info(f"[💾] {self.nb_inserts} entrées insérées et connexion fermée.")
        except Exception as e:
            logging.error(f"[❌] Erreur lors du commit/finalisation: {e}")


# ───────────────────────────────────────
# Objet Batch Reader
# ───────────────────────────────────────
class BatchReaderJSON:
    def __init__(self, fichierSource: str):
        self.fichierSource = fichierSource

    def loadLignes(self) -> List[EntreeHistorique]:
        lignes = []
        with open(self.fichierSource, "r", encoding="utf-8") as f:
            for ligne in f:
                d = json.loads(ligne)
                lignes.append(EntreeHistorique.fromDict(d))
        return lignes

# ───────────────────────────────────────
# Gestion des logs
# ───────────────────────────────────────
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("wiki")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler("logs/wiki_api.log", maxBytes=10_000_000, backupCount=4, encoding="utf-8")
formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(handler)









