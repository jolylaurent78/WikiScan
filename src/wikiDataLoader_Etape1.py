
from datetime import datetime
import json
import os
import re
import csv
from typing import List, Dict, Any, Optional, Set
import time as t
from urllib.parse import urlparse, unquote
from bs4 import BeautifulSoup, Tag
import requests
from urllib.parse import quote

from src.wikiDataLoader import BatchProcessing, BatchWriterJSON
from src.wikiDataLoader import EntreeHistorique, LigneProcess
from src.wikiDataLoader import logger

REPERTOIRE_INPUT = "input"

class BatchProcessingTitresExtraction(BatchProcessing):
    def __init__(self, runId: str, dossierSortie: str, pause: float = 0.1, max_lignes: Optional[int] = None):
        """
        Initialise le batch d'extraction pour l'étape 1 (titres Wikipédia).
        :param mot_cle: Recherche plein texte (ex: "Jeanne d'Arc")
        :param backlink: URL ou titre de page vers laquelle pointent les pages recherchées
        :param pause: Délai entre les requêtes API (en secondes)
        :param max_lignes: Nombre maximum de lignes à extraire (utile en debug)
        """

        super().__init__(runId=runId, etape=1)

        self.pause = pause
        self.max_lignes = max_lignes
        self.AnalyseDetaillee = False
        self.pagesTraitée = 0
        self.pagesIgnoree = 0
        self.batch = "CommandLine"
        self.writer = BatchWriterJSON(
            dossier_sortie=dossierSortie,
            fichierSortie=f"{runId}_Step1",
            runId=self.runId,
            etape=1,
            taille_batch=200)
        # On charge depuis un csv la liste des section à analyser
        self.plagesSections = {}  # doit être défini avant
        chemin_csv = os.path.join(REPERTOIRE_INPUT, f"{runId}.csv")
        self.chargerPlagesSectionsDepuisCSV(chemin_csv)

        # Le titre principal est la première ligne du CSV
        if not self.plagesSections:
            raise ValueError(f"[❌] Fichier {chemin_csv} vide ou invalide.")
        self.bltitle = list(self.plagesSections.keys())[0]

    #
    # Fonction de chargement du fichier CSV
    def chargerPlagesSectionsDepuisCSV(self, chemin_csv: str):
        if not os.path.exists(chemin_csv):
            print(f"[Etape 1] Aucun fichier CSV trouvé pour les plages de sections : {chemin_csv}")
            return
        with open(chemin_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                titre = unquote(row["titre"].strip())
                index_min_raw = row.get("index_min")
                index_max_raw = row.get("index_max")

                index_min = int(index_min_raw.strip()) if index_min_raw and index_min_raw.strip().isdigit() else None
                index_max = int(index_max_raw.strip()) if index_max_raw and index_max_raw.strip().isdigit() else None

                self.plagesSections[titre] = (index_min, index_max)


    def extraireTitreDepuisLienWiki(self, lien: str) -> str:
        """
        Extrait le titre de page Wikipédia à partir d’un lien brut du type '/wiki/...'
        """
        if not lien.startswith("/wiki/"):
            return ""
        titre = lien[len("/wiki/"):]
        return unquote(titre.replace("_", " ")).strip()


    def compter_backlinks_exhaustif(self, titre_page: str, pause: float = 0.2, max_pages: Optional[int] = None) -> int:
        """
        Compte de façon exhaustive le nombre total de pages qui pointent vers une page Wikipedia donnée.
        :param titre_page: ex: 'Jeanne_d\'Arc'
        :param pause: délai entre les requêtes (en secondes)
        :param max_pages: limite maximale pour test/debug
        """
        url = "https://fr.wikipedia.org/w/api.php"
        total = 0
        blcontinue = None

        while True:
            params = {
                "action": "query",
                "list": "backlinks",
                "bltitle": titre_page,
                "format": "json",
                "bllimit": 500
            }
            if blcontinue:
                params["blcontinue"] = blcontinue

            response = self.requeteWikiMedia(url, params=params)

            liens = response.get("query", {}).get("backlinks", [])
            total += len(liens)

            if max_pages and total >= max_pages:
                return max_pages

            blcontinue = response.get("continue", {}).get("blcontinue")
            if not blcontinue:
                break

            t.sleep(pause)

        return total



    def extraireArticlesDetailleesDepuisWikitexte(self, titre_page: str) -> List[str]:
        """
        Extrait les cibles des {{Article détaillé|...}} depuis le wikitexte brut.
        """
        url_api = "https://fr.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": titre_page,
            "rvslots": "main",
            "rvprop": "content"
        }

        data = self.requeteWikiMedia(url_api, params=params)
        if not data:
            return []

        articles_detailles = []

        try:
            pages = data["query"]["pages"]
            for page in pages.values():
                contenu = page.get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("*", "")
                # Match {{Article détaillé|Lien1}} ou {{Article détaillé|Lien1|Lien2}}
                matches = re.findall(r"\{\{[Aa]rticle détaillé\|([^\{\}]+?)\}\}", contenu)
                for match in matches:
                    liens = [titre.strip() for titre in match.split("|") if titre.strip()]
                    articles_detailles.extend(liens)

        except Exception as e:
            print(f"[Etape 1 ⚠️] Erreur de parsing du wikitexte : {e}")

        return articles_detailles

    def getSectionsUtile(self, titre):
        url = "https://fr.wikipedia.org/w/api.php"
        params = {
            "action": "parse",
            "page": titre,
            "format": "json",
            "prop": "sections"
        }
        r = requests.get(url, params=params)
        data = r.json()

        exclusions = {"voir aussi", "liens externes", "bibliographie", "notes et références", "sources"}
        sections_utiles = []

        for section in data.get("parse", {}).get("sections", []):
            nom = section["line"].strip()
            nom_norm = nom.lower()
            index = section["index"]
            if nom_norm not in exclusions:
                sections_utiles.append((index, nom))

        return sections_utiles


    def getLiensSortantsParAPIParse(self, titre, index_min=None, index_max=None):
        def getLiensDansSection(titre, index_section):
            url = "https://fr.wikipedia.org/w/api.php"
            params = {
                "action": "parse",
                "page": titre,
                "format": "json",
                "prop": "text",
                "section": index_section
            }
            data = self.requeteWikiMedia(url, params=params)
            html = data.get("parse", {}).get("text", {}).get("*", "")
            soup = BeautifulSoup(html, "html.parser")
            liens = [
                a["href"] for a in soup.find_all("a", href=True)
                if a["href"].startswith("/wiki/") and not a["href"].startswith("/wiki/Fichier:")
            ]
            return liens

        # Obtenir la liste des sections de la page
        url = "https://fr.wikipedia.org/w/api.php"
        params = {
            "action": "parse",
            "page": titre,
            "format": "json",
            "prop": "sections"
        }
        data = self.requeteWikiMedia(url, params=params)
        sections = data.get("parse", {}).get("sections", [])
        liens_totaux = set()

        for section in sections:
            try:
                idx = int(section["index"])
                if (index_min is None or idx >= index_min) and (index_max is None or idx <= index_max):
                    liens = getLiensDansSection(titre, idx)
                    liens_totaux.update(liens)
            except Exception as e:
                print(f"[Etape 1⚠️] Erreur section {section} de {titre} : {e}")

        return sorted(liens_totaux)





    def chargerEntrees(self) -> List[EntreeHistorique]:
        """
        Charge les entrées à partir d’un mot-clé ou d’un backlink.
        """

        def normaliserTitre(titre: str) -> str:
            titre = unquote(titre)
            titre = titre.replace("_", " ")
            return titre.strip()


        if self.bltitle:
            # 🔍 Étape 1 : construire la liste des pages qui référence la page principale
            print(f"[Etape 1 ℹ] Estimation des backlinks vers : {self.bltitle} ...")
            total = self.compter_backlinks_exhaustif(self.bltitle, pause=self.pause, max_pages=self.max_lignes)
            print(f"[Etape 1 ℹ] La page « {self.bltitle} » est référencée par {total} page(s) Wikipédia.")
            print(f"[Etape 1 ⏳] Chargement de la totalité des {total} pages...")

            lignes_brutes = self.recherche_par_backlink(self.bltitle, limit=self.max_lignes or total)
            entrees = []


            # 🔍 Étape 2 : on récupère les articles détaillés depuis le fichier CSV
            titres_lus = list(self.plagesSections.keys())
            articles_detailles = titres_lus[1:] if len(titres_lus) > 1 else []


            # On définit 2 listes pour le niveau de crossRéférence
            titres_crossRef2 = [self.bltitle]
            titres_crossRef1 = articles_detailles

            print(f"[Etape 1 🔗] Pages à explorer en plus pour liens sortants : {articles_detailles}")

            # 🔍 Étape 3 : extraire tous les liens sortants de ces pages
            liens_sortants_global1 = []
            for titre in titres_crossRef1:
                index_min, index_max = self.plagesSections.get(titre, (None, None))
                liens = self.getLiensSortantsParAPIParse(titre, index_min, index_max)
                liens_sortants_global1.extend(normaliserTitre(self.extraireTitreDepuisLienWiki(t)) for t in liens)


            liens_sortants_global2 = []
            for titre in titres_crossRef2:
                index_min, index_max = self.plagesSections.get(titre, (None, None))
                liens = self.getLiensSortantsParAPIParse(titre, index_min, index_max)
                liens_sortants_global2.extend(normaliserTitre(self.extraireTitreDepuisLienWiki(t)) for t in liens)

            print(f"[Etape 1 ✅] {len(liens_sortants_global1)+len(liens_sortants_global2)} liens sortants extraits depuis {len(articles_detailles)+1} page(s).")

            # Intersection
            titres_backlinks = set(normaliserTitre(l["titre"]) for l in lignes_brutes if "titre" in l)
            titres_sortants1 = set(liens_sortants_global1)
            titres_sortants2 = set(liens_sortants_global2)
            titres_croises1 = titres_backlinks & titres_sortants1
            titres_croises2 = titres_backlinks & titres_sortants2
            print(f"[Etape 1 🔁] {len(titres_croises2)} page(s) cross-référencée(s) de niveau 2 sur {len(lignes_brutes)} backlinks.")
            print(f"[Etape 1 🔁] {len(titres_croises1)} page(s) cross-référencée(s) de niveau 1 sur {len(lignes_brutes)} backlinks.")

            # On crée les EntreeHistorique
            for ligne in lignes_brutes:
                titre=ligne.get("titre", "")
                if  (titre in titres_croises2):
                    crossReferenceLevel = 2
                elif (titre in titres_croises1):
                    crossReferenceLevel = 1
                else:
                    crossReferenceLevel = 0
                ent = EntreeHistorique(
                    titre=titre,
                    url=ligne.get("url", ""),
                    source_backlink=self.bltitle,
                    crossReference=crossReferenceLevel
                )
                entrees.append(ent)


            return entrees



        else:
            raise ValueError("Aucun mot-clé (--motCle) ou backlink (--backlink) fourni.")



    def contientLienDansHTML(self, titre_page: str, lien_cible: str) -> bool:
        """
        Vérifie si la page Wikipedia `titre_page` contient un lien HTML vers `lien_cible`
        dans la section principale de contenu (div.mw-parser-output), en excluant les boîtes de navigation.
        """

        # Encodage du lien cible au format utilisé dans les href Wikipedia
        cible_norm = "/wiki/" + quote(lien_cible.replace(" ", "_").replace("’", "'"))

        # Appel à l'API parse de Wikipedia pour obtenir le HTML de l'article
        url_api = "https://fr.wikipedia.org/w/api.php"
        params = {
            "action": "parse",
            "format": "json",
            "page": titre_page,
            "prop": "text"
        }

        try:
            response = requests.get(url_api, params=params)
            response.raise_for_status()
            data = response.json()

            if "parse" not in data or "text" not in data["parse"]:
                print(f"[Etape 1 ⚠️] Page {titre_page} sans contenu HTML.")
                return False

            html = data["parse"]["text"]["*"]
            soup = BeautifulSoup(html, "html.parser")

            # ✅ Utiliser mw-parser-output, et non mw-content-text (inexistant dans l'API)
            content_div = soup.find("div", class_="mw-parser-output")
            if not content_div:
                print(f"[Etape 1 ❌] Pas de contenu principal trouvé pour {titre_page}")
                return False

            # 🔎 Parcours des liens dans le contenu principal
            for a in content_div.find_all("a", href=True):
                if cible_norm in a["href"]:
                    # Vérifie si le lien est dans une boîte à ignorer
                    current = a
                    while current:
                        classes = current.get("class", [])
                        if any(c in classes for c in ["navbox", "succession-box", "metadata", "infobox", "boite"]):
                            break
                        current = current.parent
                    else:
                        # Aucun parent suspect trouvé : lien valide
                        return True

        except Exception as e:
            print(f"[Etape 1 ❌] Exception dans contientLienDansHTML({titre_page}) : {e}")
            return False

        return False



    def traiterLigne(self, ligne: EntreeHistorique) -> EntreeHistorique:
        """
        Méthode par défaut : retourne la ligne sans modification.
        Peut être redéfinie dans les classes filles pour traitement spécifique.
        """
        if ligne.crossReference>0:
            self.pagesTraitée += 1
            lienDansTexte = self.contientLienDansHTML(ligne.titre, self.bltitle)
            if not lienDansTexte:
                self.pagesIgnoree+=1
                logger.warning(f"[Etape 1 ❌] {ligne.titre} ignorée (pas de lien réel vers {self.bltitle})")

            if self.pagesTraitée % 10 == 0:
                print(f"[Etape 1 ⚠️ ] {self.pagesIgnoree} pages ignorées sur {self.pagesTraitée} pages traitées ")

            if not lienDansTexte:
                return None

        return ligne



    def recherche_par_backlink(self, titre_page: str, limit: int = 10000) -> List[EntreeHistorique]:
        """
        Récupère toutes les pages qui contiennent un lien vers la page cible Wikipédia.
        :param titre_page: ex: "Jeanne_d'Arc"
        :param limit: nombre maximal de résultats
        :return: Liste d'objets EntreeHistorique
        """
        url = "https://fr.wikipedia.org/w/api.php"
        backlinks = []
        blcontinue = None

        while len(backlinks) < limit:
            params = {
                "action": "query",
                "list": "backlinks",
                "bltitle": titre_page,
                "format": "json",
                "bllimit": min(500, limit - len(backlinks)),
            }
            if blcontinue:
                params["blcontinue"] = blcontinue

            response = self.requeteWikiMedia(url, params=params)

            for entry in response.get("query", {}).get("backlinks", []):
                titre = entry["title"]
                url_page = f"https://fr.wikipedia.org/wiki/{titre.replace(' ', '_')}"

                backlinks.append({
                    "titre": titre,
                    "url": url_page,
                    "source": "backlink"
                })

            blcontinue = response.get("continue", {}).get("blcontinue")
            if not blcontinue:
                break

        return backlinks


    # On crée un fichier STOP à la fin
    def executer(self):
        super().executer()  # Ou traitement principal de l’étape 1
        self.writer.creerFichierStop()

    def afficherTitres(self):
        for titre in self.plagesSections.keys():
            print(f"\n[Etape 0 📘] Sections de : {titre}")
            sections = self.getSectionsUtile(titre)
            for index, titreSection in sections:
                print(f"  {index} → {titreSection}")

# test unitaress
if __name__ == "__main__":
    batch = BatchProcessingTitresExtraction("JD01", "dossierTest")

    for titre in batch.plagesSections.keys():
        print(f"\n📘 Sections de : {titre}")
        sections = batch.getSectionsUtile(titre)
        for index, titreSection in sections:
            print(f"  {index} → {titreSection}")
