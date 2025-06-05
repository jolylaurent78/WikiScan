# 🧠 Scan Wikipedia

Outils Python pour l'extraction, le traitement et la structuration de données historiques issues de Wikipedia et Wikidata.  
Le projet s'intègre dans un pipeline plus large destiné à alimenter une base de données géohistorique utilisée par un simulateur cartographique.

---

## 📁 Structure du projet

```
Wiki/
├── src/                   # Code source principal (pipeline, étapes, modules)
│   ├── main.py            # Point d'entrée principal (multi-étapes)
│   ├── wikiDataLoader_Etape1.py → Etape5.py
│   └── __init__.py
├── input/                # Données d'entrée (fichiers JSON, CSV, listes de QIDs)
├── data/                 # Résultats de traitement (organisés par runId)
├── logs/                 # Fichiers de journalisation
├── doc/                  # Documentation technique ou notes
├── specs/                # Spécifications de format ou d'intégration
├── launcher_etapeX.py    # Fichiers de lancement individuels par étape
├── requirements.txt      # Dépendances Python minimales
└── README.md             # Ce fichier
```

> 📌 La base de données partagée `WikiCarto.db` est stockée **dans un dossier `shared-db/` à un niveau supérieur**, accessible par les différents projets.

---

## ⚙️ Installation

Pré-requis : Python 3.10+

Installation des dépendances :

```bash
pip install -r requirements.txt
```

---

## 🚀 Lancement d'une étape

Exécution classique en ligne de commande :

```bash
python -m src.main --runId JD01 --step 1
```

Exécution manuelle depuis Pyzo pour debug :

```python
from src.main import main
main("JD01", 1)
```

Tu peux aussi utiliser les fichiers `launcher_etapeX.py` pour chaque étape individuellement.

---

## 📦 Données

| Type | Dossier | Commentaire |
|------|---------|-------------|
| Données d’entrée | `input/` | Listes de QID, fichiers de config |
| Résultats de traitement | `data/<runId>/` | Fichiers générés par étape |
| Logs | `logs/` | Fichiers `.log` ou `.txt` par étape ou runId |
| Base partagée | `../shared-db/WikiCarto.db` | Utilisée par plusieurs projets |

---

## 📚 Dépendances

```text
beautifulsoup4
```

---

## ✅ TODO

- [ ] Centraliser la gestion des chemins (`chemins.py`)
- [ ] Ajouter des tests unitaires dans `tests/`
- [ ] Documenter chaque étape dans `doc/`
