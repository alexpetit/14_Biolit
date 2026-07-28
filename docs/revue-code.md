# Revue de code — Biolit (à valider en équipe)

> Analyse réalisée après la mise en place du déploiement Clever Cloud.
> Objectif : améliorer lisibilité, structure et documentation, sans changer le comportement.
> Chaque changement touchant les dépendances ou le S3 doit être suivi d'un **rebuild + run de validation**.

## Architecture actuelle : 2 pipelines distincts

- **`pipelines/run.py`** — pipeline **opérationnel** (Clever Cloud) :
  ingestion API → géoloc → ML (crop + classification) → Label Studio → `db_finale`.
  Utilise : `export_api`, `create_table`, `geoloc`, `flow_gatekeeper`, `label_studio`, `s3`,
  `ml/crop_inference`, `ml/classification`.
- **`pipelines/export_inpn.py`** — pipeline **enrichissement INPN** (séparé, couvert par des tests).
  Utilise : `observations`, `taxref`, `lien_doris`, `inaturalist`, `visualisation`.

➡️ Documenter clairement cette dualité (deux flux, deux buts).

## 🔴 P1 — Quick wins (faible risque, allègent l'image)

1. **Code mort à supprimer**
   - `biolit/label_studio_postprocessing.py` — importé nulle part.
   - `biolit/minio.py` — utilisé uniquement par le fichier mort ci-dessus.
2. **Dépendances mortes / mal placées (`pyproject.toml`)**
   - `minio`, `s3cmd` → retirer (code correspondant supprimé).
   - `autodistill*`, `roboflow`, `supervision` → R&D uniquement (`ml/yolov8_DINO`).
     À déplacer vers `pyproject-ml.toml` → allège encore l'image de prod.

## 🟠 P2 — Duplication & cohérence

3. **`create_s3_client` en double** — `biolit/s3.py` + `ml/classification/classifier_s3.py`
   (ce dernier délègue déjà) → `classifier_s3` devrait juste importer celui de `biolit.s3`.
4. **Client S3 recréé plusieurs fois dans `geoloc.py`** (module-level + re-création dans
   `get_info_communes` / `get_trace_littoral`) → centraliser.
5. **Config S3/DB éparse** — `create_s3_client` ne lit plus que `CELLAR_ADDON_*`
   (le fallback `aws_*` / local a sauté lors d'un rewrite). Homogénéiser + réintroduire un
   fallback propre pour le dev local.

## 🟡 P3 — Robustesse / dette technique

- **`verify=False`** (SSL désactivé) dans `create_s3_client` → risque ; préférer
  `addressing_style=path` + vérification du certificat.
- **Inserts Postgres ligne par ligne** (`create_table.py` : `insert_dataframe`,
  `insert_enriched_dataframe`, …) → lent sur ~26k lignes ; passer en `executemany` / `COPY`.
- **`print()` au lieu de `LOGGER`** — `observations.py`, `lien_doris.py`.
- **Valeurs en dur dispersées** — bucket `"biolit-uploads"`, clé `"doris_data.csv"`,
  projets `"Biolit Crops"` / `"Biolit No Crops"`, chemins géoloc → centraliser en constantes/config.

## 🔵 P4 — Structure & documentation

- **`run.py`** mélange helpers et orchestration → extraire des sous-fonctions par étape,
  ajouter un docstring décrivant le flux.
- **`ml/`** mélange prod (`crop_inference`, `classification`) et R&D (`yolov8_DINO`,
  `prompt_textuel_yolo`, `BioCLIP/scripts`, `BioClipv2`) → séparer (ex. `ml/research/`) + README.
- **Docstrings** manquantes sur des fonctions clés.
- **README** décrit surtout l'ancien flux (Label Studio local) → actualiser : flux `run.py`,
  variables d'environnement (`CELLAR_ADDON_*`, `POSTGRESQL_ADDON_URI`, `LABEL_STUDIO_*`,
  `BIOLIT_API_URL`, `AWS_STORAGE_BUCKET_NAME`), procédure de déploiement Clever Cloud.

## Ordre d'exécution proposé

**P1** (code mort + deps) → **P2** (déduplication) → **P3 / P4** (dette + structure + doc), au fil de l'eau,
avec rebuild + run de validation à chaque étape sensible.
