# Déploiement sur Clever Cloud

Déploiement de la pipeline d'ingestion Biolit (`pipelines/run.py`) depuis Git, sous
forme de **Clever Task** (conteneur Docker qui démarre, exécute la pipeline, puis
s'arrête). Postgres et le stockage S3 sont fournis par des **add-ons managés**.

## Architecture

| Local (`docker-compose`) | Clever Cloud                                  |
| ------------------------ | --------------------------------------------- |
| Container Postgres       | Add-on **PostgreSQL**                         |
| Container MinIO          | Add-on **Cellar** (S3) *ou* Scaleway existant |
| `pipelines/run.py`       | App **Docker en mode Task** + GitHub Actions  |
| Container Label Studio   | App Docker séparée *(2ᵉ temps)*               |

> ⚠️ La pipeline exécute aussi l'inférence ML (YOLO crop + BioCLIP), donc l'image
> est lourde (torch) et la Task demande de la RAM/CPU. Prévoir un scaler `M`+ (≥ 4 Go).

## Pourquoi une Task et pas un cron classique ?

- Les apps Docker sur Clever Cloud **doivent écouter un port** (health check) — ce
  qui n'a pas de sens pour un batch. Le mode **Task** lève cette contrainte : pas de
  port, pas de health check.
- `clevercloud/cron.json` **n'est pas supporté en Docker**. On planifie donc la Task
  via un workflow **GitHub Actions** (`.github/workflows/clevercloud-ingestion.yml`)
  qui appelle `clever restart`.

## Variables d'environnement attendues par le code

| Variable                             | Source                               |
| ------------------------------------ | ------------------------------------ |
| `POSTGRES_URL` / `POSTGRESQL_ADDON_URI` | add-on PostgreSQL (auto) ou manuel |
| `aws_url` / `CELLAR_ADDON_HOST`      | add-on Cellar (auto) ou Scaleway     |
| `aws_access_key_id` / `CELLAR_ADDON_KEY_ID` | idem                          |
| `aws_secret_access_key` / `CELLAR_ADDON_KEY_SECRET` | idem                  |
| `aws_region` *(optionnel)*           | ex. `fr-par` pour Scaleway           |
| `BIOLIT_API_URL`                     | à définir (token API Biolit)         |
| `LABEL_STUDIO_URL`                   | URL de l'app Label Studio            |
| `LABEL_STUDIO_API_KEY_DATAFORGOOD`   | clé API Label Studio                 |
| `CC_RUN_COMMAND`                     | `uv run python -m pipelines.run`     |

Le code lit d'abord les variables locales (`POSTGRES_URL`, `aws_*`) puis bascule
automatiquement sur les variables d'add-on Clever Cloud (`*_ADDON_*`) si absentes.

## Étapes

### 1. Installer et se connecter

```bash
npm install -g clever-tools
clever login
```

### 2. Créer l'app Task (build Docker)

```bash
clever create --type docker biolit-ingestion --region par --task "uv run python -m pipelines.run"
# (selon la version de clever-tools, définir plutôt : clever env set CC_RUN_COMMAND "uv run python -m pipelines.run")
```

### 3. Add-ons managés

```bash
clever addon create postgresql-addon --plan dev    biolit-pg
clever addon create cellar-addon     --plan s      biolit-cellar
clever service link-addon biolit-pg
clever service link-addon biolit-cellar
```

> Les add-ons injectent automatiquement `POSTGRESQL_ADDON_URI`,
> `CELLAR_ADDON_HOST`, `CELLAR_ADDON_KEY_ID`, `CELLAR_ADDON_KEY_SECRET`.
> Crée le(s) bucket(s) Cellar nécessaires (ex. `biolit-uploads`) depuis la console
> ou `s3cmd` — le nom de bucket est codé en dur dans `run.py` / `crop_inference`.

#### ⚠️ Bootstrap du schéma SQL (OBLIGATOIRE)

Contrairement à `docker-compose`, **un add-on PostgreSQL n'exécute pas `init.sql`
automatiquement**. Or le code applicatif ne crée que `observations`,
`observations_enriched` et `ml_taxonomy` — **pas** `ml_crops`, `ml_no_crops` ni
`doris_table`. Sans ce bootstrap, le pipeline plante (`relation "ml_crops" does not
exist` / erreur `split_part`). À faire **une fois** après création de l'add-on :

```bash
# Récupérer l'URI exposée par l'add-on (console CC ou `clever env`)
psql "$POSTGRESQL_ADDON_URI" -f infra/init.sql
```

### 4. Variables d'environnement applicatives

```bash
clever env set BIOLIT_API_URL "https://biolit.fr/wp-json/biolit/v1/observations?token=..."
clever env set LABEL_STUDIO_URL "https://<label-studio>.cleverapps.io"
clever env set LABEL_STUDIO_API_KEY_DATAFORGOOD "..."
# Si on garde Scaleway au lieu de Cellar :
# clever env set aws_url "https://s3.fr-par.scw.cloud"
# clever env set aws_access_key_id "..." && clever env set aws_secret_access_key "..." && clever env set aws_region "fr-par"
```

### 5. Déployer

```bash
git push clever feat/clevercloud-deployment:master
# Lancement manuel d'un run :
clever restart
```

### 6. Planification (cron) via GitHub Actions

Définir dans le repo GitHub (Settings → Secrets → Actions) :
`CLEVER_TOKEN`, `CLEVER_SECRET`, `CLEVER_INGESTION_APP_ID`.
Le workflow `.github/workflows/clevercloud-ingestion.yml` relance la Task chaque
jour à 02:00 UTC (et est déclenchable à la main via *Run workflow*).

## Reste à faire

- Déployer **Label Studio** comme app Docker séparée (backend Postgres + Cellar,
  les volumes/FS Buckets n'étant pas supportés en Docker).
- Vérifier le dimensionnement de la Task (RAM pour torch) après le 1er run.
