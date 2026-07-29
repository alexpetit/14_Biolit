# Déploiement Clever Cloud — état & passation

> Doc de passation pour reprendre le travail (y compris depuis une autre machine).
> Branche de travail : `feat/clevercloud-deployment`.

## 1. Reprendre sur une nouvelle machine

```bash
# 1. Récupérer le code
git clone https://github.com/alexpetit/14_Biolit.git
cd 14_Biolit
git checkout feat/clevercloud-deployment

# 2. Outils
#   - Python 3.12 + uv        (https://docs.astral.sh/uv/)
#   - Node 18 + npm
#   - clever-tools v3 (v4 casse sur Node 18) :
npm install -g clever-tools@3
uv sync                       # crée le venv local

# 3. Auth Clever Cloud (ouvre le navigateur)
clever login

# 4. Lier le dépôt aux apps CC (recrée .clever.json, non versionné) :
clever link app_a9ba8ca0-1212-42ee-9dcf-1a17158fae08 --alias biolit-ingestion
clever link app_b338e77e-3b2c-46e9-a26f-50c0c95db8e4 --alias biolit-ls
clever link app_3048d33c-6af4-4548-8af6-803974dd14bc --alias biolit-mb
```

### Fichiers locaux NON versionnés (à recopier depuis l'ancien PC, canal sûr)
- `infra/.env` — secrets prod. Clés attendues : `BIOLIT_API_URL` (token), `POSTGRES_URL`,
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_S3_ENDPOINT` (Scaleway),
  `AWS_STORAGE_BUCKET_NAME`.
- `.clever.json` — régénéré par les `clever link` ci-dessus (pas besoin de le copier).
- (Le dump Metabase est sauvegardé sur Cellar : `migration/pg-dump-metabase-1785271271.dmp`.)

> ⚠️ Le déploiement passe par `clever deploy` (= `git push` du HEAD committé vers le git **de CC**,
> pas GitHub). L'historique ayant été réécrit, `clever deploy --force` est nécessaire.

## 2. Ce qui est déployé (compte perso — environnement de validation)

| App / add-on | ID / nom | Rôle |
|---|---|---|
| `biolit-ingestion` (Task Docker, scaler **M**) | `app_a9ba8ca0-1212-42ee-9dcf-1a17158fae08` | pipeline complet, run 100% vert |
| `biolit-label-studio` (web) | `app_b338e77e-3b2c-46e9-a26f-50c0c95db8e4` | LS 1.13.1, backend biolit-pg |
| `biolit-metabase` (web) | `app_3048d33c-6af4-4548-8af6-803974dd14bc` | Metabase v0.50.32, app-DB MySQL dev |
| add-on `biolit-pg` (PostgreSQL dev) | — | données biolit + LS |
| add-on `biolit-cellar` (Cellar S3) | — | images/crops/parquets + dumps migration |
| add-on `biolit-mb-mysql` (MySQL dev) | — | app-DB Metabase (contournement) |

**Toutes les apps sont laissées `stopped`** (économie). Les relancer : `clever restart --alias <alias>`.

Optimisations validées : image **torch CPU-only** (7,2→3,3 Go) + traits de côte clippés **France**
(1,24 Go→qq Mo) → l'ingestion tient dans un scaler **M**.

## 3. Cible finale : org Clever Cloud **Planète Mer**

`orga_33229b84-080b-48d1-98e4-7153d80e2e8d` (~200€ crédit, containers **M max**).
Rejouer les scripts sous `--org`, avec un **PostgreSQL dédié** pour Metabase
(le plan dev bloque `pg_database` pour les migrations Metabase).

**Décisions actées** : base « données » = **dev (gratuit)** ; base Metabase = **dédié xxs_sml**.

## 4. Migration depuis la prod (Coolify + infra d4g)

- **Metabase** : Coolify, version **v0.60.1.3** (`biolitmetabase.services.d4g.fr`).
  Dump `pg_dump -Fc` récupéré → **Cellar** `migration/pg-dump-metabase-1785271271.dmp`.
  → Metabase Planète Mer à pinner sur `metabase/metabase:v0.60.1.3` (restore en version ≥ source).
- **Label Studio** : infra d4g Scaleway (ansible `d4g-ansible/roles/services/biolit-labelstudio`),
  version **1.22.0** (`biolitlabel.services.dataforgood.fr`). Annotations dans le **PostgreSQL
  partagé** `shared-postgresql` (postgres:16, **volume persistant**), base `biolit`, réseau interne
  `d4g-internal` → **dump à faire côté serveur** (`pg_dump` de la base `biolit`).
  → LS Planète Mer à pinner sur **1.22.0**.

## 5. Blocages (côté humain) avant de continuer le déploiement

1. **Rôle** : l'utilisateur est **DEVELOPER** sur l'org Planète Mer → ne peut pas créer d'apps/add-ons.
   Demander à `administration@planetemer.org` une promotion **MANAGER/ADMIN**.
2. **Dump Label Studio** : à produire côté serveur d4g (voir §4).

## 6. Reste à faire (après déblocage)

1. Créer apps + add-ons dans l'org Planète Mer, appliquer `init.sql`, créer bucket Cellar,
   uploader `doris_data.csv`, configurer env vars, déployer.
2. Restaurer les dumps (Metabase `pg_restore` dans son Postgres dédié ; LS `pg_restore` dans le
   Postgres données), puis re-pointer la source Metabase sur le biolit-pg de Planète Mer.
3. Cron : secrets GitHub (`CLEVER_TOKEN`, `CLEVER_SECRET`, `CLEVER_INGESTION_APP_ID`) pour le
   workflow `.github/workflows/clevercloud-ingestion.yml`.
4. Vraies configs d'annotation Label Studio.
5. Appliquer la revue de code (voir `docs/revue-code.md`) après validation équipe.
