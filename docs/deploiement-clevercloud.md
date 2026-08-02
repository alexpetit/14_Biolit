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
| add-on `biolit-pg` (PostgreSQL **xxs_sml**, 5,25€/mois) | — | données biolit + LS (upgradé depuis `dev` : le plan gratuit limite à 5 connexions, insuffisant pour LS seul → passé à 45 connexions) |
| add-on `biolit-cellar` (Cellar S3) | — | images/crops/parquets + dumps migration |
| add-on `biolit-mb-mysql` (MySQL dev) | — | app-DB Metabase (contournement) |

**Toutes les apps sont laissées `stopped`** (économie). Les relancer : `clever restart --alias <alias>`.

Optimisations validées : image **torch CPU-only** (7,2→3,3 Go) + traits de côte clippés **France**
(1,24 Go→qq Mo) → l'ingestion tient dans un scaler **M**.

### 2.1 Affichage des images dans Label Studio (Cloud Storage S3)

Le pipeline écrit `path_s3` en base sous forme de chemin brut `s3://biolit-uploads/...`
([ml/crop_inference/predict.py](../ml/crop_inference/predict.py),
[ml/classification/classifier_s3.py](../ml/classification/classifier_s3.py)), réutilisé tel quel
comme champ `"image"` des tasks LS ([biolit/label_studio.py](../biolit/label_studio.py)). Un
`s3://` n'est pas chargeable directement par le navigateur : il faut que **chaque projet LS** ait
une **Cloud Storage S3** configurée pour que LS resigne lui-même ces chemins en URLs HTTPS.

Config actuelle (à refaire si les projets LS sont recréés — pas versionné, vit dans la DB LS) :
- Un import storage S3 par projet (`Biolit Crops` id=1, `Biolit No Crops` id=2), créé via
  `POST /api/storages/s3` (token API dans les identifiants de session) avec :
  `bucket=biolit-uploads`, `s3_endpoint=https://cellar-c2.services.clever-cloud.com`,
  `region_name=fr-par`, credentials = celles de l'addon `biolit-cellar`
  (`clever addon env addon_633d9ab6-3167-4479-8fa6-f3cd7fc86b66`), `use_blob_urls=false`,
  `presign=true`, `presign_ttl=1440`.
- **CORS sur le bucket Cellar** (`biolit-uploads`) : sans ça, l'URL présignée fonctionne en
  ouverture directe mais le viewer LS bloque le chargement (`There was an issue loading URL from
  $image value`). Réglé via `put_bucket_cors` (boto3) avec `AllowedOrigins: ["*"]`,
  `AllowedMethods: ["GET"]` — le bucket n'avait **aucune** config CORS avant.

### 2.2 Compte Metabase existant

Un admin `admin@biolit.fr` existe déjà dans l'app-DB `biolit-mb-mysql` (créé le 23/07, **26
questions + 1 dashboard + 2 connexions déjà en place** — ce n'est pas une base vierge). Le mot de
passe d'origine était perdu (aucune trace). Pas de SMTP configuré (pas de reset par email), pas de
SSH sur ce type d'app Docker (pas d'accès à `reset-password` CLI de Metabase). Récupéré en écrivant
directement un nouveau hash dans `core_user.password` (MySQL), format **bcrypt `$2a$`** sur
`password_salt + mot_de_passe` (vérifié en testant contre `POST /api/session` — un `$2b$` généré
par la lib Python par défaut ne fonctionne pas, Metabase attend `$2a$`). Nouveau mot de passe
transmis hors-repo (canal sûr) — ne pas le reset à nouveau sans nécessité, le compte a du contenu réel.

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
