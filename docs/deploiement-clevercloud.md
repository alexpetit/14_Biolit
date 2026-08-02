# Déploiement Clever Cloud - état & passation

> Doc de passation pour reprendre le travail (y compris depuis une autre machine).
> Branche cible de PR : `feat/clevercloud-deployment`.

## 1. Reprendre sur une nouvelle machine

```bash
# 1. Récupérer le code
git clone https://github.com/alexpetit/14_Biolit.git
cd 14_Biolit
git checkout feat/clevercloud-deployment

# 2. Outils
#   - Python 3.12 + uv        (https://docs.astral.sh/uv/)
#   - Node 18 + npm
#   - clever-tools :
npm install -g clever-tools
cd backend
uv sync                       # cree le venv local backend
cd ..

# 3. Auth Clever Cloud (ouvre le navigateur)
clever login

# 4. Lier le dépôt aux apps CC (recrée .clever.json, non versionné) :
clever link app_a9ba8ca0-1212-42ee-9dcf-1a17158fae08 --alias biolit-ingestion
clever link app_b338e77e-3b2c-46e9-a26f-50c0c95db8e4 --alias biolit-ls
clever link app_3048d33c-6af4-4548-8af6-803974dd14bc --alias biolit-mb
```

### Fichiers locaux NON versionnés

- `.env` ou variables Clever Cloud - partir de `backend/.env.example`, sans committer les secrets.
- `.clever.json` - régénéré par `clever link` et ignoré par Git.
- Dumps de migration - a poser localement dans `metabase/migration/` et
  `labelstudio/migration/`, ou a garder dans Cellar/sur un canal sur.

> Le déploiement passe par `clever deploy`, qui pousse le HEAD committé vers le Git Clever Cloud
> de l'app cible, pas vers GitHub.

## 2. Ce qui est déployé (compte perso — environnement de validation)

| App / add-on | ID / nom | Rôle |
|---|---|---|
| `biolit-ingestion` (Task Docker, scaler **M**) | `app_a9ba8ca0-1212-42ee-9dcf-1a17158fae08` | pipeline complet, run 100% vert |
| `biolit-label-studio` (web) | `app_b338e77e-3b2c-46e9-a26f-50c0c95db8e4` | validation technique, ancien essai LS 1.13.1 |
| `biolit-metabase` (web) | `app_3048d33c-6af4-4548-8af6-803974dd14bc` | validation technique, ancien essai Metabase v0.50.32 |
| add-on `biolit-pg` (PostgreSQL dev) | — | données biolit + LS |
| add-on `biolit-cellar` (Cellar S3) | — | images/crops/parquets + dumps migration |
| add-on `biolit-mb-mysql` (MySQL dev) | — | app-DB Metabase (contournement) |

Pour la production, Label Studio et Metabase restent en continu. L'app ingestion
est la seule a laisser arretee hors run hebdomadaire, puis a relancer avec
`clever restart --alias biolit-ingestion`.

Optimisations validées : image **torch CPU-only** (7,2→3,3 Go) + traits de côte clippés **France**
(1,24 Go→qq Mo) → l'ingestion tient dans un scaler **M**.

## 3. Cible finale : org Clever Cloud **Planète Mer**

`orga_33229b84-080b-48d1-98e4-7153d80e2e8d` (~200€ crédit, containers **M max**).
Rejouer les scripts sous `--org`, avec un **PostgreSQL dédié** pour Metabase
(le plan dev bloque `pg_database` pour les migrations Metabase).

**Décisions actées** : base « données » = **dev (gratuit)** ; base Metabase = **dédié xxs_sml**.

## 4. Migration depuis la prod (Coolify + infra d4g)

- **Metabase** : Coolify, version **v0.60.1.3** (`biolitmetabase.services.d4g.fr`).
  Dump `pg_dump -Fc` récupéré → **Cellar** `migration/pg-dump-metabase-1785271271.dmp`
  ou localement `metabase/migration/pg-dump-metabase-1785271271.dmp`.
  → Metabase Planète Mer est pinné sur `metabase/metabase:v0.60.1.3` avec base applicative
  PostgreSQL dédiée. Restaurer le dump avant le premier démarrage.
- **Label Studio** : infra d4g Scaleway (ansible `d4g-ansible/roles/services/biolit-labelstudio`),
  version **1.22.0** (`biolitlabel.services.dataforgood.fr`). Annotations dans le **PostgreSQL
  partagé** `shared-postgresql` (postgres:16, **volume persistant**), base `biolit`, réseau interne
  `d4g-internal` → **dump à faire côté serveur** (`pg_dump` de la base `biolit`) puis a poser dans
  `labelstudio/migration/`.
  → LS Planète Mer est pinné sur `heartexlabs/label-studio:1.22.0`.

Important : les apps Docker Clever Cloud ne montent pas de FS Bucket. Les "volumes" à récupérer
doivent être convertis en restauration PostgreSQL ou en objets Cellar/S3.

## 5. Blocages (côté humain) avant de continuer le déploiement

1. **Rôle** : l'utilisateur est **DEVELOPER** sur l'org Planète Mer → ne peut pas créer d'apps/add-ons.
   Demander à `administration@planetemer.org` une promotion **MANAGER/ADMIN**.
2. **Dump Label Studio** : à produire côté serveur d4g (voir §4).

## 6. Reste à faire (après déblocage)

1. Créer apps + add-ons dans l'org Planète Mer, appliquer `backend/init.sql`, créer bucket Cellar,
   uploader `doris_data.csv`, configurer env vars, déployer.
2. Restaurer les dumps : Metabase dans son PostgreSQL dédié, Label Studio dans le PostgreSQL
   cible LS. Voir `docs/clevercloud-apps.md`.
3. Cron hebdomadaire : secrets GitHub (`CLEVER_TOKEN`, `CLEVER_SECRET`,
   `CLEVER_INGESTION_APP_ID`) pour le workflow
   `.github/workflows/clevercloud-ingestion.yml`.
4. Vraies configs d'annotation Label Studio.
5. Appliquer la revue de code (voir `docs/revue-code.md`) après validation équipe.
