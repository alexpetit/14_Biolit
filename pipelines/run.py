from biolit.export_api import fetch_biolit_from_api, adapt_api_to_dataframe
from biolit.create_table import (
    get_engine,
    create_table,
    create_enriched_table,
    create_db_finale_table,
    create_taxonomy_queue_table,
    prepare_dataframe_for_postgres,
    prepare_db_finale_dataframe,
    insert_dataframe,
    insert_enriched_dataframe,
    insert_crops_dataframe,
    insert_no_crops_dataframe,
    insert_db_finale_dataframe,
    insert_taxonomy_queue_dataframe,
    load_observations_from_db_for_ML
)
from biolit.geoloc import geoloc_enrichie_data_biolit_db
from biolit.flow_gatekeeper import filter_observations_for_crop
from biolit.label_studio import (
    push_tasks_label_studio_no_crops,
    push_tasks_label_studio_crops,
    extract_crops_data_from_label_studio,
    extract_no_crops_data_from_label_studio
)
from biolit.s3 import _configure_s3cmd
from ml.crop_inference.predict import flow_ml_crops
from ml.classification.pipeline_classification import flow_ml_classification
import datetime
import structlog
import polars as pl
import subprocess
import tempfile
import os
from dotenv import load_dotenv

LOGGER = structlog.get_logger()
load_dotenv()


def check_file_exists_s3cmd(bucket_name: str, key: str) -> bool:
    """Verifie si un fichier existe sur S3 avec s3cmd."""
    try:
        host = _configure_s3cmd()
        s3_url = f"s3://{bucket_name}.{host}/{key}"
        result = subprocess.run(
            ["s3cmd", "ls", s3_url],
            capture_output=True,
            text=True
        )
        return result.returncode == 0 and s3_url in result.stdout
    except Exception as e:
        LOGGER.warning(f"Erreur verification fichier S3: {e}")
        return False


def read_file_s3cmd(bucket_name: str, key: str) -> bytes:
    """Lit un fichier depuis S3 avec s3cmd."""
    try:
        host = _configure_s3cmd()
        s3_url = f"s3://{bucket_name}.{host}/{key}"

        # Creer un fichier temporaire
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            subprocess.run(
                ["s3cmd", "get", s3_url, tmp_path],
                check=True,
                capture_output=True,
                text=True
            )
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    except Exception as e:
        LOGGER.error(f"Erreur lecture fichier S3: {e}")
        raise


def run_pipeline():
    dossier_inference = datetime.datetime.now().strftime("run_%Y%m%d_%H%M%S")
    LOGGER.info(dossier_inference)

    # -------------------------
    # 0. CONFIGURATION S3CMD POUR CLEVER CLOUD
    # -------------------------
    try:
        _configure_s3cmd()
        LOGGER.info("s3cmd configure avec succes pour Clever Cloud")
        s3_available = True
    except Exception as e:
        LOGGER.warning(f"Impossible de configurer s3cmd: {e}")
        s3_available = False

    # -------------------------
    # 0.5 VERIFIER CSV DORIS SUR CLEVER CLOUD S3
    # -------------------------
    if s3_available:
        bucket_name = "biolit-uploads"
        doris_key = "lien_doris/lien_doris.csv"

        # Verifier si le fichier existe deja sur S3
        if not check_file_exists_s3cmd(bucket_name, doris_key):
            LOGGER.warning(
                "Fichier DORIS introuvable sur S3 Clever Cloud. "
                "Le pipeline necessitera ce fichier pour l'enrichissement."
            )
        else:
            LOGGER.info("Fichier DORIS present sur S3 Clever Cloud")
    else:
        LOGGER.warning("s3cmd non disponible - verification DORIS skipped")

    # -------------------------
    # 1. INGESTION API
    # -------------------------
    LOGGER.info("Fetching data...")
    data = fetch_biolit_from_api()

    LOGGER.info("Transforming...")
    df = adapt_api_to_dataframe(data)

    LOGGER.info("Preparing for Postgres...")
    df = prepare_dataframe_for_postgres(df)

    LOGGER.info("Creating table if not exists...")
    create_table()

    LOGGER.info("Loading into Postgres...")
    insert_dataframe(df)

    # -------------------------
    # 2. ENRICHISSEMENT GEOLOC
    # -------------------------
    LOGGER.info("Starting geolocation enrichment...")
    engine = get_engine()
    df_geo = geoloc_enrichie_data_biolit_db(engine)

    LOGGER.info("Creating enriched table if not exists...")
    create_enriched_table(engine)

    LOGGER.info("Saving enriched data into Postgres...")
    insert_enriched_dataframe(df_geo, engine)
    LOGGER.info("Geoloc Enrichment DONE")

    # -------------------------
    # 3. FLOW ML CROPS
    # -------------------------
    LOGGER.info("Creating tables for ML if not exist...")
    create_db_finale_table(engine)
    create_taxonomy_queue_table(engine)

    LOGGER.info("Recuperation des donnees a traiter pour le ML")
    df_ml = load_observations_from_db_for_ML(engine)
    # On filtre le df avec toutes les images qui sont deja passees dans le flow
    df_ml_to_process = filter_observations_for_crop(df_ml, engine)
    nb_to_process = len(df_ml_to_process)

    LOGGER.info("Nombre d'observations a traiter", value=nb_to_process)

    if nb_to_process == 0:
        LOGGER.info("Aucune nouvelle observation a traiter -> arret du pipeline")
        return

    LOGGER.info("Lancement du Flow de ML Crop")
    config_name = "ml/crop_inference/config.yaml"
    df_crops, df_no_crops, crops_images = flow_ml_crops(
        df_ml_to_process, config_name, dossier_inference
    )
    LOGGER.info("Cropping des images realisees")
    LOGGER.info("Crops uploades sur S3")

    LOGGER.info("Enregistrement des observations traitees dans Postgres")
    insert_crops_dataframe(df_crops, engine)
    insert_no_crops_dataframe(df_no_crops, engine)
    LOGGER.info("Table de Crops et No Crops mises a jours")

    # -------------------------
    # 4. PASSAGE ML TAXONOMIE EXPORT VERS LABEL STUDIO
    # -------------------------

    # --- ENVOI DES NO CROPS VERS LABEL STUDIO ---
    if len(df_no_crops) > 0:
        LOGGER.info("Envoi des observations sans crops vers Label Studio...")
        push_tasks_label_studio_no_crops("Biolit No Crops", df_no_crops)
        LOGGER.info(
            f"{len(df_no_crops)} observations sans crops envoyees "
            "vers Label Studio"
        )
    else:
        LOGGER.info("Aucune observation sans crop a envoyer vers Label Studio")

    # --- ENVOI DES CROPS VERS LABEL STUDIO (avec classification taxonomique) ---
    if len(crops_images) > 0:
        LOGGER.info("Lancement du Flow de Classification Taxonomique")
        df_taxonomy = flow_ml_classification(crops_images, df_crops)

        # --- ENRICHISSEMENT AVEC LIENS DORIS (si disponible sur S3) ---
        if s3_available:
            try:
                bucket_name = "biolit-uploads"
                doris_key = "lien_doris/lien_doris.csv"

                if check_file_exists_s3cmd(bucket_name, doris_key):
                    csv_bytes = read_file_s3cmd(bucket_name, doris_key)
                    df_doris = pl.read_csv(csv_bytes)
                    LOGGER.info(f"Fichier DORIS charge: {len(df_doris)} especes")

                    # Normalisation des noms pour la jointure
                    df_doris = df_doris.with_columns(
                        pl.col("nom_scientifique").str.to_lowercase()
                    )
                    df_taxonomy = df_taxonomy.with_columns(
                        pl.col("species_name").str.to_lowercase()
                    )

                    # Enrichissement avec les liens Doris
                    df_taxonomy = df_taxonomy.join(
                        df_doris,
                        left_on="species_name",
                        right_on="nom_scientifique",
                        how="left"
                    )
                    LOGGER.info("Enrichissement Doris applique")
                else:
                    LOGGER.warning(
                        "Fichier DORIS introuvable sur S3 - "
                        "continuation sans enrichissement"
                    )
            except Exception as e:
                LOGGER.warning(f"Erreur enrichissement Doris: {e} - continuation sans")
        else:
            LOGGER.warning("s3cmd non disponible - enrichissement Doris skipped")

        df_taxonomy = df_taxonomy.with_columns(
            pl.col("id_observation").cast(pl.Int64)
        ).join(df_ml_to_process, on="id_observation")
        push_tasks_label_studio_crops("Biolit Crops", df_taxonomy)
        LOGGER.info("Classification taxonomique DONE")
    else:
        LOGGER.info("Aucun crop a classifier -> skip taxonomie")

    # -------------------------
    # 6. RECUPERATION DES INFOS DEPUIS LABEL STUDIO
    # -------------------------
    LOGGER.info("Recuperation des annotations realisees depuis le dernier run...")
    data_label_studio_crops = extract_crops_data_from_label_studio(
        "Biolit Crops", datetime.datetime(2025, 1, 1), datetime.datetime(2027, 1, 1)
    )
    LOGGER.info("Data collected from label studio projet Crops")
    data_label_studio_no_crops = extract_no_crops_data_from_label_studio(
        "Biolit No Crops", datetime.datetime(2025, 1, 1), datetime.datetime(2027, 1, 1)
    )
    LOGGER.info("Data collected from label studio projet No Crops")

    # Insertion des donnees recuperes dans les tables postgresql
    data_label_studio_crops_filtered = prepare_db_finale_dataframe(
        data_label_studio_crops
    )
    insert_db_finale_dataframe(data_label_studio_crops_filtered, engine)
    LOGGER.info(
        "Insertion db_finale terminee projet crops",
        rows_inserted=len(data_label_studio_crops_filtered)
    )
    data_label_studio_no_crops_filtered = prepare_db_finale_dataframe(
        data_label_studio_no_crops
    )
    LOGGER.info(
        "Insertion db_finale terminee projet no crops",
        rows_inserted=len(data_label_studio_no_crops_filtered)
    )
    insert_db_finale_dataframe(data_label_studio_no_crops_filtered, engine)

    # Enregistrement donnees de crops pour reentrainnement
    insert_taxonomy_queue_dataframe(data_label_studio_no_crops, engine)
    LOGGER.info(
        "Stockage donnees pour reentrainement projet, "
        "nombre de lignes stockees",
        rows_inserted=len(data_label_studio_no_crops)
    )

    # -------------------------
    # 7. CLEANING
    # -------------------------
    LOGGER.info("Cleaning des taches annotees depuis le precedent flow...")
    LOGGER.info("Cleaning du S3...")
    LOGGER.info("Cleaning de LabelStudio...")

    LOGGER.info("Fin du Flow: succes")


if __name__ == "__main__":
    run_pipeline()
