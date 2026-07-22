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
from biolit.s3 import create_s3_client
from ml.crop_inference.predict import flow_ml_crops
from ml.classification.pipeline_classification import flow_ml_classification
import datetime
import io
import structlog
import polars as pl
from dotenv import load_dotenv

LOGGER = structlog.get_logger()
load_dotenv()


def run_pipeline():
    dossier_inference = datetime.datetime.now().strftime("run_%Y%m%d_%H%M%S")
    LOGGER.info(dossier_inference)

    # -------------------------
    # 0. VERIFICATION DU FICHIER DORIS SUR S3 (Cellar)
    # -------------------------
    try:
        create_s3_client().head_object(
            Bucket="biolit-uploads", Key="doris_data.csv"
        )
        LOGGER.info("Fichier DORIS present sur S3 (doris_data.csv)")
    except Exception:
        LOGGER.warning(
            "Fichier DORIS (doris_data.csv) introuvable sur S3 - "
            "l'enrichissement DORIS sera ignore"
        )

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
        # df_no_crops ne contient que run_name/id_observation/path_s3 :
        # jointure avec df_ml_to_process pour récupérer relais, reg_nom,
        # nearest_commune, dep_nom, latitude, longitude (attendus par LS)
        df_no_crops = df_no_crops.with_columns(
            pl.col("id_observation").cast(pl.Int64)
        ).join(df_ml_to_process, on="id_observation")
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

        # --- ENRICHISSEMENT AVEC LIENS DORIS (Parquet sur Cellar via boto3) ---
        try:
            bucket_name = "biolit-uploads"
            doris_key = "doris_data.csv"

            doris_bytes = create_s3_client().get_object(
                Bucket=bucket_name, Key=doris_key
            )["Body"].read()
            df_doris = pl.read_parquet(io.BytesIO(doris_bytes))
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
        except Exception as e:
            LOGGER.warning(f"Erreur enrichissement Doris: {e} - continuation sans")

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
