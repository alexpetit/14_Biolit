from biolit.export_api import fetch_biolit_from_api, adapt_api_to_dataframe
from biolit.postgres import (
    prepare_dataframe_for_postgres,
    insert_dataframe,
    get_engine,
    insert_enriched_dataframe,
    create_table,
    create_enriched_table,
)
from biolit.geoloc import geoloc_enrichie_data_biolit_db
import structlog
from dotenv import load_dotenv
load_dotenv()


LOGGER = structlog.get_logger()
load_dotenv()

def run_pipeline():
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

    LOGGER.info("DONE ✅")

    # -------------------------
    # 3. INSERTION DES IMAGES DANS MINIO
    # -------------------------


if __name__ == "__main__":
    run_pipeline()