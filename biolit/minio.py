import requests
import pandas as pd
import structlog
import os
from dotenv import load_dotenv
from minio import Minio
from io import BytesIO
from sqlalchemy import create_engine


LOGGER = structlog.get_logger()
load_dotenv()


def get_biolit_df_from_db():
    try:
        engine = create_engine(os.environ["POSTGRES_URL"])

        query = "SELECT id_observation, photos FROM observations_biolit_api LIMIT 10"
        df = pd.read_sql(query, engine)

        LOGGER.info("biolit df uploaded", count=len(df))
        LOGGER.info("Colonnes df", values=list(df.columns))

        return df

    except Exception as e:
        LOGGER.error("Erreur lors de la récupération des données depuis PostgreSQL", error=str(e))
        return None


def _upload_photos_minio(df: pd.DataFrame):
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    # Config MinIO
    client = Minio(
        "minio:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    LOGGER.info("Connected to S3")

    bucket_name = "crops-data"

    for idx, row in df.iterrows():
        id_obs = row["id_observation"]
        url = row["photos"]

        filename = url.split("/")[-1]
        object_name = f"{id_obs}/{filename}"

        response = requests.get(url)

        if response.status_code == 200:
            data = BytesIO(response.content)

            client.put_object(
                bucket_name,
                object_name,
                data,
                length=len(response.content),
                content_type="image/jpeg"
            )

            LOGGER.info(f"Uploaded: {object_name}")
        else:
            LOGGER.info(f"Failed: {url}")