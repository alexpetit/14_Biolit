import boto3
import structlog
import os
from dotenv import load_dotenv
from io import BytesIO
from PIL import Image
from botocore.client import Config


LOGGER = structlog.get_logger()
load_dotenv()

# =============================================
# UPLOADS BOTO3 (vers Cellar)
# =============================================

def upload_parquet_s3(client, df, bucket_name: str, object_name: str):
    """Upload un DataFrame Polars (Parquet) vers Cellar via boto3."""
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    client.put_object(
        Body=buffer,
        Bucket=bucket_name,
        Key=object_name,
        ContentLength=buffer.getbuffer().nbytes,
    )
    LOGGER.info("Parquet uploaded", path=f"s3://{bucket_name}/{object_name}")


def upload_image_s3(client, pil_img: Image.Image, bucket_name: str, object_name: str):
    """Upload une image PIL (JPEG) vers Cellar via boto3."""
    buffer = BytesIO()
    pil_img.save(buffer, format="JPEG")
    buffer.seek(0)
    client.put_object(
        Body=buffer,
        Bucket=bucket_name,
        Key=object_name,
        ContentType="image/jpeg",
        ContentLength=buffer.getbuffer().nbytes,
    )
    LOGGER.info("Image uploaded", key=object_name)

# =============================================
# FONCTIONS POUR BOTO3 (Checks/Lectures)
# =============================================

def create_s3_client():
    """
    Crée un client boto3 pour Cellar (utilisé pour les checks/lectures).
    """
    host = os.getenv("CELLAR_ADDON_HOST")
    key_id = os.getenv("CELLAR_ADDON_KEY_ID")
    key_secret = os.getenv("CELLAR_ADDON_KEY_SECRET")

    if not all([host, key_id, key_secret]):
        raise ValueError("Missing Cellar credentials for boto3 client")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{host}",
        aws_access_key_id=key_id,
        aws_secret_access_key=key_secret,
        region_name="fr-par",
        config=Config(
            signature_version="s3v4",
            # Cellar rejette l'encodage aws-chunked (checksum CRC32 par défaut
            # de botocore >=1.36) -> MissingContentLength. On le désactive.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
        verify=False
    )

def _check_file_existence_s3(client, bucket_name: str, key: str) -> bool:
    """Vérifie si un fichier existe dans S3 avec boto3."""
    try:
        client.head_object(Bucket=bucket_name, Key=key)
        LOGGER.info("File exists:", key=key)
        return True
    except Exception as e:
        LOGGER.info("File does not exist:", key=key, error=str(e))
        return False

def _read_file_s3(client, bucket_name: str, key: str) -> bytes:
    """Lit un fichier depuis S3 avec boto3."""
    obj = client.get_object(Bucket=bucket_name, Key=key)
    LOGGER.info("Fichier Lu :", key=key)
    return obj["Body"].read()

def load_image_from_s3(s3_client, bucket_name: str, object_key: str) -> Image.Image:
    """Charge une image depuis S3 et retourne un PIL.Image."""
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    image_data = response["Body"].read()
    return Image.open(BytesIO(image_data)).convert("RGB")