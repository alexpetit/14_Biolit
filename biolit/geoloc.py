import boto3
import structlog
import os
from dotenv import load_dotenv
from io import BytesIO
from PIL import Image
import subprocess
import tempfile
from botocore.client import Config

LOGGER = structlog.get_logger()
load_dotenv()

# =============================================
# FONCTIONS POUR S3CMD (Uploads vers Cellar)
# =============================================

def _configure_s3cmd():
    """Configure s3cmd avec les variables Clever Cloud."""
    access_key = os.getenv("CELLAR_ADDON_KEY_ID")
    secret_key = os.getenv("CELLAR_ADDON_KEY_SECRET")
    host = os.getenv("CELLAR_ADDON_HOST")

    if not all([access_key, secret_key, host]):
        raise ValueError("Missing Cellar credentials: CELLAR_ADDON_KEY_ID, CELLAR_ADDON_KEY_SECRET, CELLAR_ADDON_HOST")

    s3cfg_path = "/root/.s3cfg"
    os.makedirs(os.path.dirname(s3cfg_path), exist_ok=True)
    with open(s3cfg_path, "w") as f:
        f.write(f"""[default]
access_key = {access_key}
secret_key = {secret_key}
host_base = {host}
host_bucket = %(bucket)s.{host}
use_https = True
""")
    return host

def upload_to_s3_with_s3cmd(df, bucket_name: str, key: str):
    """
    Upload un DataFrame (Parquet) vers Cellar avec s3cmd.
    """
    host = _configure_s3cmd()
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        df.to_parquet(tmp_path)

    try:
        s3_url = f"s3://{bucket_name}.{host}/{key}"
        result = subprocess.run(
            ["s3cmd", "put", tmp_path, s3_url],
            check=True,
            capture_output=True,
            text=True
        )
        LOGGER.info(f"✅ Upload réussi: {s3_url}")
    except subprocess.CalledProcessError as e:
        LOGGER.error(f"❌ Erreur s3cmd: {e.stderr}")
        raise
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def upload_image_s3(bucket_name: str, key: str, file_path: str):
    """
    Upload une IMAGE vers Cellar avec s3cmd.
    """
    host = _configure_s3cmd()
    s3_url = f"s3://{bucket_name}.{host}/{key}"
    try:
        result = subprocess.run(
            ["s3cmd", "put", file_path, s3_url],
            check=True,
            capture_output=True,
            text=True
        )
        LOGGER.info(f"✅ Upload réussi: {s3_url}")
    except subprocess.CalledProcessError as e:
        LOGGER.error(f"❌ Erreur s3cmd: {e.stderr}")
        raise

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
        config=Config(signature_version="s3v4"),
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