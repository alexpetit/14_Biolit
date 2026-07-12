import subprocess
import os
import structlog
from dotenv import load_dotenv
from pathlib import Path
import tempfile
from PIL import Image
from io import BytesIO

LOGGER = structlog.get_logger()
load_dotenv()

def upload_image_s3(bucket_name: str, key: str, file_path: str):
    """
    Upload une image vers Cellar (S3 Clever Cloud) avec s3cmd.
    :param bucket_name: Nom du bucket (ex: "biolit-uploads").
    :param key: Chemin relatif dans le bucket (ex: "run_20260712/no_crops/12345.jpg").
    :param file_path: Chemin absolu du fichier temporaire (ex: "/tmp/tmp12345.jpg").
    """
    # 1. Récupère les credentials Cellar
    access_key = os.getenv("CELLAR_ADDON_KEY_ID")
    secret_key = os.getenv("CELLAR_ADDON_KEY_SECRET")
    host = os.getenv("CELLAR_ADDON_HOST")  # Ex: "cellar-c2.services.clever-cloud.com"

    if not all([access_key, secret_key, host]):
        raise ValueError(
            "Missing Cellar credentials. "
            "Check: CELLAR_ADDON_KEY_ID, CELLAR_ADDON_KEY_SECRET, CELLAR_ADDON_HOST"
        )

    # 2. Crée le fichier de config s3cmd
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

    # 3. Upload avec s3cmd
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

def _check_file_existence_s3(client, bucket_name: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket_name, Key=key)
        LOGGER.info("File exists:", key=key)
        return True
    except Exception as e:
        LOGGER.info("File does not exist:", key=key, error=str(e))
        return False

def _read_file_s3(client, bucket_name: str, key: str) -> bytes:
    obj = client.get_object(Bucket=bucket_name, Key=key)
    LOGGER.info("Fichier Lu :", key=key)
    return obj["Body"].read()

def load_image_from_s3(s3_client, bucket_name: str, object_key: str) -> Image.Image:
    """Charge une image depuis S3/MinIO et retourne un PIL.Image."""
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    image_data = response["Body"].read()
    return Image.open(BytesIO(image_data)).convert("RGB")