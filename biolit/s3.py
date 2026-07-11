import boto3
import structlog
import os
from dotenv import load_dotenv
import botocore.exceptions
from botocore.exceptions import ClientError
from io import BytesIO
from PIL import Image

LOGGER = structlog.get_logger()
load_dotenv()


def test_permissions(bucket_name):

    ACCESS_KEY = os.getenv("aws_access_key_id")
    SECRET_KEY = os.getenv("aws_secret_access_key")

    ENDPOINT_URL = "https://s3.fr-par.scw.cloud"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

    LOGGER.info(f"\n🔍 Bucket: {bucket_name}")

    # 1. List bucket
    try:
        s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        LOGGER.info("✅ ListBucket: OK")
    except ClientError as e:
        LOGGER.info(f"❌ ListBucket: {e.response['Error']['Code']}")

    try:
        s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello")
        LOGGER.info("✅ PutObject (write): OK")
    except ClientError as e:
        LOGGER.info(f"❌ PutObject: {e.response['Error']['Code']}")

    try:
        s3.get_object(Bucket=bucket_name, Key="test.txt")
        LOGGER.info("✅ GetObject: OK")
    except ClientError as e:
        LOGGER.info(f"❌ GetObject: {e.response['Error']['Code']}")

    try:
        s3.delete_object(Bucket=bucket_name, Key="test.txt")
        LOGGER.info("✅ DeleteObject: OK")
    except ClientError as e:
        LOGGER.info(f"❌ DeleteObject: {e.response['Error']['Code']}")

def create_s3_client():
    # Priorité absolue à Cellar (add-on Clever Cloud)
    # Cellar injecte ces variables automatiquement quand l'add-on est lié
    CELLAR_HOST = os.getenv("CELLAR_ADDON_HOST")  # Ex: cellar-c2.services.clever-cloud.com
    CELLAR_KEY_ID = os.getenv("CELLAR_ADDON_KEY_ID")
    CELLAR_KEY_SECRET = os.getenv("CELLAR_ADDON_KEY_SECRET")

    # Si Cellar est configuré, on l'utilise en priorité
    if CELLAR_HOST and CELLAR_KEY_ID and CELLAR_KEY_SECRET:
        # Cellar nécessite l'URL complète avec https://
        endpoint_url = f"https://{CELLAR_HOST}"
        return boto3.client(
            "s3",
            aws_access_key_id=CELLAR_KEY_ID,
            aws_secret_access_key=CELLAR_KEY_SECRET,
            endpoint_url=endpoint_url,
            # Configuration spécifique pour Cellar/Clever Cloud
            region_name="fr-par",  # Région par défaut pour Clever Cloud
            # Désactive la vérification SSL si nécessaire (pour les endpoints locaux)
            verify=False,  # À décommenter si SSL pose problème
        )
        LOGGER.info("Using Cellar S3 client (Clever Cloud)") 

    # Fallback vers AWS/MinIO si Cellar n'est pas disponible
    ACCESS_KEY = (
        os.getenv("AWS_ACCESS_KEY_ID")
        or os.getenv("aws_access_key_id")
    )
    SECRET_KEY = (
        os.getenv("AWS_SECRET_ACCESS_KEY")
        or os.getenv("aws_secret_access_key")
    )
    ENDPOINT_URL = (
        os.getenv("AWS_S3_ENDPOINT")
        or os.getenv("aws_url")
    )

    if ENDPOINT_URL and not ENDPOINT_URL.startswith(("http://", "https://")):
        ENDPOINT_URL = f"https://{ENDPOINT_URL}"

    return boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

def upload_parquet_s3(client, df, bucket_name: str, object_name: str):
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

def _check_file_existence_s3(client, bucket_name: str, key: str) -> bool:
    try:
        client.head_object(Bucket= bucket_name, Key = key)
        LOGGER.info("File exists:", key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            LOGGER.info("The File does not exist:", key=key)
        elif e.response['Error']['Code'] == "403":
            LOGGER.info("Vous n'avez pas les bonnes clés d'accès au S3.")
        else:
            LOGGER.info("Autre erreur", value= e.response['Error']['Code'])
        return False

def _read_file_s3(client, bucket_name: str, key: str) -> bytes:
    obj = client.get_object(Bucket=bucket_name, Key=key)
    LOGGER.info("Fichier Lu :", key=key)
    return obj["Body"].read()

def upload_image_s3(client, pil_img: Image.Image, bucket_name: str, object_name: str):
    buffer = BytesIO()
    pil_img.save(buffer, format="JPEG")
    buffer.seek(0)

    client.put_object(
        Body=buffer,
        Bucket=bucket_name,
        Key=object_name,
        ContentEncoding="image/jpeg",
        ContentLength=buffer.getbuffer().nbytes
    )

def load_image_from_s3(s3_client,
                       bucket_name:str,
                       object_key:str)->Image.Image:
    """
    Charge une image depuis S3/MinIO
    et retourne un objet PIL.Image.
    """
    response=s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )

    image_data=response["Body"].read()

    image=Image.open(BytesIO(image_data)).convert("RGB")

    return image
