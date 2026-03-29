import yaml
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import get_logger

logger = get_logger("build_dataset")
def load_config(path: str = "configs/build_dataset.yaml") -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    logger.info(f"Config chargée : {path}")
    return cfg

# CHARGEMENT DU YAML
cfg = load_config()
DATA_PATH    = cfg["data_path"]
BASE_DIR     = Path(cfg["base_dir"])
IMAGES_DIR   = Path(cfg["images_dir"])
TIMEOUT      = cfg["timeout"]
MAX_WORKERS  = cfg["max_workers"]
VALID_EXTS   = set(cfg["valid_extensions"])
USE_COLS     = list(cfg["columns"].values())


# PIPELINE TELECHARGEMENT DATASET
def load(path: str) -> pd.DataFrame:
    """Charge le CSV, garde uniquement les observations validées et identifiables."""
    logger.info("Chargement : %s", path)
    df = pd.read_csv(path, usecols=USE_COLS)

    df = df[
        (df["validee - observation"] == "TRUE")
        # (df["espece identifiable ? - observation"] == "Identifiable")
    ].reset_index(drop=True)

    logger.info("%d observations valides et identifiables", len(df))
    return df


def explode_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par image."""
    df = (
        df.assign(**{"images - observation": df["images - observation"].str.strip().str.split("|")})
        .explode("images - observation")
        .rename(columns={"images - observation": "image_url"})
        .assign(image_url=lambda d: d["image_url"].str.strip())
    )
    # Supprimer les URLs vides ou nulles générées par les trailing "|" ou cellules vides
    df = df[df["image_url"].notna() & (df["image_url"] != "")].reset_index(drop=True)
    logger.info("%d URLs d'images après explosion", len(df))
    return df


def make_filename(row: pd.Series, idx: int) -> str:
    id_n1 = str(row["ID - N1"])
    nom   = str(row["Nom commun - observation"]) if pd.notna(row["Nom commun - observation"]) else "inconnu"
    nom   = nom.strip().replace(" ", "_").replace("/", "-")
    # Garde-fou au cas où une URL NaN passerait quand même
    url   = row["image_url"] if isinstance(row["image_url"], str) else ""
    ext   = url.split(".")[-1].split("?")[0].lower() if url else "jpg"
    ext   = ext if ext in VALID_EXTS else "jpg"
    return f"{id_n1}_{nom}_{idx}.{ext}"


def _download_one(row: pd.Series, idx: int) -> dict | None:
    """Télécharge une image. Retourne un dict si succès, None si échec."""
    filename = make_filename(row, idx)
    dest     = IMAGES_DIR / filename

    if dest.exists():
        logger.debug("Skip (déjà présente) : %s", filename)
        return {**row, "filename": filename, "filepath": str(dest)}

    try:
        r = requests.get(row["image_url"], timeout=TIMEOUT)
        r.raise_for_status()
        dest.write_bytes(r.content)
        logger.debug("OK  %s", filename)
        return {**row, "filename": filename, "filepath": str(dest)}
    except Exception as e:
        logger.warning("ÉCHEC  %s  →  %s", row["image_url"], e)
        return None


def download(df: pd.DataFrame) -> pd.DataFrame:
    """
    Télécharge chaque image en parallèle (ThreadPoolExecutor, I/O-bound).
    Retourne uniquement les lignes dont le téléchargement a réussi.
    """
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    futures = {}
    rows_ok = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for idx, row in df.iterrows():
            future = executor.submit(_download_one, row, idx)
            futures[future] = idx

        for future in tqdm(as_completed(futures), total=len(futures), desc="Téléchargement"):
            result = future.result()
            if result is not None:
                rows_ok.append(result)

    downloaded = pd.DataFrame(rows_ok).reset_index(drop=True)
    logger.info(
        "Téléchargement terminé — %d/%d images récupérées",
        len(downloaded), len(df),
    )
    return downloaded


# MAIN
def build_dataset():
    logger.info("════════ START ════════")

    df = load(DATA_PATH)
    df = explode_urls(df)
    df_ok = download(df)

    logger.info(
        "Terminé — %d images | %d espèces | dossier : %s",
        len(df_ok),
        df_ok["Nom commun - observation"].nunique(),
        IMAGES_DIR.resolve(),
    )
    logger.info("════════ END ════════")


if __name__ == "__main__":
    build_dataset()
