import os
import polars as pl
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


# -------------------------
# Connexion DB
# -------------------------

def get_engine():
    postgres_url = os.getenv("POSTGRES_URL")

    if not postgres_url:
        raise ValueError("Missing POSTGRES_URL")

    return create_engine(postgres_url)


# -------------------------
# Préparation des données
# -------------------------
def prepare_dataframe_for_postgres(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([

        # -------------------------
        # IDs
        # -------------------------
        pl.col("id_observation")
        .cast(pl.Int64),

        pl.col("id_espece")
        .cast(pl.Float64, strict=False)
        .fill_nan(None)
        .cast(pl.Int64, strict=False),

        pl.col("categorie_programme")
        .cast(pl.Float64, strict=False)
        .fill_nan(None)
        .cast(pl.Int64, strict=False),

        pl.col("relais")
        .cast(pl.Utf8)
        .replace("", None)
        .cast(pl.Float64, strict=False)
        .fill_nan(None)
        .cast(pl.Int64, strict=False),

        # -------------------------
        # Coordonnées
        # -------------------------
        pl.col("latitude")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Float64, strict=False),

        pl.col("longitude")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Float64, strict=False),

        # -------------------------
        # Dates
        # -------------------------
        pl.col("date_observation")
        .str.strptime(pl.Datetime, strict=False),

        pl.col("heure_debut")
        .str.strptime(pl.Time, strict=False),

        pl.col("heure_fin")
        .str.strptime(pl.Time, strict=False),
    ])

# -------------------------
# Insert avec sécurité (UPSERT)
# -------------------------

def insert_dataframe(df: pl.DataFrame):
    engine = get_engine()

    rows = df.to_dicts()

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
                INSERT INTO observations (
                    id_observation,
                    date_observation,
                    lien_observation,
                    observateur,
                    url_sortie,
                    espece_identifiee,
                    heure_debut,
                    heure_fin,
                    latitude,
                    longitude,
                    photos,
                    relais,
                    id_espece,
                    nom_scientifique,
                    nom_commun,
                    categorie_programme,
                    programme
                ) VALUES (
                    :id_observation,
                    :date_observation,
                    :lien_observation,
                    :observateur,
                    :url_sortie,
                    :espece_identifiee,
                    :heure_debut,
                    :heure_fin,
                    :latitude,
                    :longitude,
                    :photos,
                    :relais,
                    :id_espece,
                    :nom_scientifique,
                    :nom_commun,
                    :categorie_programme,
                    :programme
                )
                ON CONFLICT (id_observation) DO NOTHING
            """), row)

def insert_enriched_dataframe(df: pd.DataFrame, engine):
    pl_df = pl.from_pandas(df)
    rows = pl_df.to_dicts()

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
                INSERT INTO observations_enriched (
                    id_observation,
                    nearest_commune,
                    code_insee,
                    distance_commune_m,
                    code_postal,
                    reg_nom,
                    dep_nom,
                    distance_to_coast,
                    is_coastal
                ) VALUES (
                    :id_observation,
                    :nearest_commune,
                    :code_insee,
                    :distance_commune_m,
                    :code_postal,
                    :reg_nom,
                    :dep_nom,
                    :distance_to_coast,
                    :is_coastal
                )
                ON CONFLICT (id_observation) DO NOTHING
            """), row)










def load_observations_from_db(engine) -> pl.DataFrame:
    query = """
        SELECT *
        FROM observations
    """

    return pl.read_database(query, engine)