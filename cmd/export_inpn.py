import sys
from pathlib import Path

import polars as pl

_base_dir = str(Path(__file__).parent.parent)
if _base_dir not in sys.path:
    sys.path.insert(0, _base_dir)

if True:
    from biolit import DATADIR
    from biolit.observations import format_observations
    from biolit.taxref import format_taxref
    from biolit.visualisation.species_distribution import plot_species_distribution


def main():
    format_taxref()
    format_observations()
    biolit_df = pl.read_parquet(DATADIR / "biolit_valid_observations.parquet")
    plot_species_distribution(biolit_df, fn=DATADIR / "distribution_images.html")


if __name__ == "__main__":
    main()
