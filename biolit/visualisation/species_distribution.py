from pathlib import Path

import matplotlib as mpl
import plotly.graph_objects as go
import polars as pl
from polars import col

from biolit import DATADIR
from biolit.taxref import TAXREF_HIERARCHY

COLOR_MATCHING = {
    i: f"rgb({', '.join(str(int(x * 255)) for x in mpl.colormaps['tab10'](i)[:3])})"
    for i in range(20)
}


def _species_colors(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame["regne"]
        .unique()
        .sort()
        .to_frame()
        .with_row_index("color")
        .with_columns(col("color").replace_strict(COLOR_MATCHING))
    )


def plot_species_distribution(frame: pl.DataFrame, fn: Path):
    colors = _species_colors(frame)
    species_counts = (
        frame.filter(col("cd_nom").is_not_null())
        .group_by(["nom_scientifique", "cd_nom"] + TAXREF_HIERARCHY)
        .agg(col("id").count())
        .join(colors, on="regne")
    )

    edges = _baseline_edges(species_counts)
    nodes = nodes_from_edges(edges)
    edges = enrich_edges(edges, nodes)
    edges.write_parquet(DATADIR / "species_edges.parquet")
    nodes.write_parquet(DATADIR / "species_node.parquet")
    save_sankey_plot(edges, nodes, fn)


def save_sankey_plot(edges: pl.DataFrame, nodes: pl.DataFrame, fn: Path) -> Path:
    _data = go.Sankey(
        link=edges.to_dict(as_series=False),
        node=nodes.select("label", "color", "customdata").to_dict(as_series=False)
        | {
            "line": dict(color="lightgrey", width=0.1),
            "hovertemplate": "<b>%{customdata.name}</b><br>"
            "node_id: %{customdata.node_id}<br>"
            "# images: %{value}<br>"
            "# sub level: %{customdata.n_incoming}<br>"
            "# species: %{customdata.n_species}<br>"
            "<extra></extra>",
        },
    )

    _fig = go.Figure(_data)
    _fig.update_layout(
        autosize=False,
        width=1000,
        height=1500,
        title_text="Répartition des images Biolit en selon les différentes strates de la hierarchie",
        font_size=10,
    )
    _fig.write_html(fn)


def _baseline_edges(species_counts: pl.DataFrame) -> pl.DataFrame:
    _edges = []

    _steps = ["nom_scientifique"] + TAXREF_HIERARCHY[:-1][::-1]
    for _source, _target in zip(_steps, _steps[1:]):
        tmp = (
            species_counts.group_by(_source, _target)
            .agg(
                col("id").sum(),
                col("id").count().alias("n_species"),
                col("color").first(),
            )
            .rename({_source: "source", _target: "target", "id": "value"})
        )
        _edges.append(tmp)
    return pl.concat(_edges)


def nodes_from_edges(edges: pl.DataFrame) -> pl.DataFrame:
    has_labels = _node_has_labels(edges)
    return (
        pl.concat([edges["source"], edges["target"]])
        .unique()
        .sort()
        .to_frame()
        .with_row_index("id")
        .with_columns(col("id") - 1)
        .join(has_labels, left_on="source", right_on="target")
        .with_columns(
            pl.when(col("has_label")).then(col("source")).alias("label"),
            pl.when(col("has_label"))
            .then(pl.lit("blue"))
            .otherwise(pl.lit("lightgrey"))
            .alias("color"),
            pl.struct(
                name=col("source"),
                n_incoming=col("n_incoming"),
                n_species=col("n_species"),
                node_id=col("id"),
            ).alias("customdata"),
        )
    )


def _node_has_labels(edges: pl.DataFrame) -> pl.DataFrame:
    return (
        edges.group_by("target")
        .agg(
            col("value").sum(),
            col("source").count().alias("n_incoming"),
            col("n_species").sum(),
        )
        .with_columns(
            (col("value") > 300).alias("has_label"),
            col("target").str.count_matches("|", literal=True).alias("n_levels"),
        )
    )


def enrich_edges(edges: pl.DataFrame, nodes: pl.DataFrame) -> pl.DataFrame:
    _sub_nodes = nodes.select("id", "source")
    return (
        edges.select("source", "target", "value", "color")
        .join(_sub_nodes, left_on="source", right_on="source")
        .join(_sub_nodes, left_on="target", right_on="source")
        .drop("target", "source")
        .rename({"id": "source", "id_right": "target"})
        .sort("source", "target")
    )
