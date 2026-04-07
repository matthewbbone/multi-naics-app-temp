"""Streamlit app for comparing Amazon location-level V0 and V1 labeling."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

WORKBOOK_PATH = Path("multi-naics-review.xlsx")
COMPANY_NAME = "Amazon"
V0_SHEET_NAME = "multi-naics-0"
V1_SHEET_NAME = "multi-naics-1"
V0_SHEET_ALIASES = (V0_SHEET_NAME, "multi-naics V0")
V1_SHEET_ALIASES = (V1_SHEET_NAME, "multi-naics V1")

V0_REQUIRED_COLUMNS = {
    "COMPANY",
    "BGI_CITY",
    "BGI_STATE",
    "BGI_COUNTRY",
    "PRIMARY_ESTABLISHMENT_NAICS6_NAME",
    "SECONDARY_ESTABLISHMENT_NAICS6_NAME",
    "TERTIARY_ESTABLISHMENT_NAICS6_NAME",
    "POSTINGS_COUNT",
}
V1_REQUIRED_COLUMNS = {
    "COMPANY",
    "BGI_CITY",
    "BGI_STATE",
    "BGI_COUNTRY",
    "ESTABLISHMENT_NAME",
    "ESTABLISHMENT_ADDRESS",
    "ESTABLISHMENT_NAICS6_NAME",
    "POSTINGS_COUNT",
}
TOP3_COLUMNS = [
    "PRIMARY_ESTABLISHMENT_NAICS6_NAME",
    "SECONDARY_ESTABLISHMENT_NAICS6_NAME",
    "TERTIARY_ESTABLISHMENT_NAICS6_NAME",
]


def normalize_text(value: object) -> str:
    """Return a stripped string for a scalar workbook value."""
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def build_location_key(city: object, state: object, country: object) -> str:
    """Build a normalized location key from city/state/country."""
    return "||".join(
        [
            normalize_text(city).casefold(),
            normalize_text(state).casefold(),
            normalize_text(country).casefold(),
        ],
    )


def build_location_label(city: object, state: object, country: object) -> str:
    """Build a human-readable location label from city/state/country."""
    parts = [normalize_text(part) for part in (city, state, country) if normalize_text(part)]
    return ", ".join(parts)


def validate_sheet_columns(df: pd.DataFrame, required_columns: set[str], sheet_name: str) -> None:
    """Raise a clear error when a workbook sheet is missing required columns."""
    missing = sorted(required_columns.difference(df.columns))
    if missing:
        raise ValueError(
            f"Sheet '{sheet_name}' is missing required columns: {', '.join(missing)}",
        )


def add_location_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Add normalized location key and display label columns."""
    result = df.copy()
    result["LOCATION_KEY"] = result.apply(
        lambda row: build_location_key(row["BGI_CITY"], row["BGI_STATE"], row["BGI_COUNTRY"]),
        axis=1,
    )
    result["LOCATION_LABEL"] = result.apply(
        lambda row: build_location_label(row["BGI_CITY"], row["BGI_STATE"], row["BGI_COUNTRY"]),
        axis=1,
    )
    return result


def prepare_v0_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Validate, filter, and normalize the V0 workbook sheet."""
    validate_sheet_columns(df, V0_REQUIRED_COLUMNS, V0_SHEET_NAME)
    result = df[df["COMPANY"].astype(str).str.casefold() == COMPANY_NAME.casefold()].copy()
    result = add_location_metadata(result)
    result["POSTINGS_COUNT"] = pd.to_numeric(result["POSTINGS_COUNT"], errors="coerce").fillna(0).astype(int)
    return result.sort_values(["LOCATION_LABEL"]).reset_index(drop=True)


def prepare_v1_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Validate, filter, and normalize the V1 workbook sheet."""
    validate_sheet_columns(df, V1_REQUIRED_COLUMNS, V1_SHEET_NAME)
    result = df[df["COMPANY"].astype(str).str.casefold() == COMPANY_NAME.casefold()].copy()
    result = add_location_metadata(result)
    if "ESTABLISHMENT_NAICS6" not in result.columns:
        result["ESTABLISHMENT_NAICS6"] = ""
    result["ESTABLISHMENT_NAICS6"] = result["ESTABLISHMENT_NAICS6"].apply(normalize_text)
    result["POSTINGS_COUNT"] = pd.to_numeric(result["POSTINGS_COUNT"], errors="coerce").fillna(0).astype(int)
    return result.sort_values(
        ["LOCATION_LABEL", "ESTABLISHMENT_NAME", "ESTABLISHMENT_ADDRESS"],
    ).reset_index(drop=True)


def aggregate_v1_locations(v1_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate V1 establishment rows into one row per location."""
    location_rows: list[dict[str, Any]] = []

    group_columns = [
        "LOCATION_KEY",
        "LOCATION_LABEL",
        "BGI_CITY",
        "BGI_STATE",
        "BGI_COUNTRY",
    ]
    for group_key, group in v1_df.groupby(group_columns, dropna=False, sort=True):
        naics_counts = (
            group.groupby(
                ["ESTABLISHMENT_NAICS6_NAME", "ESTABLISHMENT_NAICS6"],
                dropna=False,
            )
            .agg(
                ESTABLISHMENT_COUNT=("ESTABLISHMENT_NAME", "size"),
                POSTINGS_COUNT=("POSTINGS_COUNT", "sum"),
            )
            .reset_index()
            .sort_values(
                ["ESTABLISHMENT_COUNT", "POSTINGS_COUNT", "ESTABLISHMENT_NAICS6_NAME", "ESTABLISHMENT_NAICS6"],
                ascending=[False, False, True, True],
            )
            .reset_index(drop=True)
        )
        top_row = naics_counts.iloc[0]
        establishments = (
            group[
                [
                    "ESTABLISHMENT_NAME",
                    "ESTABLISHMENT_ADDRESS",
                    "ESTABLISHMENT_NAICS6",
                    "ESTABLISHMENT_NAICS6_NAME",
                    "POSTINGS_COUNT",
                ]
            ]
            .sort_values(
                [
                    "ESTABLISHMENT_NAICS6_NAME",
                    "ESTABLISHMENT_NAME",
                    "ESTABLISHMENT_ADDRESS",
                ],
            )
            .to_dict(orient="records")
        )
        naics_frequency_map = {
            normalize_text(name): int(count)
            for name, count in zip(
                naics_counts["ESTABLISHMENT_NAICS6_NAME"],
                naics_counts["ESTABLISHMENT_COUNT"],
                strict=False,
            )
        }
        total_establishments = int(len(group))
        total_postings = int(group["POSTINGS_COUNT"].sum())
        location_postings = int(group["POSTINGS_COUNT"].max())
        top_count = int(top_row["ESTABLISHMENT_COUNT"])

        location_rows.append(
            {
                "LOCATION_KEY": group_key[0],
                "LOCATION_LABEL": group_key[1],
                "BGI_CITY": group_key[2],
                "BGI_STATE": group_key[3],
                "BGI_COUNTRY": group_key[4],
                "ESTABLISHMENT_COUNT": total_establishments,
                "POSTINGS_COUNT": total_postings,
                "LOCATION_POSTINGS_COUNT": location_postings,
                "DISTINCT_NAICS_COUNT": int(naics_counts.shape[0]),
                "TOP_V1_NAICS_CODE": normalize_text(top_row["ESTABLISHMENT_NAICS6"]),
                "TOP_V1_NAICS_NAME": normalize_text(top_row["ESTABLISHMENT_NAICS6_NAME"]),
                "TOP_V1_NAICS_SHARE": top_count / total_establishments,
                "TOP_V1_NAICS_POSTINGS_COUNT": int(top_row["POSTINGS_COUNT"]),
                "NAICS_FREQUENCY_MAP": naics_frequency_map,
                "ESTABLISHMENT_ROWS": establishments,
            },
        )

    return pd.DataFrame(location_rows).sort_values(["LOCATION_LABEL"]).reset_index(drop=True)


def build_comparison_tables(
    v0_df: pd.DataFrame,
    v1_location_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build overlap and coverage comparison tables for V0 versus V1."""
    v0_locations = v0_df[
        [
            "LOCATION_KEY",
            "LOCATION_LABEL",
            "BGI_CITY",
            "BGI_STATE",
            "BGI_COUNTRY",
            "POSTINGS_COUNT",
            *TOP3_COLUMNS,
        ]
    ].rename(columns={"POSTINGS_COUNT": "V0_POSTINGS_COUNT"}).copy()

    comparison_df = v0_locations.merge(
        v1_location_df[
            [
                "LOCATION_KEY",
                "TOP_V1_NAICS_CODE",
                "TOP_V1_NAICS_NAME",
                "TOP_V1_NAICS_SHARE",
                "ESTABLISHMENT_COUNT",
                "POSTINGS_COUNT",
                "LOCATION_POSTINGS_COUNT",
                "DISTINCT_NAICS_COUNT",
                "NAICS_FREQUENCY_MAP",
                "ESTABLISHMENT_ROWS",
            ]
        ],
        on="LOCATION_KEY",
        how="inner",
    )
    comparison_df["MATCH_PRIMARY"] = (
        comparison_df["TOP_V1_NAICS_NAME"].fillna("").astype(str)
        == comparison_df["PRIMARY_ESTABLISHMENT_NAICS6_NAME"].fillna("").astype(str)
    )
    comparison_df["MATCH_ANY_TOP3"] = comparison_df.apply(
        lambda row: normalize_text(row["TOP_V1_NAICS_NAME"])
        in {
            normalize_text(row[column])
            for column in TOP3_COLUMNS
            if normalize_text(row[column])
        },
        axis=1,
    )
    comparison_df = comparison_df.sort_values(["LOCATION_LABEL"]).reset_index(drop=True)

    v0_only_df = v0_locations[~v0_locations["LOCATION_KEY"].isin(v1_location_df["LOCATION_KEY"])].copy()
    v0_only_df = v0_only_df.sort_values(["LOCATION_LABEL"]).reset_index(drop=True)

    v1_only_df = v1_location_df[~v1_location_df["LOCATION_KEY"].isin(v0_locations["LOCATION_KEY"])].copy()
    v1_only_df = v1_only_df.sort_values(["LOCATION_LABEL"]).reset_index(drop=True)

    return comparison_df, v0_only_df, v1_only_df


def build_review_payload(v0_raw: pd.DataFrame, v1_raw: pd.DataFrame) -> dict[str, Any]:
    """Prepare all normalized dataframes and summary metrics for the app."""
    v0_df = prepare_v0_sheet(v0_raw)
    v1_df = prepare_v1_sheet(v1_raw)
    v1_location_df = aggregate_v1_locations(v1_df)
    comparison_df, v0_only_df, v1_only_df = build_comparison_tables(v0_df, v1_location_df)

    metrics = {
        "v0_rows": int(len(v0_df)),
        "v1_rows": int(len(v1_df)),
        "v0_postings": int(v0_df["POSTINGS_COUNT"].sum()),
        "v1_postings": int(v1_df["POSTINGS_COUNT"].sum()),
        "v0_unique_locations": int(v0_df["LOCATION_KEY"].nunique()),
        "v1_unique_locations": int(v1_location_df["LOCATION_KEY"].nunique()),
        "overlap_locations": int(len(comparison_df)),
        "v0_only_locations": int(len(v0_only_df)),
        "v1_only_locations": int(len(v1_only_df)),
    }

    return {
        "v0": v0_df,
        "v1_locations": v1_location_df,
        "metrics": metrics,
    }


def read_sheet_with_aliases(workbook: Path, sheet_names: tuple[str, ...], label: str) -> pd.DataFrame:
    """Read the first available sheet from a list of supported sheet names."""
    available_sheets = pd.ExcelFile(workbook).sheet_names
    for sheet_name in sheet_names:
        if sheet_name in available_sheets:
            return pd.read_excel(workbook, sheet_name=sheet_name)
    raise ValueError(
        f"Workbook must contain a {label} sheet. Tried: {', '.join(sheet_names)}. "
        f"Available sheets: {', '.join(available_sheets)}",
    )


@st.cache_data(show_spinner=False)
def load_review_payload(workbook_path: str | Path) -> dict[str, Any]:
    """Load the workbook and build the cached review payload."""
    workbook = Path(workbook_path)
    if not workbook.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook}")

    v0_raw = read_sheet_with_aliases(workbook, V0_SHEET_ALIASES, "V0")
    v1_raw = read_sheet_with_aliases(workbook, V1_SHEET_ALIASES, "V1")

    return build_review_payload(v0_raw, v1_raw)


def build_v0_location_postings_chart(location_row: pd.Series) -> pd.DataFrame:
    """Build V0 location chart rows using only the primary NAICS label."""
    label = normalize_text(location_row["PRIMARY_ESTABLISHMENT_NAICS6_NAME"])
    if not label:
        return pd.DataFrame(columns=["NAICS", "LOCATION_LABEL", "POSTINGS_COUNT"])

    postings_count = float(location_row["V0_POSTINGS_COUNT"])
    location_label = normalize_text(location_row.get("LOCATION_LABEL", ""))
    if not location_label:
        location_label = build_location_label(
            location_row.get("BGI_CITY", ""),
            location_row.get("BGI_STATE", ""),
            location_row.get("BGI_COUNTRY", ""),
        )
    chart_df = pd.DataFrame(
        {
            "NAICS": [label],
            "LOCATION_LABEL": [location_label],
            "POSTINGS_COUNT": [postings_count],
        },
    )
    return chart_df.sort_values(["POSTINGS_COUNT", "NAICS"], ascending=[False, True]).reset_index(drop=True)


def build_v0_all_locations_postings_chart(v0_df: pd.DataFrame) -> pd.DataFrame:
    """Build all-location V0 chart rows using each location's primary NAICS label."""
    chart_frames: list[pd.DataFrame] = []
    for _, location_row in v0_df.rename(columns={"POSTINGS_COUNT": "V0_POSTINGS_COUNT"}).iterrows():
        chart_frames.append(build_v0_location_postings_chart(location_row))

    if not chart_frames:
        return pd.DataFrame(columns=["NAICS", "LOCATION_LABEL", "POSTINGS_COUNT"])

    return (
        pd.concat(chart_frames, ignore_index=True)
        .groupby(["NAICS", "LOCATION_LABEL"], dropna=False)["POSTINGS_COUNT"]
        .sum()
        .reset_index()
        .sort_values(["POSTINGS_COUNT", "NAICS", "LOCATION_LABEL"], ascending=[False, True, True])
        .reset_index(drop=True)
    )


def build_v1_location_postings_chart(location_row: pd.Series) -> pd.DataFrame:
    """Build V1 location chart rows by evenly splitting location postings across establishments."""
    establishment_rows = pd.DataFrame(location_row["ESTABLISHMENT_ROWS"])
    if establishment_rows.empty:
        return pd.DataFrame(
            columns=[
                "NAICS",
                "ESTABLISHMENT_NAME",
                "LOCATION_LABEL",
                "ESTABLISHMENT_LOCATION",
                "POSTINGS_COUNT",
            ],
        )

    location_postings_count = float(location_row["LOCATION_POSTINGS_COUNT"])
    postings_per_establishment = location_postings_count / len(establishment_rows)
    location_label = normalize_text(location_row["LOCATION_LABEL"])
    chart_df = establishment_rows.assign(
        NAICS=establishment_rows["ESTABLISHMENT_NAICS6_NAME"].apply(normalize_text),
        ESTABLISHMENT_NAME=establishment_rows["ESTABLISHMENT_NAME"].apply(normalize_text),
        LOCATION_LABEL=location_label,
        POSTINGS_COUNT=postings_per_establishment,
    )
    chart_df["ESTABLISHMENT_NAME"] = chart_df["ESTABLISHMENT_NAME"].replace("", "Unknown establishment")
    chart_df["ESTABLISHMENT_LOCATION"] = chart_df["ESTABLISHMENT_NAME"] + " | " + chart_df["LOCATION_LABEL"]
    return (
        chart_df.groupby(
            ["NAICS", "ESTABLISHMENT_NAME", "LOCATION_LABEL", "ESTABLISHMENT_LOCATION"],
            dropna=False,
        )["POSTINGS_COUNT"]
        .sum()
        .reset_index()
        .sort_values(
            ["POSTINGS_COUNT", "NAICS", "ESTABLISHMENT_NAME", "LOCATION_LABEL"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )


def format_location_names(values: pd.Series, *, max_locations: int = 12) -> str:
    """Format associated location names for chart tooltips."""
    location_names = sorted({normalize_text(value) for value in values if normalize_text(value)})
    if len(location_names) <= max_locations:
        return "; ".join(location_names)
    visible_names = "; ".join(location_names[:max_locations])
    return f"{visible_names}; +{len(location_names) - max_locations} more"


def build_v1_all_locations_postings_chart(v1_location_df: pd.DataFrame) -> pd.DataFrame:
    """Build all-location V1 chart rows from per-location equal establishment splits."""
    chart_frames = [
        build_v1_location_postings_chart(location_row)
        for _, location_row in v1_location_df.iterrows()
    ]
    if not chart_frames:
        return pd.DataFrame(
            columns=[
                "NAICS",
                "ESTABLISHMENT_NAME",
                "LOCATION_LABEL",
                "ESTABLISHMENT_LOCATION",
                "LOCATION_COUNT",
                "LOCATION_NAMES",
                "POSTINGS_COUNT",
            ],
        )

    chart_df = pd.concat(chart_frames, ignore_index=True)
    return (
        chart_df.groupby(
            ["NAICS", "ESTABLISHMENT_NAME", "LOCATION_LABEL", "ESTABLISHMENT_LOCATION"],
            dropna=False,
        )
        .agg(
            POSTINGS_COUNT=("POSTINGS_COUNT", "sum"),
            LOCATION_COUNT=("LOCATION_LABEL", "nunique"),
            LOCATION_NAMES=("LOCATION_LABEL", format_location_names),
        )
        .reset_index()
        .sort_values(
            ["POSTINGS_COUNT", "NAICS", "ESTABLISHMENT_NAME", "LOCATION_LABEL"],
            ascending=[False, True, True, True],
        )
        .reset_index(drop=True)
    )


def shorten_chart_label(value: object, *, max_chars: int = 42) -> str:
    """Shorten long NAICS labels for chart axes."""
    label = normalize_text(value)
    if len(label) <= max_chars:
        return label
    return f"{label[: max_chars - 3]}..."


def prepare_horizontal_chart_data(
    chart_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Add display labels and ordering for stacked horizontal bar charts."""
    total_by_naics = (
        chart_df.groupby("NAICS", dropna=False)["POSTINGS_COUNT"]
        .sum()
        .reset_index(name="NAICS_TOTAL")
        .sort_values(["NAICS_TOTAL", "NAICS"], ascending=[False, True])
        .reset_index(drop=True)
    )
    display_labels: dict[str, str] = {}
    used_labels: set[str] = set()
    for naics in total_by_naics["NAICS"]:
        display_label = shorten_chart_label(naics)
        if display_label in used_labels:
            display_label = f"{display_label} [{len(used_labels) + 1}]"
        display_labels[naics] = display_label
        used_labels.add(display_label)

    display_df = chart_df.merge(total_by_naics, on="NAICS", how="left")
    display_df["NAICS_DISPLAY"] = display_df["NAICS"].map(display_labels)
    display_df = display_df.sort_values(
        ["NAICS_TOTAL", "NAICS", "POSTINGS_COUNT"],
        ascending=[False, True, False],
    ).reset_index(drop=True)
    return display_df, [display_labels[naics] for naics in total_by_naics["NAICS"]]


def render_horizontal_postings_chart(chart_df: pd.DataFrame, *, color_column: str) -> None:
    """Render a horizontal stacked postings bar chart."""
    if chart_df.empty:
        st.info("No postings data available for these locations.")
        return

    display_df, naics_order = prepare_horizontal_chart_data(chart_df)
    color_title = color_column.replace("_", " ").title()
    tooltip = [
        alt.Tooltip("NAICS:N", title="NAICS"),
        alt.Tooltip(f"{color_column}:N", title=color_title),
        alt.Tooltip("POSTINGS_COUNT:Q", title="# Postings", format=",.1f"),
    ]
    if "LOCATION_COUNT" in display_df.columns and "LOCATION_NAMES" in display_df.columns:
        tooltip.extend(
            [
                alt.Tooltip("LOCATION_COUNT:Q", title="Location count", format=",.0f"),
                alt.Tooltip("LOCATION_NAMES:N", title="Associated locations"),
            ],
        )
    chart_height = max(240, min(640, 96 + 64 * display_df["NAICS_DISPLAY"].nunique()))
    chart = (
        alt.Chart(display_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "POSTINGS_COUNT:Q",
                stack="zero",
                title="# Postings",
                axis=alt.Axis(format=",.0f", grid=True, labelColor="#7a7f90", titleColor="#7a7f90"),
            ),
            y=alt.Y(
                "NAICS_DISPLAY:N",
                sort=naics_order,
                title=None,
                axis=alt.Axis(labelLimit=300, labelColor="#7a7f90", labelFontSize=15),
            ),
            color=alt.Color(f"{color_column}:N", legend=None),
            tooltip=tooltip,
        )
        .properties(height=chart_height)
        .configure_axis(gridColor="#e5e7eb", domain=False, tickColor="#e5e7eb")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


def render_location_comparison_summary(payload: dict[str, Any]) -> None:
    """Render the top-of-page summary for the single-page comparison app."""
    metrics = payload["metrics"]
    summary_cols = st.columns(4)
    summary_cols[0].metric("Locations in both versions", metrics["overlap_locations"])
    summary_cols[1].metric("V0-only locations", metrics["v0_only_locations"])
    summary_cols[2].metric("V1-only locations", metrics["v1_only_locations"])
    summary_cols[3].metric("Workbook rows analyzed", metrics["v0_rows"] + metrics["v1_rows"])

    st.caption(
        "This app compares V0 location-level NAICS labels against V1 establishment-level labels "
        "aggregated back to the same city, state, and country location.",
    )


def render_location_comparison_page(payload: dict[str, Any]) -> None:
    """Render the single-page V0 versus V1 location comparison experience."""
    v0_df = payload["v0"]
    v1_location_df = payload["v1_locations"]

    st.subheader("V0 Location Postings Distribution")
    st.caption(
        "V0 stores one postings count per location and no establishment-name column, so this view "
        "assigns each location's postings to its primary NAICS label and colors the stacked bars by "
        "city, state, and country location.",
    )
    render_horizontal_postings_chart(build_v0_all_locations_postings_chart(v0_df), color_column="LOCATION_LABEL")

    st.subheader("V1 Establishment-Based Location Distribution")
    st.caption(
        "V1 repeats each location's postings count on every establishment row, so this view splits "
        "each location count evenly across its establishments, then stacks the result by NAICS and "
        "colors by establishment and location pairing.",
    )
    render_horizontal_postings_chart(
        build_v1_all_locations_postings_chart(v1_location_df),
        color_column="ESTABLISHMENT_LOCATION",
    )


def main() -> None:
    """Run the Streamlit app."""
    st.set_page_config(
        page_title="Amazon Multi-Industry Comparison",
        layout="wide",
    )
    st.title("Amazon Multi-Industry Comparison")
    st.caption("Workbook source: `multi-naics-review.xlsx`")

    try:
        payload = load_review_payload(WORKBOOK_PATH)
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    render_location_comparison_summary(payload)
    render_location_comparison_page(payload)


if __name__ == "__main__":
    main()
