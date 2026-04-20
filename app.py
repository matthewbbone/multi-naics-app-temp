"""Streamlit app for comparing Amazon multi-industry V0 and V1 labeling."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener
import http.cookiejar

import altair as alt
import pandas as pd
import streamlit as st

WORKBOOK_PATH = Path("multi-naics-review.xlsx")
CURRENT_PORTABLE_CSV_URL = (
    "https://drive.google.com/file/d/1aAHXN7rW6XBnm2whhmRr4ChCxHuHG-bH/view?usp=sharing"
)
CURRENT_PORTABLE_CSV_DOWNLOAD_PATH = Path(tempfile.gettempdir()) / "multi-naics-current-minimal.csv"
JOB_MATCH_CACHE_DIR = Path(".job_establishment_match_cache")
INDUSTRY_LABEL_CACHE_DIR = Path(".industry_label_cache")
DISCOVERIES_PATH = Path("discovered_establishments.json")
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
CURRENT_PORTABLE_REQUIRED_COLUMNS = {
    "JOB_ID",
    "COMPANY",
    "BGI_CITY",
    "BGI_STATE",
    "BGI_COUNTRY",
    "ESTABLISHMENT_NAME",
    "ESTABLISHMENT_ADDRESS",
    "ESTABLISHMENT_NAICS6",
    "ESTABLISHMENT_NAICS6_NAME",
    "POSTINGS_COUNT",
}
CURRENT_PORTABLE_COLUMN_ORDER = [
    "JOB_ID",
    "COMPANY",
    "BGI_CITY",
    "BGI_STATE",
    "BGI_COUNTRY",
    "ESTABLISHMENT_NAME",
    "ESTABLISHMENT_ADDRESS",
    "ESTABLISHMENT_NAICS6",
    "ESTABLISHMENT_NAICS6_NAME",
    "POSTINGS_COUNT",
]
TOP3_COLUMNS = [
    "PRIMARY_ESTABLISHMENT_NAICS6_NAME",
    "SECONDARY_ESTABLISHMENT_NAICS6_NAME",
    "TERTIARY_ESTABLISHMENT_NAICS6_NAME",
]
HTTP_USER_AGENT = "Mozilla/5.0 (compatible; multi-naics-app/1.0)"


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


def read_json_payload(path: Path) -> Any:
    """Load one JSON payload from disk when present and valid."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def extract_google_drive_file_id(url: str) -> str:
    """Extract a Google Drive file id from a shared file URL."""
    parsed = urlparse(url)
    if parsed.netloc not in {"drive.google.com", "www.drive.google.com"}:
        raise ValueError(f"Unsupported Google Drive URL: {url}")

    match = re.search(r"/file/d/([^/]+)", parsed.path)
    if match:
        return match.group(1)

    file_ids = parse_qs(parsed.query).get("id", [])
    if file_ids:
        return file_ids[0]

    raise ValueError(f"Could not extract Google Drive file id from URL: {url}")


def build_google_drive_download_url(file_id: str, extra_params: dict[str, str] | None = None) -> str:
    """Build a Google Drive download URL for one file id."""
    params = {"export": "download", "id": file_id}
    if extra_params:
        params.update(extra_params)
    return f"https://drive.google.com/uc?{urlencode(params)}"


def response_is_download(response: Any) -> bool:
    """Return whether a URL response looks like a file download."""
    content_disposition = response.headers.get("Content-Disposition", "")
    content_type = response.headers.get("Content-Type", "")
    return "attachment" in content_disposition.casefold() or "text/csv" in content_type.casefold()


def extract_drive_confirmation_params(html: str, cookie_jar: http.cookiejar.CookieJar) -> dict[str, str]:
    """Extract the hidden confirmation form fields Google Drive returns for large files."""
    hidden_inputs = {
        name: value
        for name, value in re.findall(
            r'<input[^>]+type="hidden"[^>]+name="([^"]+)"[^>]+value="([^"]*)"',
            html,
        )
    }
    if {"id", "export", "confirm"}.issubset(hidden_inputs):
        return hidden_inputs

    for cookie in cookie_jar:
        if cookie.name.startswith("download_warning"):
            return {"confirm": cookie.value}

    confirm_match = re.search(r"confirm=([0-9A-Za-z_-]+)", html)
    if confirm_match:
        return {"confirm": confirm_match.group(1)}

    return {}


def stream_response_to_file(response: Any, destination_path: Path) -> None:
    """Persist a streamed HTTP response to a local file atomically."""
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination_path.with_suffix(destination_path.suffix + ".tmp")
    with temp_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    temp_path.replace(destination_path)


def download_google_drive_file(shared_url: str, destination_path: Path) -> Path:
    """Download one shared Google Drive file to a local path."""
    file_id = extract_google_drive_file_id(shared_url)
    cookie_jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))

    def open_drive_url(url: str) -> Any:
        request = Request(url, headers={"User-Agent": HTTP_USER_AGENT})
        return opener.open(request, timeout=180)

    try:
        initial_response = open_drive_url(build_google_drive_download_url(file_id))
        if response_is_download(initial_response):
            stream_response_to_file(initial_response, destination_path)
            return destination_path

        html = initial_response.read().decode("utf-8", errors="ignore")
        confirm_params = extract_drive_confirmation_params(html, cookie_jar)
        if not confirm_params:
            raise ValueError("Google Drive did not return downloadable content for the shared file.")

        confirm_params.setdefault("id", file_id)
        confirm_params.setdefault("export", "download")
        confirmed_response = open_drive_url(
            f"https://drive.google.com/uc?{urlencode(confirm_params)}",
        )
        if not response_is_download(confirmed_response):
            raise ValueError("Google Drive confirmation flow did not return the CSV download.")

        stream_response_to_file(confirmed_response, destination_path)
        return destination_path
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"Failed to download current CSV from Google Drive: {exc}") from exc


def build_discovery_establishment_cache_key(
    context: dict[str, Any],
    establishment: dict[str, Any],
) -> str:
    """Rebuild the industry-label cache key for one discovered establishment."""
    payload = {
        "candidate_address": normalize_text(establishment.get("address")),
        "candidate_description": normalize_text(establishment.get("description")),
        "candidate_maps_link": normalize_text(establishment.get("maps_link")),
        "candidate_name": normalize_text(establishment.get("name")),
        "candidate_source": normalize_text(establishment.get("source")),
        "candidate_type": normalize_text(establishment.get("type")),
        "company": normalize_text(context.get("company")) or "Unknown company",
        "establishment_id": normalize_text(context.get("establishment_id")),
        "location_name": normalize_text(context.get("location_name")),
        "query": normalize_text(context.get("query")),
    }
    # Match the serializer used by DiscoveryEstablishmentRecord.cache_key exactly.
    serialized = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_candidate_lookup_keys(
    *,
    company: object,
    location_label: object,
    city: object,
    state: object,
    country: object,
    candidate_name: object,
    candidate_address: object,
    candidate_cid: object,
    candidate_maps_link: object,
) -> list[str]:
    """Build stable join keys for matching cached jobs to labeled establishments."""
    company_key = normalize_text(company).casefold()
    location_name_key = normalize_text(location_label).casefold()
    location_key = build_location_key(city, state, country)
    candidate_name_key = normalize_text(candidate_name).casefold()
    candidate_address_key = normalize_text(candidate_address).casefold()
    candidate_cid_key = normalize_text(candidate_cid)
    candidate_maps_key = normalize_text(candidate_maps_link)

    keys: list[str] = []
    if candidate_cid_key:
        keys.append(f"cid::{candidate_cid_key}")
    if candidate_maps_key:
        keys.append(f"maps::{candidate_maps_key}")
    if company_key and location_key and candidate_name_key and candidate_address_key:
        keys.append(
            "company_location_key_name_address::"
            + "||".join([company_key, location_key, candidate_name_key, candidate_address_key]),
        )
    if company_key and location_name_key and candidate_name_key and candidate_address_key:
        keys.append(
            "company_location_name_name_address::"
            + "||".join([company_key, location_name_key, candidate_name_key, candidate_address_key]),
        )
    if company_key and candidate_name_key and candidate_address_key:
        keys.append(
            "company_name_address::"
            + "||".join([company_key, candidate_name_key, candidate_address_key]),
        )
    if company_key and location_key and candidate_name_key:
        keys.append(
            "company_location_key_name::"
            + "||".join([company_key, location_key, candidate_name_key]),
        )
    return keys


def build_labeled_establishment_lookup(
    *,
    label_cache_dir: Path,
    discoveries_path: Path,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Load labeled establishments keyed by candidate identifiers for job-match joins."""
    establishment_cache_dir = label_cache_dir / "establishments"
    if not establishment_cache_dir.exists():
        raise FileNotFoundError(f"Labeled establishment cache not found: {establishment_cache_dir}")

    discoveries = read_json_payload(discoveries_path)
    if not isinstance(discoveries, list):
        raise ValueError(f"Expected list payload in {discoveries_path}")

    discovery_index: dict[str, dict[str, Any]] = {}
    for context in discoveries:
        if not isinstance(context, dict):
            continue
        establishments = context.get("discovered_establishments", [])
        if not isinstance(establishments, list):
            continue
        for establishment in establishments:
            if not isinstance(establishment, dict):
                continue
            review = establishment.get("llm_review")
            if not isinstance(review, dict) or not review.get("is_company_establishment_location"):
                continue
            cache_key = build_discovery_establishment_cache_key(context, establishment)
            discovery_index.setdefault(
                cache_key,
                {
                    "company": normalize_text(context.get("company")),
                    "location_label": normalize_text(context.get("location_name"))
                    or build_location_label(
                        context.get("bgi_city"),
                        context.get("bgi_state"),
                        context.get("bgi_country"),
                    ),
                    "bgi_city": normalize_text(context.get("bgi_city")),
                    "bgi_state": normalize_text(context.get("bgi_state")),
                    "bgi_country": normalize_text(context.get("bgi_country")),
                    "establishment_name": normalize_text(establishment.get("name")),
                    "establishment_address": normalize_text(establishment.get("address")),
                    "establishment_cid": normalize_text(establishment.get("cid")),
                    "establishment_maps_link": normalize_text(establishment.get("maps_link")),
                },
            )

    lookup: dict[str, dict[str, Any]] = {}
    labeled_rows = 0
    for path in sorted(establishment_cache_dir.glob("*.json")):
        payload = read_json_payload(path)
        if not isinstance(payload, dict):
            continue
        label = payload.get("label")
        if not isinstance(label, dict):
            continue
        cache_key = normalize_text(payload.get("cache_key")) or path.stem
        discovery_entry = discovery_index.get(cache_key)
        if discovery_entry is None:
            continue

        labeled_row = {
            "COMPANY": discovery_entry["company"],
            "LOCATION_LABEL": discovery_entry["location_label"],
            "BGI_CITY": discovery_entry["bgi_city"],
            "BGI_STATE": discovery_entry["bgi_state"],
            "BGI_COUNTRY": discovery_entry["bgi_country"],
            "ESTABLISHMENT_NAME": discovery_entry["establishment_name"],
            "ESTABLISHMENT_ADDRESS": discovery_entry["establishment_address"],
            "ESTABLISHMENT_CID": discovery_entry["establishment_cid"],
            "ESTABLISHMENT_MAPS_LINK": discovery_entry["establishment_maps_link"],
            "ESTABLISHMENT_NAICS6": normalize_text(label.get("naics_code")),
            "ESTABLISHMENT_NAICS6_NAME": normalize_text(label.get("naics_title")),
        }
        labeled_rows += 1
        for key in build_candidate_lookup_keys(
            company=labeled_row["COMPANY"],
            location_label=labeled_row["LOCATION_LABEL"],
            city=labeled_row["BGI_CITY"],
            state=labeled_row["BGI_STATE"],
            country=labeled_row["BGI_COUNTRY"],
            candidate_name=labeled_row["ESTABLISHMENT_NAME"],
            candidate_address=labeled_row["ESTABLISHMENT_ADDRESS"],
            candidate_cid=labeled_row["ESTABLISHMENT_CID"],
            candidate_maps_link=labeled_row["ESTABLISHMENT_MAPS_LINK"],
        ):
            lookup.setdefault(key, labeled_row)

    return lookup, labeled_rows


def build_current_match_row(
    payload: dict[str, Any],
    *,
    labeled_establishment_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Convert one cached matched job into the normalized current comparison row."""
    source_row = payload.get("source_row")
    matched_candidate = payload.get("matched_candidate")
    if not isinstance(source_row, dict) or not isinstance(matched_candidate, dict) or not matched_candidate:
        return None

    location_label = normalize_text(source_row.get("LOCATION_NAME")) or build_location_label(
        source_row.get("BGI_CITY"),
        source_row.get("BGI_STATE"),
        source_row.get("BGI_COUNTRY"),
    )
    labeled_establishment = None
    for key in build_candidate_lookup_keys(
        company=source_row.get("COMPANY"),
        location_label=location_label,
        city=source_row.get("BGI_CITY"),
        state=source_row.get("BGI_STATE"),
        country=source_row.get("BGI_COUNTRY"),
        candidate_name=matched_candidate.get("name"),
        candidate_address=matched_candidate.get("address"),
        candidate_cid=matched_candidate.get("cid"),
        candidate_maps_link=matched_candidate.get("maps_link"),
    ):
        labeled_establishment = labeled_establishment_lookup.get(key)
        if labeled_establishment is not None:
            break

    if labeled_establishment is None:
        return None

    return {
        "JOB_ID": normalize_text(source_row.get("JOB_ID")),
        "REV_ID": normalize_text(source_row.get("REV_ID")),
        "COMPANY": normalize_text(source_row.get("COMPANY")),
        "BGI_CITY": normalize_text(source_row.get("BGI_CITY")),
        "BGI_STATE": normalize_text(source_row.get("BGI_STATE")),
        "BGI_COUNTRY": normalize_text(source_row.get("BGI_COUNTRY")),
        "LOCATION_NAME": location_label,
        "MATCH_STATUS": normalize_text(payload.get("match_status")),
        "MATCH_METHOD": normalize_text(payload.get("match_method")),
        "MATCH_CONFIDENCE": normalize_text(payload.get("confidence")),
        "ESTABLISHMENT_NAME": normalize_text(matched_candidate.get("name"))
        or labeled_establishment["ESTABLISHMENT_NAME"],
        "ESTABLISHMENT_ADDRESS": normalize_text(matched_candidate.get("address"))
        or labeled_establishment["ESTABLISHMENT_ADDRESS"],
        "ESTABLISHMENT_NAICS6": labeled_establishment["ESTABLISHMENT_NAICS6"],
        "ESTABLISHMENT_NAICS6_NAME": labeled_establishment["ESTABLISHMENT_NAICS6_NAME"],
        "POSTINGS_COUNT": 1,
    }


def load_current_match_rows(
    *,
    match_cache_dir: Path,
    label_cache_dir: Path,
    discoveries_path: Path,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Load job-level matched establishments joined to cached establishment NAICS labels."""
    matches_dir = match_cache_dir / "matches"
    if not matches_dir.exists():
        raise FileNotFoundError(f"Job match cache not found: {matches_dir}")

    labeled_establishment_lookup, labeled_establishments = build_labeled_establishment_lookup(
        label_cache_dir=label_cache_dir,
        discoveries_path=discoveries_path,
    )

    rows: list[dict[str, Any]] = []
    matched_jobs = 0
    missing_establishment_labels = 0
    for path in sorted(matches_dir.glob("*/*.json")):
        payload = read_json_payload(path)
        if not isinstance(payload, dict):
            continue
        matched_candidate = payload.get("matched_candidate")
        if not isinstance(matched_candidate, dict) or not matched_candidate:
            continue
        matched_jobs += 1
        row = build_current_match_row(
            payload,
            labeled_establishment_lookup=labeled_establishment_lookup,
        )
        if row is None:
            missing_establishment_labels += 1
            continue
        rows.append(row)

    if not rows:
        raise ValueError("No matched jobs with labeled establishments were available for comparison.")

    return pd.DataFrame(rows), {
        "matched_jobs": matched_jobs,
        "represented_jobs": len(rows),
        "missing_establishment_labels": missing_establishment_labels,
        "labeled_establishments": labeled_establishments,
        "source": "cache_join",
    }


def build_current_portable_df(v1_df: pd.DataFrame) -> pd.DataFrame:
    """Return the smallest current-side dataframe that preserves app behavior."""
    validate_sheet_columns(v1_df, set(CURRENT_PORTABLE_COLUMN_ORDER), "current-side rows")
    portable_df = v1_df[CURRENT_PORTABLE_COLUMN_ORDER].copy()
    portable_df["POSTINGS_COUNT"] = (
        pd.to_numeric(portable_df["POSTINGS_COUNT"], errors="coerce").fillna(0).astype(int)
    )
    return portable_df.sort_values(
        [
            "BGI_COUNTRY",
            "BGI_STATE",
            "BGI_CITY",
            "ESTABLISHMENT_NAME",
            "ESTABLISHMENT_ADDRESS",
            "JOB_ID",
        ],
        kind="stable",
    ).reset_index(drop=True)


def load_current_portable_rows(
    current_csv_path: Path,
    *,
    source: str = "portable_csv",
    source_label: str | None = None,
    source_url: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load already-normalized current-side rows from a portable CSV."""
    if not current_csv_path.exists():
        raise FileNotFoundError(f"Portable current CSV not found: {current_csv_path}")

    current_df = pd.read_csv(current_csv_path, dtype=str).fillna("")
    validate_sheet_columns(current_df, CURRENT_PORTABLE_REQUIRED_COLUMNS, current_csv_path.name)
    current_df["POSTINGS_COUNT"] = (
        pd.to_numeric(current_df["POSTINGS_COUNT"], errors="coerce").fillna(0).astype(int)
    )
    return current_df, {
        "source": source,
        "portable_csv_path": str(current_csv_path),
        "portable_csv_label": source_label or current_csv_path.name,
        "portable_csv_url": source_url,
        "represented_jobs": int(len(current_df)),
    }


def export_current_portable_csv(
    output_path: Path,
    *,
    match_cache_dir: Path = JOB_MATCH_CACHE_DIR,
    label_cache_dir: Path = INDUSTRY_LABEL_CACHE_DIR,
    discoveries_path: Path = DISCOVERIES_PATH,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Export the minimal current-side rows needed by this app to a CSV."""
    current_df, stats = load_current_match_rows(
        match_cache_dir=match_cache_dir,
        label_cache_dir=label_cache_dir,
        discoveries_path=discoveries_path,
    )
    portable_df = build_current_portable_df(current_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    portable_df.to_csv(output_path, index=False)

    export_stats = dict(stats)
    export_stats.update(
        {
            "source": "portable_csv",
            "portable_csv_path": str(output_path),
            "portable_csv_rows": int(len(portable_df)),
            "portable_csv_columns": list(portable_df.columns),
        },
    )
    return portable_df, export_stats


def load_current_source_rows(
    *,
    current_csv_url: str | None,
    current_csv_download_path: Path | None,
    match_cache_dir: Path,
    label_cache_dir: Path,
    discoveries_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load current-side rows from the downloaded CSV when available, otherwise from caches."""
    if current_csv_url is not None and current_csv_download_path is not None:
        try:
            downloaded_path = download_google_drive_file(current_csv_url, current_csv_download_path)
            return load_current_portable_rows(
                downloaded_path,
                source="portable_csv_download",
                source_label="Google Drive CSV",
                source_url=current_csv_url,
            )
        except (RuntimeError, ValueError, FileNotFoundError):
            pass

    return load_current_match_rows(
        match_cache_dir=match_cache_dir,
        label_cache_dir=label_cache_dir,
        discoveries_path=discoveries_path,
    )


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


def build_v0_postings_distribution(v0_df: pd.DataFrame) -> pd.DataFrame:
    """Return postings-weighted V0 primary NAICS distribution."""
    return (
        v0_df.groupby("PRIMARY_ESTABLISHMENT_NAICS6_NAME", dropna=False)["POSTINGS_COUNT"]
        .sum()
        .reset_index(name="POSTINGS_COUNT")
        .sort_values(["POSTINGS_COUNT", "PRIMARY_ESTABLISHMENT_NAICS6_NAME"], ascending=[False, True])
        .reset_index(drop=True)
    )


def build_v1_postings_distribution(v1_df: pd.DataFrame) -> pd.DataFrame:
    """Return V1 postings-weighted NAICS distribution split by establishment."""
    return (
        v1_df.groupby(["ESTABLISHMENT_NAICS6_NAME", "ESTABLISHMENT_NAME"], dropna=False)["POSTINGS_COUNT"]
        .sum()
        .reset_index(name="POSTINGS_COUNT")
        .sort_values(["POSTINGS_COUNT", "ESTABLISHMENT_NAICS6_NAME", "ESTABLISHMENT_NAME"], ascending=[False, True, True])
        .reset_index(drop=True)
    )


def is_job_weighted_v1(v1_df: pd.DataFrame) -> bool:
    """Return whether the current-side rows are job-level rather than location-level."""
    if "JOB_ID" not in v1_df.columns:
        return False
    return v1_df["JOB_ID"].fillna("").astype(str).str.strip().ne("").any()


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
        job_weighted = is_job_weighted_v1(group)
        establishment_rows = (
            group[
                [
                    "ESTABLISHMENT_NAME",
                    "ESTABLISHMENT_ADDRESS",
                    "ESTABLISHMENT_NAICS6",
                    "ESTABLISHMENT_NAICS6_NAME",
                    "POSTINGS_COUNT",
                ]
            ]
            .groupby(
                [
                    "ESTABLISHMENT_NAME",
                    "ESTABLISHMENT_ADDRESS",
                    "ESTABLISHMENT_NAICS6",
                    "ESTABLISHMENT_NAICS6_NAME",
                ],
                dropna=False,
            )["POSTINGS_COUNT"]
            .sum()
            .reset_index()
            .sort_values(
                [
                    "POSTINGS_COUNT",
                    "ESTABLISHMENT_NAICS6_NAME",
                    "ESTABLISHMENT_NAME",
                    "ESTABLISHMENT_ADDRESS",
                ],
                ascending=[False, True, True, True],
            )
            .reset_index(drop=True)
        )
        naics_counts = (
            establishment_rows.groupby(
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
        establishments = establishment_rows.to_dict(orient="records")
        naics_frequency_map = {
            normalize_text(name): int(count)
            for name, count in zip(
                naics_counts["ESTABLISHMENT_NAICS6_NAME"],
                naics_counts["ESTABLISHMENT_COUNT"],
                strict=False,
            )
        }
        total_establishments = int(len(establishment_rows))
        if job_weighted:
            total_postings = int(establishment_rows["POSTINGS_COUNT"].sum())
            location_postings = total_postings
            top_share = (
                int(top_row["POSTINGS_COUNT"]) / location_postings
                if location_postings
                else 0.0
            )
        else:
            total_postings = int(group["POSTINGS_COUNT"].sum())
            location_postings = int(group["POSTINGS_COUNT"].max())
            top_share = (
                int(top_row["ESTABLISHMENT_COUNT"]) / total_establishments
                if total_establishments
                else 0.0
            )

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
                "TOP_V1_NAICS_SHARE": top_share,
                "TOP_V1_NAICS_POSTINGS_COUNT": int(top_row["POSTINGS_COUNT"]),
                "NAICS_FREQUENCY_MAP": naics_frequency_map,
                "IS_JOB_WEIGHTED": job_weighted,
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
                "IS_JOB_WEIGHTED",
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


def build_review_payload(
    v0_raw: pd.DataFrame,
    v1_raw: pd.DataFrame,
    *,
    current_source_stats: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Prepare all normalized dataframes and summary metrics for the app."""
    v0_df = prepare_v0_sheet(v0_raw)
    v1_df = prepare_v1_sheet(v1_raw)
    v1_location_df = aggregate_v1_locations(v1_df)
    v0_postings_distribution = build_v0_postings_distribution(v0_df)
    v1_postings_distribution = build_v1_postings_distribution(v1_df)
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
        "v1": v1_df,
        "v1_locations": v1_location_df,
        "v0_postings_distribution": v0_postings_distribution,
        "v1_postings_distribution": v1_postings_distribution,
        "comparison": comparison_df,
        "v0_only": v0_only_df,
        "v1_only": v1_only_df,
        "metrics": metrics,
        "current_source_stats": dict(current_source_stats or {}),
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
def load_review_payload(
    workbook_path: str | Path,
    current_csv_url: str | None = CURRENT_PORTABLE_CSV_URL,
    current_csv_download_path: str | Path | None = CURRENT_PORTABLE_CSV_DOWNLOAD_PATH,
) -> dict[str, Any]:
    """Load the workbook plus cached current results and build the review payload."""
    workbook = Path(workbook_path)
    if not workbook.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook}")

    v0_raw = read_sheet_with_aliases(workbook, V0_SHEET_ALIASES, "V0")
    resolved_current_csv_download_path = (
        None if current_csv_download_path is None else Path(current_csv_download_path)
    )
    v1_raw, current_source_stats = load_current_source_rows(
        current_csv_url=current_csv_url,
        current_csv_download_path=resolved_current_csv_download_path,
        match_cache_dir=JOB_MATCH_CACHE_DIR,
        label_cache_dir=INDUSTRY_LABEL_CACHE_DIR,
        discoveries_path=DISCOVERIES_PATH,
    )

    return build_review_payload(v0_raw, v1_raw, current_source_stats=current_source_stats)


def apply_search_filter(df: pd.DataFrame, query: str, columns: list[str]) -> pd.DataFrame:
    """Filter a dataframe to rows whose selected columns contain the query string."""
    query_text = query.strip().casefold()
    if not query_text:
        return df

    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column not in df.columns:
            continue
        mask |= df[column].fillna("").astype(str).str.casefold().str.contains(query_text, regex=False)
    return df[mask].copy()


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
    """Build V1 location chart rows using real job counts when available."""
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

    location_label = normalize_text(location_row["LOCATION_LABEL"])
    establishment_postings = pd.to_numeric(
        establishment_rows["POSTINGS_COUNT"],
        errors="coerce",
    ).fillna(0.0)
    if bool(location_row.get("IS_JOB_WEIGHTED")):
        postings_by_establishment = establishment_postings
    else:
        location_postings_count = float(location_row["LOCATION_POSTINGS_COUNT"])
        postings_by_establishment = pd.Series(
            location_postings_count / len(establishment_rows),
            index=establishment_rows.index,
        )
    chart_df = establishment_rows.assign(
        NAICS=establishment_rows["ESTABLISHMENT_NAICS6_NAME"].apply(normalize_text),
        ESTABLISHMENT_NAME=establishment_rows["ESTABLISHMENT_NAME"].apply(normalize_text),
        LOCATION_LABEL=location_label,
        POSTINGS_COUNT=postings_by_establishment,
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


def compute_shared_horizontal_axis_domain(*chart_dfs: pd.DataFrame) -> tuple[float, float] | None:
    """Return a shared numeric x-axis domain for one or more stacked postings charts."""
    max_total = 0.0
    for chart_df in chart_dfs:
        if chart_df.empty:
            continue
        chart_max = float(
            chart_df.groupby("NAICS", dropna=False)["POSTINGS_COUNT"].sum().max() or 0.0,
        )
        max_total = max(max_total, chart_max)
    if max_total <= 0:
        return None
    return (0.0, max_total)


def render_horizontal_postings_chart(
    chart_df: pd.DataFrame,
    *,
    color_column: str,
    x_domain: tuple[float, float] | None = None,
) -> None:
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
                scale=alt.Scale(domain=x_domain) if x_domain is not None else alt.Undefined,
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


def render_metric_row(metrics: dict[str, int]) -> None:
    """Render the headline app metrics."""
    top_cols = st.columns(4)
    top_cols[0].metric("V0 Amazon rows", metrics["v0_rows"])
    top_cols[1].metric("V1 matched jobs", metrics["v1_rows"])
    top_cols[2].metric("V0 unique locations", metrics["v0_unique_locations"])
    top_cols[3].metric("V1 unique locations", metrics["v1_unique_locations"])

    bottom_cols = st.columns(3)
    bottom_cols[0].metric("Overlapping locations", metrics["overlap_locations"])
    bottom_cols[1].metric("V0-only locations", metrics["v0_only_locations"])
    bottom_cols[2].metric("V1-only locations", metrics["v1_only_locations"])

    postings_cols = st.columns(2)
    postings_cols[0].metric("V0 postings represented", metrics["v0_postings"])
    postings_cols[1].metric("V1 matched jobs represented", metrics["v1_postings"])


def render_current_quality_highlights() -> None:
    """Render hard-coded quality highlights for current results."""
    stat_cols = st.columns(5)
    stat_items = [
        ("83.7%", "of job posts need LLM review"),
        ("16.3%", "have only one establishment within a location"),
        ("25.9%", "of LLM-reviewed matches are good matches"),
        ("69.8%", "of LLM-reviewed matches are ambiguous"),
        ("4.7%", "of LLM-reviewed matches are bad matches"),
    ]
    for column, (value, label) in zip(stat_cols, stat_items, strict=True):
        column.markdown(
            f"""
            <div style="padding: 0.25rem 0 0.75rem 0;">
                <div style="font-size: 2rem; font-weight: 700; line-height: 1.1;">{value}</div>
                <div style="font-size: 0.9rem; color: #6b7280; line-height: 1.35; padding-top: 0.2rem;">
                    {label}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_overview_tab(payload: dict[str, Any]) -> None:
    """Render the high-level overview tab."""
    metrics = payload["metrics"]
    comparison_df = payload["comparison"]
    current_source_stats = payload["current_source_stats"]
    render_metric_row(metrics)

    if current_source_stats:
        if current_source_stats.get("source") in {"portable_csv", "portable_csv_download"}:
            source_label = current_source_stats.get("portable_csv_label") or Path(
                str(current_source_stats["portable_csv_path"]),
            ).name
            stats_text = (
                "V1 represents "
                f"{current_source_stats['represented_jobs']:,} matched jobs loaded from "
                f"`{source_label}`."
            )
        else:
            stats_text = (
                "V1 represents "
                f"{current_source_stats['represented_jobs']:,} matched jobs from `.job_establishment_match_cache`, "
                f"joined to {current_source_stats['labeled_establishments']:,} labeled establishments in `.industry_label_cache`."
            )
            if current_source_stats["missing_establishment_labels"]:
                stats_text += (
                    " "
                    f"Excluded {current_source_stats['missing_establishment_labels']:,} matched jobs with no local establishment label."
                )
        st.caption(stats_text)

    st.info(
        "V0 assigns ordered top-3 NAICS labels at the location level, while V1 "
        "matches individual jobs back to specific establishments and rolls those matched-job "
        "establishment NAICS labels back up to the location level for comparison.",
    )

    match_counts = comparison_df["MATCH_ANY_TOP3"].map(
        {
            True: "Matches any V0 top-3",
            False: "No V0/V1 overlap",
        },
    ).value_counts()
    st.subheader("Overlap Match Summary")
    st.bar_chart(match_counts)

    overview_table = comparison_df[
        [
            "LOCATION_LABEL",
            "PRIMARY_ESTABLISHMENT_NAICS6_NAME",
            "TOP_V1_NAICS_NAME",
            "ESTABLISHMENT_COUNT",
            "DISTINCT_NAICS_COUNT",
            "MATCH_PRIMARY",
            "MATCH_ANY_TOP3",
        ]
    ].rename(
        columns={
            "LOCATION_LABEL": "Location",
            "PRIMARY_ESTABLISHMENT_NAICS6_NAME": "V0 Primary",
            "TOP_V1_NAICS_NAME": "V1 Top NAICS",
            "ESTABLISHMENT_COUNT": "V1 Establishments",
            "DISTINCT_NAICS_COUNT": "V1 Distinct NAICS",
            "MATCH_PRIMARY": "Matches V0 Primary",
            "MATCH_ANY_TOP3": "Matches any V0 Top-3",
        },
    )
    st.subheader("Overlapping Location Snapshot")
    st.dataframe(overview_table, use_container_width=True, hide_index=True)


def render_v0_tab(payload: dict[str, Any]) -> None:
    """Render the V0 summary tab."""
    v0_df = payload["v0"]
    v0_postings_distribution = payload["v0_postings_distribution"]
    primary_counts = v0_df["PRIMARY_ESTABLISHMENT_NAICS6_NAME"].value_counts().head(15)
    secondary_tertiary = pd.DataFrame(
        {
            "Secondary label count": v0_df["SECONDARY_ESTABLISHMENT_NAICS6_NAME"].value_counts().head(15),
            "Tertiary label count": v0_df["TERTIARY_ESTABLISHMENT_NAICS6_NAME"].value_counts().head(15),
        },
    ).fillna(0)

    st.subheader("Top V0 Primary NAICS Labels")
    st.bar_chart(primary_counts)

    st.subheader("V0 Primary NAICS Distribution by Postings Count")
    st.bar_chart(
        v0_postings_distribution.head(15),
        x="PRIMARY_ESTABLISHMENT_NAICS6_NAME",
        y="POSTINGS_COUNT",
    )

    st.subheader("Secondary and Tertiary V0 Labels")
    st.dataframe(secondary_tertiary, use_container_width=True)

    search_query = st.text_input(
        "Search V0 locations or labels",
        key="v0_search_query",
        placeholder="Try a city, state, or NAICS label",
    )
    filtered_v0 = apply_search_filter(
        v0_df,
        search_query,
        ["LOCATION_LABEL", *TOP3_COLUMNS],
    )
    display_df = filtered_v0[
        [
            "LOCATION_LABEL",
            "POSTINGS_COUNT",
            *TOP3_COLUMNS,
        ]
    ].rename(
        columns={
            "LOCATION_LABEL": "Location",
            "POSTINGS_COUNT": "Postings count",
            "PRIMARY_ESTABLISHMENT_NAICS6_NAME": "Primary",
            "SECONDARY_ESTABLISHMENT_NAICS6_NAME": "Secondary",
            "TERTIARY_ESTABLISHMENT_NAICS6_NAME": "Tertiary",
        },
    )

    st.subheader("All V0 Amazon Locations")
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_v1_tab(payload: dict[str, Any]) -> None:
    """Render the current-version summary tab."""
    v1_df = payload["v1"]
    v1_locations = payload["v1_locations"]
    v1_postings_distribution = payload["v1_postings_distribution"]

    top_v1_counts = v1_df["ESTABLISHMENT_NAICS6_NAME"].value_counts().head(15)
    v1_postings_by_naics = (
        v1_df.groupby("ESTABLISHMENT_NAICS6_NAME", dropna=False)["POSTINGS_COUNT"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
    )
    establishments_per_location = v1_locations["ESTABLISHMENT_COUNT"].value_counts().sort_index()
    distinct_naics_per_location = v1_locations["DISTINCT_NAICS_COUNT"].value_counts().sort_index()

    render_current_quality_highlights()
    st.subheader("Top V1 NAICS Labels Across Matched Jobs")
    st.bar_chart(top_v1_counts)

    st.subheader("V1 NAICS Distribution by Matched Job Count")
    st.bar_chart(v1_postings_by_naics)

    st.subheader("V1 Matched-Job Distribution Colored by Establishment")
    max_chart_establishments = st.slider(
        "Number of establishment segments to show",
        min_value=25,
        max_value=300,
        value=100,
        step=25,
        help="Limits the number of establishment-colored segments so the chart remains readable.",
    )
    st.bar_chart(
        v1_postings_distribution.head(max_chart_establishments),
        x="ESTABLISHMENT_NAICS6_NAME",
        y="POSTINGS_COUNT",
        color="ESTABLISHMENT_NAME",
    )

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Establishments per Location")
        st.bar_chart(establishments_per_location)
    with chart_cols[1]:
        st.subheader("Distinct NAICS per Location")
        st.bar_chart(distinct_naics_per_location)

    search_query = st.text_input(
        "Search V1 locations or top NAICS",
        key="v1_search_query",
        placeholder="Try a city, state, or NAICS label",
    )
    filtered_v1_locations = apply_search_filter(
        v1_locations,
        search_query,
        ["LOCATION_LABEL", "TOP_V1_NAICS_NAME"],
    )
    display_df = filtered_v1_locations[
        [
            "LOCATION_LABEL",
            "POSTINGS_COUNT",
            "ESTABLISHMENT_COUNT",
            "DISTINCT_NAICS_COUNT",
            "TOP_V1_NAICS_NAME",
            "TOP_V1_NAICS_SHARE",
        ]
    ].rename(
        columns={
            "LOCATION_LABEL": "Location",
            "POSTINGS_COUNT": "Postings count",
            "ESTABLISHMENT_COUNT": "Establishment count",
            "DISTINCT_NAICS_COUNT": "Distinct NAICS count",
            "TOP_V1_NAICS_NAME": "Top V1 NAICS",
            "TOP_V1_NAICS_SHARE": "Top V1 share",
        },
    )
    display_df["Top V1 share"] = display_df["Top V1 share"].map(lambda value: f"{value:.1%}")

    st.subheader("Aggregated V1 Amazon Locations")
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_location_comparison_tab(payload: dict[str, Any]) -> None:
    """Render all-location V0 versus V1 postings comparison charts."""
    v0_df = payload["v0"]
    v1_location_df = payload["v1_locations"]
    v0_chart_df = build_v0_all_locations_postings_chart(v0_df)
    v1_chart_df = build_v1_all_locations_postings_chart(v1_location_df)
    shared_x_domain = compute_shared_horizontal_axis_domain(v0_chart_df, v1_chart_df)

    st.subheader("V0 NAICS Distribution")
    st.caption(
        "V0 has one location-level POSTINGS_COUNT and no establishment-name column, so this splits "
        "each location's postings by its primary NAICS label and colors the stacked bars by unique "
        "city/state/country location.",
    )
    render_horizontal_postings_chart(
        v0_chart_df,
        color_column="LOCATION_LABEL",
        x_domain=shared_x_domain,
    )

    st.subheader("V1 NAICS Distribution")
    st.caption(
        "V1 rows are matched job postings, each tied to a specific establishment, so this stacks "
        "actual matched-job counts by NAICS and colors by the unique establishment x location pairing.",
    )
    render_current_quality_highlights()
    render_horizontal_postings_chart(
        v1_chart_df,
        color_column="ESTABLISHMENT_LOCATION",
        x_domain=shared_x_domain,
    )


def main() -> None:
    """Run the Streamlit app."""
    st.set_page_config(
        page_title="Amazon Multi-Industry Location Comparison",
        layout="wide",
    )
    st.title("Amazon Multi-Industry Location Comparison")
    st.caption(
        "Sources: `multi-naics-review.xlsx` (`multi-naics-0`) + Google Drive current CSV on startup, "
        "otherwise `.job_establishment_match_cache` + `.industry_label_cache` + `discovered_establishments.json`.",
    )

    try:
        payload = load_review_payload(WORKBOOK_PATH, CURRENT_PORTABLE_CSV_URL)
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    render_location_comparison_tab(payload)


if __name__ == "__main__":
    main()
