# Amazon Location Comparison

Single-page Streamlit app for comparing Amazon V0 location-level NAICS labeling with V1 establishment-level NAICS labeling aggregated back to each location.

## Local Run

Install the locked dependencies with `uv`, then start Streamlit from the repo root:

```bash
uv sync
uv run streamlit run app.py
```

The app expects the workbook file `multi-naics-review.xlsx` to remain in the repository root next to `app.py`.

## Streamlit Community Cloud Deploy

Deploy this repository as a Streamlit Community Cloud app with:

- Repository root as the working directory
- `app.py` as the main file path

Community Cloud will install dependencies from the uv-managed project files in this repo, so keep `pyproject.toml` and `uv.lock` as the dependency source of truth.
