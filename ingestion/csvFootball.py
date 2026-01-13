import os
import pandas as pd
import requests
from snowflake_io import insertVariantRows


def ingestCsvMatches():
    """
    Ingest football match data from a remote CSV into the Snowflake RAW layer.

    The CSV is downloaded from the URL defined in the environment variable
    `FOOTBALLCSV_URL`. If the URL is invalid or returns a non-200 status code,
    the function raises a RuntimeError with a helpful message.

    Expected environment variables:
        - FOOTBALLCSV_URL

    Target table:
        - FWA.RAW.CSV_MATCHES
    """
    csvUrl = os.environ["FOOTBALLCSV_URL"]

    response = requests.get(csvUrl, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to download CSV. HTTP {response.status_code} for URL: {csvUrl}\n"
            "Fix: update FOOTBALLCSV_URL in your .env to a valid CSV location."
        )

    # pandas can read directly from URL, but we already validated it above
    df = pd.read_csv(csvUrl)
    records = df.fillna("").to_dict(orient="records")

    insertVariantRows(
        tableFqn="FWA.RAW.CSV_MATCHES",
        rows=records,
        source="CSV",
    )
