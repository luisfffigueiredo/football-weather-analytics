import os
import json
from datetime import datetime, timezone
import snowflake.connector


def sfConnect():
    """
    Create and return a connection to Snowflake using environment variables.

    Expected environment variables:
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_USER
        - SNOWFLAKE_PASSWORD
        - SNOWFLAKE_ROLE (optional)
        - SNOWFLAKE_WAREHOUSE
        - SNOWFLAKE_DATABASE (defaults to 'FWA')

    Returns
    -------
    snowflake.connector.connection.SnowflakeConnection
        An active Snowflake connection.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FWA"),
    )


def insertVariantRows(tableFqn: str, rows: list[dict], source: str):
    """
    Insert raw records into a Snowflake RAW table using a VARIANT payload.

    Each record is serialized to JSON and stored in the `payload` column.
    Metadata columns (`source`, `ingested_at`) are automatically populated.

    Parameters
    ----------
    tableFqn : str
        Fully qualified table name (e.g. 'FWA.RAW.CSV_MATCHES').
    rows : list[dict]
        List of raw records to insert.
    source : str
        Source identifier (e.g. 'CSV', 'S3', 'API').

    Notes
    -----
    - Intended for RAW-layer ingestion only.
    - No transformation or validation is applied.
    - Ingestion timestamp is generated in UTC.
    """
    ingestedAt = datetime.now(timezone.utc).replace(tzinfo=None)

    with sfConnect() as conn, conn.cursor() as cur:
        for record in rows:
            cur.execute(
                f"""
                INSERT INTO {tableFqn} (source, ingested_at, payload)
                SELECT %s, %s, PARSE_JSON(%s)
                """,
                (
                    source,
                    ingestedAt,
                    json.dumps(record, ensure_ascii=False),
                ),
            )
