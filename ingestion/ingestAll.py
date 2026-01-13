from dotenv import load_dotenv
from csvFootball import ingestCsvMatches


def main():
    """
    Orchestrate ingestion of all raw data sources.

    This function represents the ingestion happy path and is intended
    to be executed via a single command (e.g. `make ingest`).

    Current sources:
        - Football match data from public CSV files
        - Daily weather data from NOAA public S3

    Notes
    -----
    - Environment variables are loaded from `.env`.
    - Each source is ingested independently.
    - Error handling and retries can be added per source.
    """
    load_dotenv()
    ingestCsvMatches()



if __name__ == "__main__":
    main()
    print("OK: ingest done")
