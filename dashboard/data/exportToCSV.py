
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

def sfConnect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FWA"),
    )

def export(query: str, outputPath: str):
    with sfConnect() as conn:
        df = pd.read_sql(query, conn)
    df.to_csv(outputPath, index=False)
    print(f"Exported {outputPath}")

def main():
    load_dotenv()

    export(
        """
        SELECT * FROM MART.TEAM_STANDINGS
        """,
        "dashboard/data/team_standings_2022_23.csv",
    )

    export(
        """
        SELECT * FROM MART.TEAM_PPG_TIMESERIES
        """,
        "dashboard/data/ppg_timeseries_2022_23.csv",
    )

    export(
        """
        SELECT * FROM MART.TEAM_MONTHLY_GOALS
        """,
        "dashboard/data/monthly_goals_2022_23.csv",
    )

if __name__ == "__main__":
    main()
