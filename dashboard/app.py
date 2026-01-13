import os
import html
import pandas as pd
import streamlit as st
import snowflake.connector
import streamlit.components.v1 as components
from dotenv import load_dotenv

TEAM_LOGOS = {
    "Benfica": "https://upload.wikimedia.org/wikipedia/en/a/a2/SL_Benfica_logo.svg",
    "Porto": "https://upload.wikimedia.org/wikipedia/pt/c/c5/F.C._Porto_logo.png",
    "Sp Lisbon": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/76/Sporting_logo.png/960px-Sporting_logo.png",
    "Sp Braga": "https://upload.wikimedia.org/wikipedia/pt/f/f9/150px-Sporting_Clube_Braga.png",
    "Guimaraes": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d5/Vit%C3%B3ria_Guimar%C3%A3es.svg/250px-Vit%C3%B3ria_Guimar%C3%A3es.svg.png",
    "Boavista": "https://upload.wikimedia.org/wikipedia/pt/5/5c/Logo_Boavista_FC.png",
    "Gil Vicente": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8f/Gil_Vicente_F.C.png/250px-Gil_Vicente_F.C.png",
    "Famalicao": "https://upload.wikimedia.org/wikipedia/pt/6/61/Escudo_Famalic%C3%A3o.png",
    "Arouca": "https://upload.wikimedia.org/wikipedia/pt/8/84/2310.png",
    "Rio Ave": "https://upload.wikimedia.org/wikipedia/pt/f/f5/Logo_Rio_Ave.png",
    "Portimonense": "https://upload.wikimedia.org/wikipedia/pt/1/1c/Logo_Portimonense.png",
    "Estoril": "https://upload.wikimedia.org/wikipedia/de/thumb/1/14/GD_Estoril_Praia.svg/500px-GD_Estoril_Praia.svg.png",
    "Vizela": "https://upload.wikimedia.org/wikipedia/pt/b/b9/Futebol_Clube_de_Vizela.png",
    "Chaves": "https://upload.wikimedia.org/wikipedia/pt/0/05/G_D_Chaves.png",
    "Casa Pia": "https://upload.wikimedia.org/wikipedia/en/4/4d/Casa_Pia_A.C._logo.png",
    "Maritimo": "https://upload.wikimedia.org/wikipedia/pt/a/a2/Logo_CS_Maritimo.png",
    "Pacos Ferreira": "https://upload.wikimedia.org/wikipedia/pt/3/3d/Futebol_Clube_Pa%C3%A7os_de_Ferreira.png",
    "Santa Clara": "https://upload.wikimedia.org/wikipedia/pt/b/bc/Logo_Santa_Clara.png",
}


def sfConnect():
    """
    Create a Snowflake connection using environment variables.

    Returns
    -------
    snowflake.connector.connection.SnowflakeConnection
        An active Snowflake connection to the configured account/database.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FWA"),
    )


def normalizeColumns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame column names to lowercase for consistent access.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame.

    Returns
    -------
    pandas.DataFrame
        Copy of the DataFrame with lowercase column names.
    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=300)
def loadStandingsDf(seasonStart: str, seasonEnd: str) -> pd.DataFrame:
    """
    Load final league standings computed from STG.MATCHES for a given date range.

    Parameters
    ----------
    seasonStart : str
        Lower bound date (YYYY-MM-DD).
    seasonEnd : str
        Upper bound date (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        Standings with 1-based position plus points, goals and points per game.
    """
    query = """
    WITH base AS (
      SELECT
        match_date,
        home_team AS team,
        home_goals AS goals_for,
        away_goals AS goals_against,
        CASE WHEN home_goals > away_goals THEN 3
             WHEN home_goals = away_goals THEN 1
             ELSE 0 END AS points
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s

      UNION ALL

      SELECT
        match_date,
        away_team AS team,
        away_goals AS goals_for,
        home_goals AS goals_against,
        CASE WHEN away_goals > home_goals THEN 3
             WHEN away_goals = home_goals THEN 1
             ELSE 0 END AS points
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s
    ),
    agg AS (
      SELECT
        team,
        COUNT(*) AS matches,
        SUM(points) AS points,
        SUM(goals_for) AS goals_for,
        SUM(goals_against) AS goals_against,
        SUM(goals_for) - SUM(goals_against) AS goal_diff,
        (SUM(points) * 1.0) / NULLIF(COUNT(*), 0) AS points_per_game
      FROM base
      GROUP BY 1
    )
    SELECT *
    FROM agg
    ORDER BY points DESC, goal_diff DESC, goals_for DESC, team ASC
    """
    with sfConnect() as conn:
        df = pd.read_sql(query, conn, params=[seasonStart, seasonEnd, seasonStart, seasonEnd])

    df = normalizeColumns(df)
    df.insert(0, "position", range(1, len(df) + 1))
    return df


@st.cache_data(ttl=300)
def loadPpgTimeSeriesDf(seasonStart: str, seasonEnd: str) -> pd.DataFrame:
    """
    Load a per-team time series of cumulative points-per-game (PPG) over time.

    Parameters
    ----------
    seasonStart : str
        Lower bound date (YYYY-MM-DD).
    seasonEnd : str
        Upper bound date (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        Long-form DataFrame with columns: match_date, team, points_per_game.
    """
    query = """
    WITH base AS (
      SELECT
        match_date,
        home_team AS team,
        CASE WHEN home_goals > away_goals THEN 3
             WHEN home_goals = away_goals THEN 1
             ELSE 0 END AS points
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s

      UNION ALL

      SELECT
        match_date,
        away_team AS team,
        CASE WHEN away_goals > home_goals THEN 3
             WHEN away_goals = home_goals THEN 1
             ELSE 0 END AS points
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s
    ),
    ordered AS (
      SELECT
        team,
        match_date,
        points,
        SUM(points) OVER (
          PARTITION BY team
          ORDER BY match_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_points,
        COUNT(*) OVER (
          PARTITION BY team
          ORDER BY match_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_matches
      FROM base
    )
    SELECT
      match_date,
      team,
      (cum_points * 1.0) / NULLIF(cum_matches, 0) AS points_per_game
    FROM ordered
    ORDER BY match_date, team
    """
    with sfConnect() as conn:
        df = pd.read_sql(query, conn, params=[seasonStart, seasonEnd, seasonStart, seasonEnd])

    df = normalizeColumns(df)
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
    return df.dropna(subset=["match_date"])


@st.cache_data(ttl=300)
def loadTeamMonthlyGoalsDf(team: str, seasonStart: str, seasonEnd: str) -> pd.DataFrame:
    """
    Load monthly goals scored and conceded for a single team.

    Parameters
    ----------
    team : str
        Team name (as stored in STG.MATCHES).
    seasonStart : str
        Lower bound date (YYYY-MM-DD).
    seasonEnd : str
        Upper bound date (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns: month, goals_scored, goals_conceded.
    """
    query = """
    WITH base AS (
      SELECT
        match_date,
        home_team AS team,
        home_goals AS goals_scored,
        away_goals AS goals_conceded
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s

      UNION ALL

      SELECT
        match_date,
        away_team AS team,
        away_goals AS goals_scored,
        home_goals AS goals_conceded
      FROM STG.MATCHES
      WHERE match_date BETWEEN %s AND %s
    )
    SELECT
      DATE_TRUNC('month', match_date) AS month,
      SUM(goals_scored) AS goals_scored,
      SUM(goals_conceded) AS goals_conceded
    FROM base
    WHERE team = %s
    GROUP BY 1
    ORDER BY 1
    """
    with sfConnect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params=[seasonStart, seasonEnd, seasonStart, seasonEnd, team],
        )

    df = normalizeColumns(df)
    df["month"] = pd.to_datetime(df["month"], errors="coerce")
    return df.dropna(subset=["month"])


def pivotTimeSeries(df: pd.DataFrame, dateCol: str, teamCol: str, valueCol: str) -> pd.DataFrame:
    """
    Pivot a long-form time series (date, team, value) into a wide format for Streamlit charts.

    Returns
    -------
    pandas.DataFrame
        Wide-form DataFrame with date index and team columns.
    """
    return (
        df.pivot_table(index=dateCol, columns=teamCol, values=valueCol, aggfunc="last")
        .sort_index()
    )


def sortStandingsDf(standingsDf: pd.DataFrame, sortBy: str, descending: bool) -> pd.DataFrame:
    """
    Sort standings by a user-selected column and re-generate 1-based positions.

    Notes
    -----
    Sorting changes the displayed 'position' to reflect the chosen ordering.
    """
    df = standingsDf.copy()

    if sortBy == "Official position":
        df = df.sort_values(["position"], ascending=[True])
    elif sortBy == "Team":
        df = df.sort_values(["team"], ascending=[not descending])
    else:
        colMap = {
            "Points": "points",
            "Points per game (PPG)": "points_per_game",
            "Matches played": "matches",
            "Goals for": "goals_for",
            "Goals against": "goals_against",
            "Goal difference": "goal_diff",
        }
        col = colMap[sortBy]
        df = df.sort_values([col, "team"], ascending=[not descending, True])

    df = df.reset_index(drop=True)
    df["position"] = range(1, len(df) + 1)
    return df


def buildStandingsHtml(standingsDf: pd.DataFrame, relegationFromPos: int = 16) -> str:
    """
    Build an HTML table for standings with:
      - sticky header
      - champion highlight (gold)
      - relegation zone highlight (red)
      - explicit white background for normal rows (theme-safe)
      - team logos displayed left-to-right next to team names
    """
    styles = """
    <style>
      .standings-wrap { max-height: 560px; overflow: auto; border: 1px solid #e6e6e6; border-radius: 10px; }

      table.standings {
        border-collapse: collapse;
        width: 100%;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
        background: #ffffff;
      }

      table.standings th, table.standings td {
        padding: 10px 12px;
        border-bottom: 1px solid #f0f0f0;
        white-space: nowrap;
        background: #ffffff; /* default: white background for ALL normal rows */
        color: #111111;
      }

      table.standings th {
        position: sticky;
        top: 0;
        background: #ffffff;
        z-index: 2;
        text-align: left;
        border-bottom: 2px solid #e6e6e6;
      }

      table.standings tr:hover td { background: #fafafa; }
      .num { text-align: right; }
      .pos { width: 52px; }
      .teamCell { display: flex; align-items: center; gap: 10px; }
      .logo { width: 22px; height: 22px; object-fit: contain; }

      .champion td { background: #fff4cc !important; }   /* gold-ish */
      .relegation td { background: #ffe0e0 !important; } /* red-ish */
    </style>
    """

    header = """
    <div class="standings-wrap">
      <table class="standings">
        <thead>
          <tr>
            <th class="pos">Pos</th>
            <th>Team</th>
            <th class="num">Pts</th>
            <th class="num">MP</th>
            <th class="num">GF</th>
            <th class="num">GA</th>
            <th class="num">GD</th>
            <th class="num">PPG</th>
          </tr>
        </thead>
        <tbody>
    """

    rowsHtml = []
    for _, r in standingsDf.iterrows():
        pos = int(r["position"])
        team = str(r["team"])
        logo = TEAM_LOGOS.get(team)

        rowClass = ""
        if pos == 1:
            rowClass = ' class="champion"'
        elif pos >= relegationFromPos:
            rowClass = ' class="relegation"'

        teamSafe = html.escape(team)
        logoHtml = f'<img class="logo" src="{html.escape(logo)}" />' if logo else ""

        rowsHtml.append(
            f"""
            <tr{rowClass}>
              <td class="pos num">{pos}</td>
              <td>
                <div class="teamCell">
                  {logoHtml}
                  <span>{teamSafe}</span>
                </div>
              </td>
              <td class="num">{int(r["points"])}</td>
              <td class="num">{int(r["matches"])}</td>
              <td class="num">{int(r["goals_for"])}</td>
              <td class="num">{int(r["goals_against"])}</td>
              <td class="num">{int(r["goal_diff"])}</td>
              <td class="num">{float(r["points_per_game"]):.2f}</td>
            </tr>
            """
        )

    footer = """
        </tbody>
      </table>
    </div>
    """

    return styles + header + "\n".join(rowsHtml) + footer


def main():
    """
    Render the Streamlit dashboard with:
      - Tab 1: Standings (logos + champion/relegation styling + sticky header + sortable)
      - Tab 1: PPG over time (all teams or selected team)
      - Tab 2: Monthly goals scored and conceded (bar charts) for a selected team
    """
    load_dotenv()
    st.title("Primeira Liga 2022/23 — Simple Example without dbt ")

    seasonStart = st.sidebar.text_input("Season start (YYYY-MM-DD)", "2022-08-05")
    seasonEnd = st.sidebar.text_input("Season end (YYYY-MM-DD)", "2023-05-27")

    tabs = st.tabs(["Standings", "Goals"])

    with tabs[0]:
        baseStandingsDf = loadStandingsDf(seasonStart, seasonEnd)

        st.subheader("Final standings")

        sortCol1, sortCol2, sortCol3 = st.columns([2.4, 1.6, 2.0])
        sortBy = sortCol1.selectbox(
            "Sort by",
            [
                "Official position",
                "Points",
                "Points per game (PPG)",
                "Goals for",
                "Goals against",
                "Goal difference",
                "Matches played",
                "Team",
            ],
            index=0,
        )
        descending = sortCol2.checkbox("Descending", value=False)
        relegationFromPos = sortCol3.selectbox(
            "Relegation zone starts at position",
            [16, 17, 18],
            index=0,
            help="Primeira Liga typically has bottom 2 relegated and 16th in playoff. Choose the zone you want to highlight.",
        )

        standingsDf = sortStandingsDf(baseStandingsDf, sortBy, descending)
        htmlTable = buildStandingsHtml(standingsDf, relegationFromPos=relegationFromPos)
        components.html(htmlTable, height=620, scrolling=False)

        st.markdown("### Points per game (PPG) over time")
        teamOptions = ["All teams"] + baseStandingsDf["team"].tolist()
        selectedTeam = st.selectbox("Show PPG for:", teamOptions)

        ppgDf = loadPpgTimeSeriesDf(seasonStart, seasonEnd)
        if selectedTeam != "All teams":
            ppgDf = ppgDf[ppgDf["team"] == selectedTeam]

        ppgWideDf = pivotTimeSeries(ppgDf, "match_date", "team", "points_per_game")
        st.line_chart(ppgWideDf)

    with tabs[1]:
        standingsDf = loadStandingsDf(seasonStart, seasonEnd)

        st.subheader("Monthly goals")
        selectedTeam = st.selectbox("Team", standingsDf["team"].tolist(), key="goalsTeam")

        monthlyDf = loadTeamMonthlyGoalsDf(selectedTeam, seasonStart, seasonEnd).set_index("month")
        monthlyDf = monthlyDf.sort_index()

        st.markdown("#### Goals conceded per month")
        st.bar_chart(monthlyDf[["goals_conceded"]])

        st.markdown("#### Goals scored per month")
        st.bar_chart(monthlyDf[["goals_scored"]])


if __name__ == "__main__":
    main()
