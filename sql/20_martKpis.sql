USE DATABASE FWA;

CREATE OR REPLACE TABLE MART.KPIS AS
WITH base AS (
  SELECT
    match_date,
    home_team AS team,
    home_goals AS goals_for,
    away_goals AS goals_against,
    CASE
      WHEN home_goals > away_goals THEN 3
      WHEN home_goals = away_goals THEN 1
      ELSE 0
    END AS points
  FROM STG.MATCHES

  UNION ALL

  SELECT
    match_date,
    away_team AS team,
    away_goals AS goals_for,
    home_goals AS goals_against,
    CASE
      WHEN away_goals > home_goals THEN 3
      WHEN away_goals = home_goals THEN 1
      ELSE 0
    END AS points
  FROM STG.MATCHES
)
SELECT
  team,
  COUNT(*)                                   AS matches,
  SUM(points)                                 AS points,
  SUM(goals_for)                              AS goals_for,
  SUM(goals_against)                          AS goals_against,
  ROUND(AVG(points), 3)                       AS points_per_match
FROM base
GROUP BY 1
ORDER BY points DESC;
