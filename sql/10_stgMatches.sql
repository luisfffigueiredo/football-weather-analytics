USE DATABASE FWA;

CREATE OR REPLACE TABLE STG.MATCHES AS
SELECT
  TRY_TO_DATE(payload:Date::string)                 AS match_date,
  payload:HomeTeam::string                          AS home_team,
  payload:AwayTeam::string                          AS away_team,
  TRY_TO_NUMBER(payload:FTHG::string)               AS home_goals,
  TRY_TO_NUMBER(payload:FTAG::string)               AS away_goals,
  payload:Div::string                               AS division,
  payload:Season::string                            AS season,
  payload:Referee::string                           AS referee,
  source                                            AS source,
  ingested_at                                       AS ingested_at
FROM RAW.CSV_MATCHES
WHERE TRY_TO_DATE(payload:Date::string) IS NOT NULL
  AND payload:HomeTeam::string IS NOT NULL
  AND payload:AwayTeam::string IS NOT NULL;