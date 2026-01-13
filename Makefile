.PHONY: setup ingest transform dashboard

setup:
	python -m pip install -r requirements.txt
	snowsql -c $$SNOWSQL_CONN -f sql/00_setup.sql

ingest:
	python ingestion/ingest_all.py

transform:
	snowsql -c $$SNOWSQL_CONN -f sql/10_stg_matches.sql
	snowsql -c $$SNOWSQL_CONN -f sql/11_stg_weather_daily.sql
	snowsql -c $$SNOWSQL_CONN -f sql/20_mart_team_daily_form.sql
	snowsql -c $$SNOWSQL_CONN -f sql/21_mart_match_weather_impact.sql
	snowsql -c $$SNOWSQL_CONN -f sql/22_mart_kpis.sql

dashboard:
	streamlit run dashboard/app.py
