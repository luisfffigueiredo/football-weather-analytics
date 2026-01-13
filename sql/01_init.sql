create database if not exists fWA;

use database fWA;

create schema if not exists RAW;

create schema if not exists STG;

create schema if not exists MART;

USE DATABASE FWA;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.CSV_MATCHES (
  source STRING,
  ingested_at TIMESTAMP_NTZ,
  payload VARIANT
);

CREATE OR REPLACE TABLE RAW.NOAA_DAILY (
  source STRING,
  ingested_at TIMESTAMP_NTZ,
  payload VARIANT
);