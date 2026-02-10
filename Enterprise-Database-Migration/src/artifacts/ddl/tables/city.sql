-- Table: city - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:11.143000

CREATE TABLE "city" (
    "city_id" SMALLSERIAL PRIMARY KEY,
    "city" VARCHAR(50) NOT NULL,
    "country_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);