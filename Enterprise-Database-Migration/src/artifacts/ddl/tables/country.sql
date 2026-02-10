-- Table: country - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:15.981548

CREATE TABLE "country" (
    "country_id" SMALLSERIAL PRIMARY KEY,
    "country" VARCHAR(50) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);