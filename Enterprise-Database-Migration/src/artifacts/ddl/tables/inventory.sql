-- Table: inventory - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:08.937068

CREATE TABLE "inventory" (
    "inventory_id" SERIAL PRIMARY KEY,
    "film_id" SMALLINT NOT NULL,
    "store_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);