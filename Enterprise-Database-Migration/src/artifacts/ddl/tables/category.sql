-- Table: category - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:07.235322

CREATE TABLE "category" (
    "category_id" SMALLSERIAL PRIMARY KEY,
    "name" VARCHAR(25) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);