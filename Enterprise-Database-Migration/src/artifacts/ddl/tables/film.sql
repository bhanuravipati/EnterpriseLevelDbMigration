-- Table: film - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:39.452043

CREATE TABLE "film" (
    "film_id" SMALLSERIAL PRIMARY KEY,
    "title" VARCHAR(128) NOT NULL,
    "description" TEXT,
    "release_year" SMALLINT,
    "language_id" SMALLINT NOT NULL,
    "original_language_id" SMALLINT,
    "rental_duration" SMALLINT NOT NULL DEFAULT 3,
    "rental_rate" NUMERIC(4,2) NOT NULL DEFAULT 4.99,
    "length" SMALLINT,
    "replacement_cost" NUMERIC(5,2) NOT NULL DEFAULT 19.99,
    "rating" TEXT DEFAULT 'G' CHECK ("rating" IN ('G','PG','PG-13','R','NC-17')),
    "special_features" TEXT[],
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);