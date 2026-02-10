-- Table: film_text - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:03.947739

CREATE TABLE "film_text" (
    "film_id" SMALLINT NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "description" TEXT,
    PRIMARY KEY ("film_id")
);