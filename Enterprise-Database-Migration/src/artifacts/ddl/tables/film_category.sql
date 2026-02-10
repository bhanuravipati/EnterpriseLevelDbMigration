-- Table: film_category - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:59.196704

CREATE TABLE "film_category" (
    "film_id" SMALLINT NOT NULL,
    "category_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("film_id", "category_id")
);