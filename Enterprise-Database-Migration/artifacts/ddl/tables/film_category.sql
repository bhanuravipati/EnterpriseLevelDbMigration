-- Table: film_category - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:05.202019
=======
-- Generated: 2026-01-27T15:50:58.557103
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "film_category" (
    "film_id" SMALLINT NOT NULL,
    "category_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("film_id", "category_id")
);