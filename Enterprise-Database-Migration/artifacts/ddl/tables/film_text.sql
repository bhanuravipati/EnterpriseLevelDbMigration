-- Table: film_text - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:09.452259
=======
-- Generated: 2026-01-27T15:50:59.838751
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "film_text" (
    "film_id" SMALLINT NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "description" TEXT,
    PRIMARY KEY ("film_id")
);