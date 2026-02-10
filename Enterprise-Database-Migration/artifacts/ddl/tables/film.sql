-- Table: film - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:34.032053
=======
-- Generated: 2026-01-27T15:50:56.753734
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

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
<<<<<<< HEAD
    "rating" TEXT NOT NULL DEFAULT 'G',
=======
    "rating" TEXT DEFAULT 'G' CHECK ("rating" IN ('G','PG','PG-13','R','NC-17')),
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    "special_features" TEXT[],
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (rating IN ('G','PG','PG-13','R','NC-17'))
);