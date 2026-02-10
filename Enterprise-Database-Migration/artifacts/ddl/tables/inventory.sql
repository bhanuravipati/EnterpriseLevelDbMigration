-- Table: inventory - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:14.794195
=======
-- Generated: 2026-01-27T15:51:01.321012
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "inventory" (
    "inventory_id" SERIAL PRIMARY KEY,
    "film_id" SMALLINT NOT NULL,
    "store_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);