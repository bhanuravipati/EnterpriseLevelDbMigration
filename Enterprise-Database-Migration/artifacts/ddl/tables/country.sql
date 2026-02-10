-- Table: country - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:20.000374
=======
-- Generated: 2026-01-27T15:50:51.550220
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "country" (
    "country_id" SMALLSERIAL PRIMARY KEY,
    "country" VARCHAR(50) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);