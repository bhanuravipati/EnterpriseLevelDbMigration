-- Table: city - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:03.542013
=======
-- Generated: 2026-01-27T15:50:49.800354
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "city" (
    "city_id" SMALLSERIAL PRIMARY KEY,
    "city" VARCHAR(50) NOT NULL,
    "country_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);