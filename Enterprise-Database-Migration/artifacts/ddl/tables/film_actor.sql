-- Table: film_actor - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:50.079863
=======
-- Generated: 2026-01-27T15:50:57.548617
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "film_actor" (
    "actor_id" SMALLINT NOT NULL,
    "film_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("actor_id", "film_id")
);