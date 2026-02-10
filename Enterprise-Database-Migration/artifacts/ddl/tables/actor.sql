-- Table: actor - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:10:56.813986
=======
-- Generated: 2026-01-27T15:50:45.767850
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "actor" (
    "actor_id" SMALLSERIAL PRIMARY KEY,
    "first_name" VARCHAR(45) NOT NULL,
    "last_name" VARCHAR(45) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);