-- Table: film_actor - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:55.236619

CREATE TABLE "film_actor" (
    "actor_id" SMALLINT NOT NULL,
    "film_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("actor_id", "film_id")
);