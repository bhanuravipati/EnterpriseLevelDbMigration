-- Table: actor - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:04.524888

CREATE TABLE "actor" (
    "actor_id" SMALLSERIAL PRIMARY KEY,
    "first_name" VARCHAR(45) NOT NULL,
    "last_name" VARCHAR(45) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);