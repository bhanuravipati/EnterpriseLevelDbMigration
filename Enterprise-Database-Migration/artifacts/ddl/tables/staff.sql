-- Table: staff - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:49.877424
=======
-- Generated: 2026-01-27T15:51:17.563995
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "staff" (
    "staff_id" SMALLSERIAL PRIMARY KEY,
    "first_name" VARCHAR(45) NOT NULL,
    "last_name" VARCHAR(45) NOT NULL,
    "address_id" SMALLINT NOT NULL,
    "picture" BYTEA,
    "email" VARCHAR(50),
    "store_id" SMALLINT NOT NULL,
    "active" SMALLINT NOT NULL DEFAULT 1,
    "username" VARCHAR(16) NOT NULL,
    "password" VARCHAR(40),
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);