-- Table: staff - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:43.568196

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