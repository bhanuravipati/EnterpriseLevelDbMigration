-- Table: language - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:12.706106

CREATE TABLE "language" (
    "language_id" SMALLSERIAL PRIMARY KEY,
    "name" CHAR(20) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);