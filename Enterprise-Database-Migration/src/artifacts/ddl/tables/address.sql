-- Table: address - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:06.245502

CREATE TABLE "address" (
    "address_id" SMALLSERIAL PRIMARY KEY,
    "address" VARCHAR(50) NOT NULL,
    "address2" VARCHAR(50),
    "district" VARCHAR(20) NOT NULL,
    "city_id" SMALLINT NOT NULL,
    "postal_code" VARCHAR(10),
    "phone" VARCHAR(20) NOT NULL,
    "location" POINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);