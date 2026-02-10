-- Table: rental - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:25.642119

CREATE TABLE "rental" (
    "rental_id" SERIAL PRIMARY KEY,
    "rental_date" TIMESTAMP NOT NULL,
    "inventory_id" INTEGER NOT NULL,
    "customer_id" SMALLINT NOT NULL,
    "return_date" TIMESTAMP,
    "staff_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("rental_date", "inventory_id", "customer_id")
);