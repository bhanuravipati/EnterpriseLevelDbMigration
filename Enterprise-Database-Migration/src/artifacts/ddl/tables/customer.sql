-- Table: customer - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:11:21.428243

CREATE TABLE "customer" (
    "customer_id" SMALLSERIAL PRIMARY KEY,
    "store_id" SMALLINT NOT NULL,
    "first_name" VARCHAR(45) NOT NULL,
    "last_name" VARCHAR(45) NOT NULL,
    "email" VARCHAR(50),
    "address_id" SMALLINT NOT NULL,
    "active" SMALLINT NOT NULL DEFAULT 1,
    "create_date" TIMESTAMP NOT NULL,
    "last_update" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);