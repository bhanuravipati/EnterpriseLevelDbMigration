-- Table: store - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:50.747360

CREATE TABLE "store" (
    "store_id" SMALLSERIAL PRIMARY KEY,
    "manager_staff_id" SMALLINT NOT NULL,
    "address_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("manager_staff_id")
);