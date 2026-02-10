-- Table: payment - MySQL to PostgreSQL
-- Generated: 2026-01-23T15:12:19.074271

CREATE TABLE "payment" (
    "payment_id" SMALLSERIAL PRIMARY KEY,
    "customer_id" SMALLINT NOT NULL,
    "staff_id" SMALLINT NOT NULL,
    "rental_id" INTEGER,
    "amount" NUMERIC(5,2) NOT NULL,
    "payment_date" TIMESTAMP NOT NULL,
    "last_update" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);