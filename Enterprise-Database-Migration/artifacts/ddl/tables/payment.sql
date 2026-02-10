-- Table: payment - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:24.240134
=======
-- Generated: 2026-01-27T15:51:04.160926
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "payment" (
    "payment_id" SMALLSERIAL PRIMARY KEY,
    "customer_id" SMALLINT NOT NULL,
    "staff_id" SMALLINT NOT NULL,
    "rental_id" INTEGER,
    "amount" NUMERIC(5,2) NOT NULL,
    "payment_date" TIMESTAMP NOT NULL,
    "last_update" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);