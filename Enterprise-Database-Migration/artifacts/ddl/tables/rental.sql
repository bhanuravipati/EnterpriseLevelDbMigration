-- Table: rental - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:41.788168
=======
-- Generated: 2026-01-27T15:51:09.854320
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

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