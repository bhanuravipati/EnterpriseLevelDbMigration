-- Table: customer - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:25.891647
=======
-- Generated: 2026-01-27T15:50:53.732823
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

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