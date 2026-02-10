-- Table: store - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:57.705344
=======
-- Generated: 2026-01-27T15:51:21.907866
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "store" (
    "store_id" SMALLSERIAL PRIMARY KEY,
    "manager_staff_id" SMALLINT NOT NULL,
    "address_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("manager_staff_id")
);