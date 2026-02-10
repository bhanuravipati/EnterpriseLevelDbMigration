-- Table: address - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:10:58.418544
=======
-- Generated: 2026-01-27T15:50:47.945374
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

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