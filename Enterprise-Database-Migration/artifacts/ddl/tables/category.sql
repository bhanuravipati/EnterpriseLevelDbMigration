-- Table: category - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:11:00.163676
=======
-- Generated: 2026-01-27T15:50:48.625197
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "category" (
    "category_id" SMALLSERIAL PRIMARY KEY,
    "name" VARCHAR(25) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);