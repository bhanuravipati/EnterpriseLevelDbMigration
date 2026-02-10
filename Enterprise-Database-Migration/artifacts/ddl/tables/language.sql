-- Table: language - MySQL to PostgreSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:12:19.613431
=======
-- Generated: 2026-01-27T15:51:02.597083
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE TABLE "language" (
    "language_id" SMALLSERIAL PRIMARY KEY,
    "name" CHAR(20) NOT NULL,
    "last_update" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);