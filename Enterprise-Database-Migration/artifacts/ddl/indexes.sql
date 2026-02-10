-- Database Indexes (Two-Pass approach)
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:30.300398
=======
-- Generated: 2026-01-27T15:52:30.549528
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE INDEX "idx_actor_last_name" ON "actor" ("last_name");
CREATE INDEX "idx_address_city_id" ON "address" ("city_id");
CREATE INDEX "idx_address_location" ON "address" ("location");
CREATE INDEX "idx_city_country_id" ON "city" ("country_id");
CREATE INDEX "idx_customer_address_id" ON "customer" ("address_id");
CREATE INDEX "idx_customer_store_id" ON "customer" ("store_id");
CREATE INDEX "idx_customer_last_name" ON "customer" ("last_name");
CREATE INDEX "idx_film_language_id" ON "film" ("language_id");
CREATE INDEX "idx_film_original_language_id" ON "film" ("original_language_id");
CREATE INDEX "idx_film_title" ON "film" ("title");
CREATE INDEX "idx_film_actor_film_id" ON "film_actor" ("film_id");
CREATE INDEX "fk_film_category_category" ON "film_category" ("category_id");
CREATE INDEX "idx_film_text_title_description" ON "film_text" ("title", "description");
CREATE INDEX "idx_inventory_film_id" ON "inventory" ("film_id");
CREATE INDEX "idx_inventory_store_id_film_id" ON "inventory" ("store_id", "film_id");
CREATE INDEX "fk_payment_rental" ON "payment" ("rental_id");
CREATE INDEX "idx_payment_customer_id" ON "payment" ("customer_id");
CREATE INDEX "idx_payment_staff_id" ON "payment" ("staff_id");
CREATE INDEX "idx_rental_customer_id" ON "rental" ("customer_id");
CREATE INDEX "idx_rental_inventory_id" ON "rental" ("inventory_id");
CREATE INDEX "idx_rental_staff_id" ON "rental" ("staff_id");
CREATE UNIQUE INDEX "rental_date" ON "rental" ("rental_date", "inventory_id", "customer_id");
CREATE INDEX "idx_staff_address_id" ON "staff" ("address_id");
CREATE INDEX "idx_staff_store_id" ON "staff" ("store_id");
CREATE INDEX "idx_store_address_id" ON "store" ("address_id");
CREATE UNIQUE INDEX "idx_store_unique_manager" ON "store" ("manager_staff_id");