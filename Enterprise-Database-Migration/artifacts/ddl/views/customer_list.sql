-- View: customer_list
<<<<<<< HEAD
-- Generated: 2026-02-02T15:13:25.964096
=======
-- Generated: 2026-01-27T15:51:31.146402
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE VIEW customer_list AS
SELECT
    cu."customer_id" AS "ID",
    cu."first_name" || ' ' || cu."last_name" AS "name",
    a."address" AS "address",
    a."postal_code" AS "zip code",
    a."phone" AS "phone",
    city."city" AS "city",
    country."country" AS "country",
    CASE WHEN cu."active" = 1 THEN 'active' ELSE '' END AS "notes",
    cu."store_id" AS "SID"
FROM "customer" cu
JOIN "address" a ON cu."address_id" = a."address_id"
JOIN "city" city ON a."city_id" = city."city_id"
JOIN "country" country ON city."country_id" = country."country_id";