-- View: customer_list
-- Generated: 2026-01-23T15:12:58.145925

CREATE OR REPLACE VIEW "customer_list" AS
SELECT
    cu.customer_id AS "ID",
    (cu.first_name || ' ' || cu.last_name) AS "name",
    a.address AS "address",
    a.postal_code AS "zip code",
    a.phone AS "phone",
    city.city AS "city",
    country.country AS "country",
    CASE WHEN cu.active = 1 THEN 'active' ELSE '' END AS "notes",
    cu.store_id AS "SID"
FROM customer cu
JOIN address a ON cu.address_id = a.address_id
JOIN city ON a.city_id = city.city_id
JOIN country ON city.country_id = country.country_id;