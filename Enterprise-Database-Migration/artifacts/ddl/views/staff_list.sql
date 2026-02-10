-- View: staff_list
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:30.167141

CREATE OR REPLACE VIEW staff_list AS
SELECT
    s.staff_id AS "ID",
    CONCAT(s.first_name, ' ', s.last_name) AS "name",
    a.address AS "address",
    a.postal_code AS "zip code",
    a.phone AS "phone",
    city.city AS "city",
    country.country AS "country",
    s.store_id AS "SID"
=======
-- Generated: 2026-01-27T15:52:01.722007

CREATE OR REPLACE VIEW staff_list AS
SELECT s.staff_id AS "ID",
       CONCAT(s.first_name, ' ', s.last_name) AS "name",
       a.address AS "address",
       a.postal_code AS "zip code",
       a.phone AS "phone",
       city.city AS "city",
       country.country AS "country",
       s.store_id AS "SID"
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
FROM staff s
JOIN address a ON s.address_id = a.address_id
JOIN city ON a.city_id = city.city_id
JOIN country ON city.country_id = country.country_id;