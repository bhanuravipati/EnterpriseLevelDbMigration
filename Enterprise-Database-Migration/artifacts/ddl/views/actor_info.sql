-- View: actor_info
<<<<<<< HEAD
-- Generated: 2026-02-02T15:13:10.447025

CREATE VIEW actor_info AS
SELECT
    a.actor_id,
    a.first_name,
    a.last_name,
    fi.film_info
FROM actor a
LEFT JOIN LATERAL (
    SELECT STRING_AGG(cat_info, '; ' ORDER BY cat_name) AS film_info
    FROM (
        SELECT DISTINCT
            c.category_id,
            c.name AS cat_name,
            c.name || ': ' ||
            (
                SELECT STRING_AGG(f.title, ', ' ORDER BY f.title)
                FROM film f
                JOIN film_category fc2 ON f.film_id = fc2.film_id
                WHERE fc2.category_id = c.category_id
                  AND EXISTS (
                      SELECT 1
                      FROM film_actor fa2
                      WHERE fa2.film_id = f.film_id
                        AND fa2.actor_id = a.actor_id
                  )
            ) AS cat_info
        FROM film_actor fa
        JOIN film_category fc ON fa.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE fa.actor_id = a.actor_id
    ) sub
) fi ON true;
=======
-- Generated: 2026-01-27T15:52:30.539944

CREATE OR REPLACE VIEW actor_info AS
SELECT a.actor_id,
       a.first_name,
       a.last_name,
       (
         SELECT STRING_AGG(cat_str, '; ' ORDER BY cat_str)
         FROM (
           SELECT c.name || ': ' || STRING_AGG(f.title, ', ' ORDER BY f.title) AS cat_str
           FROM category c
           JOIN film_category fc ON c.category_id = fc.category_id
           JOIN film_actor fa ON fc.film_id = fa.film_id
           JOIN film f ON fc.film_id = f.film_id
           WHERE fa.actor_id = a.actor_id
           GROUP BY c.category_id, c.name
         ) sub
       ) AS film_info
FROM actor a;
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
