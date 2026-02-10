-- View: actor_info
-- Generated: 2026-01-23T15:14:13.087810

CREATE VIEW "actor_info" AS
WITH actor_cat AS (
    SELECT a."actor_id",
           a."first_name",
           a."last_name",
           c."name" AS cat_name,
           STRING_AGG(f."title", ', ' ORDER BY f."title") AS titles
    FROM "actor" a
    LEFT JOIN "film_actor" fa ON a."actor_id" = fa."actor_id"
    LEFT JOIN "film_category" fc ON fa."film_id" = fc."film_id"
    LEFT JOIN "category" c ON fc."category_id" = c."category_id"
    LEFT JOIN "film" f ON fc."film_id" = f."film_id"
    GROUP BY a."actor_id", a."first_name", a."last_name", c."name"
)
SELECT actor_id,
       first_name,
       last_name,
       STRING_AGG(cat_name || ': ' || titles, '; ' ORDER BY cat_name) AS film_info
FROM actor_cat
GROUP BY actor_id, first_name, last_name;