-- View: nicer_but_slower_film_list
-- Generated: 2026-01-23T15:13:18.519488

CREATE VIEW nicer_but_slower_film_list AS
SELECT f.film_id AS "FID",
       f.title,
       f.description,
       c.name AS category,
       f.rental_rate AS price,
       f.length,
       f.rating,
       STRING_AGG(initcap(a.first_name) || ' ' || initcap(a.last_name), ', ' ORDER BY initcap(a.first_name) || ' ' || initcap(a.last_name)) AS actors
FROM film f
LEFT JOIN film_category fc ON fc.film_id = f.film_id
LEFT JOIN category c ON c.category_id = fc.category_id
LEFT JOIN film_actor fa ON fa.film_id = f.film_id
LEFT JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY f.film_id, c.name;