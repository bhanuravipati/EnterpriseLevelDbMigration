-- View: film_list
-- Generated: 2026-01-23T15:13:08.898763

CREATE OR REPLACE VIEW film_list AS
SELECT
    film.film_id AS "FID",
    film.title,
    film.description,
    category.name AS category,
    film.rental_rate AS price,
    film.length,
    film.rating,
    STRING_AGG(actor.first_name || ' ' || actor.last_name, ', ') AS actors
FROM film
LEFT JOIN film_category ON film_category.film_id = film.film_id
LEFT JOIN category ON category.category_id = film_category.category_id
LEFT JOIN film_actor ON film.film_id = film_actor.film_id
LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
GROUP BY film.film_id, category.name;