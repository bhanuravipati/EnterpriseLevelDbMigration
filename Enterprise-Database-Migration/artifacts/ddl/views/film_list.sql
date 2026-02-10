-- View: film_list
<<<<<<< HEAD
-- Generated: 2026-02-02T15:13:35.564883

CREATE OR REPLACE VIEW film_list AS
SELECT
    film.film_id AS "FID",
    film.title AS title,
    film.description AS description,
    category.name AS category,
    film.rental_rate AS price,
    film.length AS length,
    film.rating AS rating,
    STRING_AGG(actor.first_name || ' ' || actor.last_name, ', ') AS actors
FROM film
LEFT JOIN film_category ON film_category.film_id = film.film_id
LEFT JOIN category ON category.category_id = film_category.category_id
LEFT JOIN film_actor ON film.film_id = film_actor.film_id
LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
GROUP BY
    film.film_id,
    film.title,
    film.description,
    category.name,
    film.rental_rate,
    film.length,
    film.rating;
=======
-- Generated: 2026-01-27T15:51:40.076926

CREATE OR REPLACE VIEW film_list AS
SELECT
    f.film_id AS "FID",
    f.title,
    f.description,
    c.name AS category,
    f.rental_rate AS price,
    f.length,
    f.rating,
    STRING_AGG(a.first_name || ' ' || a.last_name, ', ') AS actors
FROM film f
LEFT JOIN film_category fc ON fc.film_id = f.film_id
LEFT JOIN category c ON c.category_id = fc.category_id
LEFT JOIN film_actor fa ON fa.film_id = f.film_id
LEFT JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY f.film_id, f.title, f.description, c.name, f.rental_rate, f.length, f.rating;
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
