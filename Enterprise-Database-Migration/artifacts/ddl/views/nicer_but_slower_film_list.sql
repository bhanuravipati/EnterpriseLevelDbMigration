-- View: nicer_but_slower_film_list
<<<<<<< HEAD
-- Generated: 2026-02-02T15:13:57.443112

CREATE VIEW "nicer_but_slower_film_list" AS
=======
-- Generated: 2026-01-27T15:51:54.069912

CREATE VIEW nicer_but_slower_film_list AS
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
SELECT
    film.film_id AS "FID",
    film.title,
    film.description,
<<<<<<< HEAD
    category.name AS "category",
    film.rental_rate AS "price",
    film.length,
    film.rating,
    STRING_AGG(
        initcap(actor.first_name) || ' ' || initcap(actor.last_name),
        ', '
        ORDER BY initcap(actor.first_name) || ' ' || initcap(actor.last_name)
    ) AS "actors"
FROM film
LEFT JOIN film_category ON film_category.film_id = film.film_id
LEFT JOIN category ON category.category_id = film_category.category_id
LEFT JOIN film_actor ON film_actor.film_id = film.film_id
LEFT JOIN actor ON actor.actor_id = film_actor.actor_id
=======
    category.name AS category,
    film.rental_rate AS price,
    film.length,
    film.rating,
    STRING_AGG(
        upper(substr(actor.first_name,1,1)) ||
        lower(substr(actor.first_name,2)) ||
        ' ' ||
        upper(substr(actor.last_name,1,1)) ||
        lower(substr(actor.last_name,2)),
        ', '
        ORDER BY actor.last_name, actor.first_name
    ) AS actors
FROM film
LEFT JOIN film_category ON film_category.film_id = film.film_id
LEFT JOIN category ON category.category_id = film_category.category_id
LEFT JOIN film_actor ON film.film_id = film_actor.film_id
LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
GROUP BY film.film_id, category.name;