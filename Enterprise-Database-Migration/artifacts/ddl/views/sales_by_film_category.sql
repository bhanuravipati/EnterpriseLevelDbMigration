-- View: sales_by_film_category
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:07.458781
=======
-- Generated: 2026-01-27T15:52:24.801258
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE VIEW sales_by_film_category AS
SELECT c.name AS category,
       SUM(p.amount) AS total_sales
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_sales DESC;