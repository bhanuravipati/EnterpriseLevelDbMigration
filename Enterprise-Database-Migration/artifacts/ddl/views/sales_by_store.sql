-- View: sales_by_store
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:17.624832
=======
-- Generated: 2026-01-27T15:52:12.778948
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE VIEW sales_by_store AS
SELECT
<<<<<<< HEAD
    concat(c.city, ',', cy.country) AS store,
    concat(m.first_name, ' ', m.last_name) AS manager,
    sum(p.amount) AS total_sales
=======
    (c.city || ',' || cy.country) AS store,
    (m.first_name || ' ' || m.last_name) AS manager,
    SUM(p.amount) AS total_sales
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN store s ON i.store_id = s.store_id
JOIN address a ON s.address_id = a.address_id
JOIN city c ON a.city_id = c.city_id
JOIN country cy ON c.country_id = cy.country_id
JOIN staff m ON s.manager_staff_id = m.staff_id
GROUP BY s.store_id, c.city, cy.country, m.first_name, m.last_name
ORDER BY cy.country, c.city;