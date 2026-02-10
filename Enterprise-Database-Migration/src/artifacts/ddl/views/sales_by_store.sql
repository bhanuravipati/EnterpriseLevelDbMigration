-- Fixed view: sales_by_store
-- Generated: 2026-01-23T15:16:38.809326

CREATE OR REPLACE VIEW sales_by_store AS
SELECT
    CONCAT(c.city, ',', cy.country) AS "store",
    CONCAT(m.first_name, ' ', m.last_name) AS "manager",
    SUM(p.amount) AS "total_sales"
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