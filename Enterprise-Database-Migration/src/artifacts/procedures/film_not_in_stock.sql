-- Procedure: film_not_in_stock - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:15:19.440079

CREATE OR REPLACE FUNCTION film_not_in_stock(p_film_id INT, p_store_id INT)
RETURNS INTEGER AS $$
DECLARE
    p_film_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO p_film_count
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id
      AND NOT inventory_in_stock(inventory_id);
    RETURN p_film_count;
END;
$$ LANGUAGE plpgsql;