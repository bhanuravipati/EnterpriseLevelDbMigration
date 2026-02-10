-- Procedure: film_in_stock - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:15:05.776854

CREATE OR REPLACE FUNCTION film_in_stock(p_film_id INT, p_store_id INT)
RETURNS INTEGER AS $$
DECLARE
    v_film_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_film_count
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id
      AND inventory_in_stock(inventory_id);

    RETURN v_film_count;
END;
$$ LANGUAGE plpgsql;