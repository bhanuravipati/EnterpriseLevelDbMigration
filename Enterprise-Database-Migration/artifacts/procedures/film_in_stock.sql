-- Procedure: film_in_stock - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:15:16.974625
=======
-- Generated: 2026-01-27T15:53:03.363853
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION film_in_stock(p_film_id INT, p_store_id INT)
RETURNS INTEGER AS $$
DECLARE
    v_film_count INTEGER;
BEGIN
<<<<<<< HEAD
    PERFORM inventory_id
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id
      AND inventory_in_stock(inventory_id);

=======
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    SELECT COUNT(*) INTO v_film_count
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id
      AND inventory_in_stock(inventory_id);

    RETURN v_film_count;
END;
$$ LANGUAGE plpgsql;