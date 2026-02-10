-- Procedure: film_not_in_stock - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:15:30.297702
=======
-- Generated: 2026-01-27T15:53:14.475318
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION film_not_in_stock(p_film_id INT, p_store_id INT)
RETURNS INTEGER AS $$
DECLARE
<<<<<<< HEAD
    v_film_count INTEGER;
=======
    v_film_count INT;
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
BEGIN
    SELECT COUNT(*)
    INTO v_film_count
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id
      AND NOT inventory_in_stock(inventory_id);

    RETURN v_film_count;
END;
$$ LANGUAGE plpgsql;