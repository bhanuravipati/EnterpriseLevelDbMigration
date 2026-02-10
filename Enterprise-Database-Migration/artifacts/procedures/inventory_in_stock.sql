-- Procedure: inventory_in_stock - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:15:02.812309
=======
-- Generated: 2026-01-27T15:52:53.701085
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION inventory_in_stock(p_inventory_id INT)
RETURNS BOOLEAN AS $$
DECLARE
    v_rentals INT;
    v_out INT;
BEGIN
    SELECT COUNT(*) INTO v_rentals
    FROM rental
    WHERE inventory_id = p_inventory_id;

    IF v_rentals = 0 THEN
        RETURN TRUE;
    END IF;

    SELECT COUNT(rental_id) INTO v_out
    FROM inventory i
    LEFT JOIN rental r USING (inventory_id)
    WHERE i.inventory_id = p_inventory_id
      AND r.return_date IS NULL;

    IF v_out > 0 THEN
        RETURN FALSE;
    ELSE
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;