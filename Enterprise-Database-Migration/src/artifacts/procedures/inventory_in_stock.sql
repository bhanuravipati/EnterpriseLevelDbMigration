-- Procedure: inventory_in_stock - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:14:52.910276

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