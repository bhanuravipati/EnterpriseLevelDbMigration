-- Procedure: inventory_held_by_customer - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:49.453984

CREATE OR REPLACE FUNCTION inventory_held_by_customer(p_inventory_id INT)
=======
-- Generated: 2026-01-27T15:52:49.365487

CREATE OR REPLACE FUNCTION inventory_held_by_customer(p_inventory_id INTEGER)
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
RETURNS INTEGER AS $$
DECLARE
    v_customer_id INTEGER;
BEGIN
    SELECT r.customer_id INTO v_customer_id
    FROM rental r
    WHERE r.return_date IS NULL
      AND r.inventory_id = p_inventory_id
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    RETURN v_customer_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;