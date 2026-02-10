-- Procedure: inventory_held_by_customer - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:14:39.698503

CREATE OR REPLACE FUNCTION inventory_held_by_customer(p_inventory_id INT)
RETURNS INTEGER AS $$
DECLARE
    v_customer_id INTEGER;
BEGIN
    SELECT customer_id INTO v_customer_id
    FROM rental
    WHERE return_date IS NULL
      AND inventory_id = p_inventory_id;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    RETURN v_customer_id;
END;
$$ LANGUAGE plpgsql;