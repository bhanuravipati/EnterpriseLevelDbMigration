-- Fixed trigger: customer_create_date
-- Generated: 2026-01-23T17:04:03.453417

DROP TRIGGER IF EXISTS customer_create_date ON customer;

CREATE OR REPLACE FUNCTION customer_create_date_fn()
RETURNS trigger AS $$
BEGIN
    NEW.create_date := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_create_date
BEFORE INSERT ON customer
FOR EACH ROW
EXECUTE FUNCTION customer_create_date_fn();