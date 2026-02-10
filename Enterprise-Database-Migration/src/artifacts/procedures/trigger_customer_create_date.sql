-- Trigger: customer_create_date on customer
-- Generated: 2026-01-23T15:16:07.650900

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