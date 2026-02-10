-- Trigger: customer_create_date on customer
<<<<<<< HEAD
-- Generated: 2026-02-02T15:16:10.221867
=======
-- Generated: 2026-01-27T15:53:54.961906
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION customer_create_date_fn()
RETURNS trigger AS $$
BEGIN
    NEW.create_date := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customer_create_date
BEFORE INSERT ON customer
FOR EACH ROW
EXECUTE FUNCTION customer_create_date_fn();