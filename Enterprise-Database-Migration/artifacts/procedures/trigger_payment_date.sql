-- Trigger: payment_date on payment
<<<<<<< HEAD
-- Generated: 2026-02-02T15:16:13.954730
=======
-- Generated: 2026-01-27T15:53:58.512978
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION set_payment_date()
RETURNS trigger AS $$
BEGIN
    NEW.payment_date := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER payment_date
BEFORE INSERT ON payment
FOR EACH ROW
EXECUTE FUNCTION set_payment_date();