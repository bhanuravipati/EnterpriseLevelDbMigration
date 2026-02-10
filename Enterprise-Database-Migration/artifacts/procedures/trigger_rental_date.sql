-- Trigger: rental_date on rental
<<<<<<< HEAD
-- Generated: 2026-02-02T15:16:22.495411
=======
-- Generated: 2026-01-27T15:54:01.966360
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION rental_date_trigger()
RETURNS trigger AS $$
BEGIN
    NEW.rental_date := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER rental_date
BEFORE INSERT ON rental
FOR EACH ROW
EXECUTE FUNCTION rental_date_trigger();