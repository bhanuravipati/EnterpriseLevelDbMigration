-- Fixed trigger: rental_date
-- Generated: 2026-01-23T17:04:14.168275

DROP TRIGGER IF EXISTS rental_date ON rental;

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