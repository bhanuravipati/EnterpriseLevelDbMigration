-- Trigger: rental_date on rental
-- Generated: 2026-01-23T15:16:13.147046

CREATE OR REPLACE FUNCTION rental_date_trigger_fn()
RETURNS trigger AS $$
BEGIN
    NEW.rental_date := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER rental_date
BEFORE INSERT ON rental
FOR EACH ROW
EXECUTE FUNCTION rental_date_trigger_fn();