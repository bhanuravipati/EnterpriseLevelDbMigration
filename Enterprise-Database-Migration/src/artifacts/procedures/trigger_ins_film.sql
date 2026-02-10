-- Trigger: ins_film on film
-- Generated: 2026-01-23T15:15:48.196058

CREATE OR REPLACE FUNCTION ins_film_fn()
RETURNS trigger AS $$
BEGIN
    INSERT INTO film_text (film_id, title, description)
    VALUES (NEW.film_id, NEW.title, NEW.description);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ins_film
AFTER INSERT ON film
FOR EACH ROW
EXECUTE FUNCTION ins_film_fn();