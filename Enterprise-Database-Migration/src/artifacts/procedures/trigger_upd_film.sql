-- Trigger: upd_film on film
-- Generated: 2026-01-23T15:15:58.334308

CREATE OR REPLACE FUNCTION upd_film_fn()
RETURNS trigger AS $$
BEGIN
    IF (OLD.title IS DISTINCT FROM NEW.title) OR
       (OLD.description IS DISTINCT FROM NEW.description) OR
       (OLD.film_id IS DISTINCT FROM NEW.film_id) THEN
        UPDATE film_text
        SET title = NEW.title,
            description = NEW.description,
            film_id = NEW.film_id
        WHERE film_id = OLD.film_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER upd_film
AFTER UPDATE ON film
FOR EACH ROW
EXECUTE FUNCTION upd_film_fn();