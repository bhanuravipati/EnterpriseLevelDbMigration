-- Trigger: upd_film on film
<<<<<<< HEAD
-- Generated: 2026-02-02T15:16:03.561557
=======
-- Generated: 2026-01-27T15:53:47.002761
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION upd_film_fn()
RETURNS trigger AS $$
BEGIN
    IF (OLD.title IS DISTINCT FROM NEW.title)
       OR (OLD.description IS DISTINCT FROM NEW.description)
       OR (OLD.film_id IS DISTINCT FROM NEW.film_id) THEN
        UPDATE film_text
           SET title = NEW.title,
               description = NEW.description,
               film_id = NEW.film_id
         WHERE film_id = OLD.film_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS upd_film ON film;

CREATE TRIGGER upd_film
AFTER UPDATE ON film
FOR EACH ROW
EXECUTE FUNCTION upd_film_fn();