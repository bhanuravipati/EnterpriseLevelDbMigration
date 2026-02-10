-- Trigger: del_film on film
<<<<<<< HEAD
-- Generated: 2026-02-02T15:16:06.487062
=======
-- Generated: 2026-01-27T15:53:50.858388
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION del_film_fn()
RETURNS trigger AS $$
BEGIN
    DELETE FROM film_text WHERE film_id = OLD.film_id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER del_film
AFTER DELETE ON film
FOR EACH ROW
EXECUTE FUNCTION del_film_fn();