-- Trigger: del_film on film
-- Generated: 2026-01-23T15:16:05.384994

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