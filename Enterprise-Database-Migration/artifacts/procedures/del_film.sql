-- Fixed trigger: del_film
-- Generated: 2026-01-23T17:03:57.722092

DROP TRIGGER IF EXISTS del_film ON film;

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