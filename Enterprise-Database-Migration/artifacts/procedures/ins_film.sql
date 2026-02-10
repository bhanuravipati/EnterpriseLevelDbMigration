-- Fixed trigger: ins_film
-- Generated: 2026-01-23T17:03:44.636559

DROP TRIGGER IF EXISTS ins_film ON film;

CREATE OR REPLACE FUNCTION ins_film_fn()
RETURNS trigger AS $$
BEGIN
    INSERT INTO film_text (film_id, title, description)
    VALUES (NEW.film_id, NEW.title, NEW.description);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ins_film
AFTER INSERT ON film
FOR EACH ROW
EXECUTE FUNCTION ins_film_fn();