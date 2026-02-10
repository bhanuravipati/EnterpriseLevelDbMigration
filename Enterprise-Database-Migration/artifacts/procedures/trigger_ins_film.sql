-- Trigger: ins_film on film
<<<<<<< HEAD
-- Generated: 2026-02-02T15:15:59.537596
=======
-- Generated: 2026-01-27T15:53:42.502785
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

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