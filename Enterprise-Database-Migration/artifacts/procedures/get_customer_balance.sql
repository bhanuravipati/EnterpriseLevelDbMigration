-- Procedure: get_customer_balance - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:14:37.030157
=======
-- Generated: 2026-01-27T15:52:48.064232
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION get_customer_balance(
    p_customer_id INTEGER,
    p_effective_date TIMESTAMP
) RETURNS NUMERIC(5,2) AS $$
DECLARE
<<<<<<< HEAD
    v_rentfees  NUMERIC(5,2) := 0;
    v_overfees  INTEGER      := 0;
    v_payments  NUMERIC(5,2) := 0;
BEGIN
    -- Rental fees for all previous rentals
    SELECT COALESCE(SUM(film.rental_rate),0)
    INTO v_rentfees
    FROM film
    JOIN inventory ON film.film_id = inventory.film_id
    JOIN rental    ON inventory.inventory_id = rental.inventory_id
    WHERE rental.rental_date <= p_effective_date
      AND rental.customer_id = p_customer_id;

    -- One dollar for every day the previous rentals are overdue
    SELECT COALESCE(SUM(
               GREATEST( (rental.return_date::date - rental.rental_date::date) - film.rental_duration, 0)
           ),0)
    INTO v_overfees
    FROM rental
    JOIN inventory ON inventory.inventory_id = rental.inventory_id
    JOIN film     ON film.film_id = inventory.film_id
    WHERE rental.rental_date <= p_effective_date
      AND rental.customer_id = p_customer_id
      AND rental.return_date IS NOT NULL;

    -- Subtract all payments made before the effective date
    SELECT COALESCE(SUM(payment.amount),0)
    INTO v_payments
    FROM payment
    WHERE payment.payment_date <= p_effective_date
      AND payment.customer_id = p_customer_id;
=======
    v_rentfees  NUMERIC(5,2);
    v_overfees  INTEGER;
    v_payments  NUMERIC(5,2);
BEGIN
    -- Rental fees for all previous rentals
    SELECT COALESCE(SUM(f.rental_rate), 0)
      INTO v_rentfees
    FROM film f
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE r.rental_date <= p_effective_date
      AND r.customer_id = p_customer_id;

    -- One dollar for every day the previous rentals are overdue
    SELECT COALESCE(SUM(
               GREATEST(
                   (r.return_date::date - r.rental_date::date) - f.rental_duration,
                   0
               )
           ), 0)
      INTO v_overfees
    FROM rental r
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film f ON f.film_id = i.film_id
    WHERE r.rental_date <= p_effective_date
      AND r.customer_id = p_customer_id
      AND r.return_date IS NOT NULL;

    -- Subtract all payments made before the specified date
    SELECT COALESCE(SUM(p.amount), 0)
      INTO v_payments
    FROM payment p
    WHERE p.payment_date <= p_effective_date
      AND p.customer_id = p_customer_id;
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

    RETURN v_rentfees + v_overfees - v_payments;
END;
$$ LANGUAGE plpgsql;