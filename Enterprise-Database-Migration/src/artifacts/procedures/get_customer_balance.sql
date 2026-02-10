-- Procedure: get_customer_balance - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:14:27.451651

CREATE OR REPLACE FUNCTION get_customer_balance(p_customer_id INT, p_effective_date TIMESTAMP)
RETURNS NUMERIC(5,2) AS $$
DECLARE
    v_rentfees  NUMERIC(5,2);
    v_overfees  INTEGER;
    v_payments  NUMERIC(5,2);
BEGIN
    -- Rental fees for all previous rentals
    SELECT COALESCE(SUM(film.rental_rate), 0)
      INTO v_rentfees
    FROM film
    JOIN inventory ON film.film_id = inventory.film_id
    JOIN rental    ON inventory.inventory_id = rental.inventory_id
    WHERE rental.rental_date <= p_effective_date
      AND rental.customer_id = p_customer_id;

    -- One dollar for every day the previous rentals are overdue
    SELECT COALESCE(SUM(
               CASE
                 WHEN rental.return_date IS NOT NULL
                      AND (rental.return_date::date - rental.rental_date::date) > film.rental_duration
                 THEN (rental.return_date::date - rental.rental_date::date) - film.rental_duration
                 ELSE 0
               END), 0)
      INTO v_overfees
    FROM rental
    JOIN inventory ON inventory.inventory_id = rental.inventory_id
    JOIN film      ON film.film_id = inventory.film_id
    WHERE rental.rental_date <= p_effective_date
      AND rental.customer_id = p_customer_id;

    -- Subtract all payments made before the specified date
    SELECT COALESCE(SUM(payment.amount), 0)
      INTO v_payments
    FROM payment
    WHERE payment.payment_date <= p_effective_date
      AND payment.customer_id = p_customer_id;

    RETURN v_rentfees + v_overfees - v_payments;
END;
$$ LANGUAGE plpgsql STABLE;