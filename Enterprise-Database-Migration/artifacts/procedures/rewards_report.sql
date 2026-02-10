-- Procedure: rewards_report - Converted to PL/pgSQL
<<<<<<< HEAD
-- Generated: 2026-02-02T15:15:48.392550
=======
-- Generated: 2026-01-27T15:53:32.499631
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b

CREATE OR REPLACE FUNCTION rewards_report(
    min_monthly_purchases SMALLINT,
    min_dollar_amount_purchased NUMERIC
)
RETURNS TABLE (
    count_rewardees INTEGER,
    customer_id SMALLINT,
    store_id SMALLINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    address_id SMALLINT,
    active BOOLEAN,
    create_date TIMESTAMP,
    last_update TIMESTAMP
) AS $$
DECLARE
    last_month_start DATE;
    last_month_end DATE;
BEGIN
    IF min_monthly_purchases = 0 THEN
        RAISE EXCEPTION 'Minimum monthly purchases parameter must be > 0';
    END IF;

    IF min_dollar_amount_purchased = 0 THEN
        RAISE EXCEPTION 'Minimum monthly dollar amount purchased parameter must be > $0.00';
    END IF;

<<<<<<< HEAD
    -- Determine start and end of the previous month
    last_month_start := date_trunc('month', current_date - interval '1 month')::date;
    last_month_end   := (date_trunc('month', current_date) - interval '1 day')::date;

=======
    -- Determine start of previous month (first day)
    last_month_start := (CURRENT_DATE - INTERVAL '1 month')::date;
    last_month_start := make_date(
        EXTRACT(YEAR FROM last_month_start)::int,
        EXTRACT(MONTH FROM last_month_start)::int,
        1
    );

    -- End of that month (last day)
    last_month_end := (date_trunc('month', last_month_start) + INTERVAL '1 month - 1 day')::date;

>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    RETURN QUERY
    WITH qualified_customers AS (
        SELECT p.customer_id
        FROM payment p
        WHERE p.payment_date::date BETWEEN last_month_start AND last_month_end
        GROUP BY p.customer_id
        HAVING SUM(p.amount) > min_dollar_amount_purchased
           AND COUNT(*) > min_monthly_purchases
    ),
    cnt AS (
<<<<<<< HEAD
        SELECT COUNT(*) AS cnt FROM qualified_customers
=======
        SELECT COUNT(*) AS cnt FROM qualified
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    )
    SELECT
        cnt.cnt,
        c.customer_id,
        c.store_id,
        c.first_name,
        c.last_name,
        c.email,
        c.address_id,
        c.active,
        c.create_date,
        c.last_update
    FROM qualified_customers qc
    JOIN customer c ON c.customer_id = qc.customer_id
    CROSS JOIN cnt;
END;
$$ LANGUAGE plpgsql;