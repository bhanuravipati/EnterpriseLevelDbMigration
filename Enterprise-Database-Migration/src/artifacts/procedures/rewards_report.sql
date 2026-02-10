-- Procedure: rewards_report - Converted to PL/pgSQL
-- Generated: 2026-01-23T15:15:38.513799

CREATE OR REPLACE FUNCTION rewards_report(
    p_min_monthly_purchases SMALLINT,
    p_min_dollar_amount_purchased NUMERIC
) RETURNS TABLE (
    customer_id SMALLINT,
    store_id SMALLINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    address_id SMALLINT,
    active SMALLINT,
    create_date TIMESTAMP,
    last_update TIMESTAMP,
    count_rewardees INTEGER
) AS $$
DECLARE
    v_last_month_start DATE;
    v_last_month_end   DATE;
BEGIN
    IF p_min_monthly_purchases = 0 THEN
        RAISE EXCEPTION 'Minimum monthly purchases parameter must be > 0';
    END IF;

    IF p_min_dollar_amount_purchased = 0 THEN
        RAISE EXCEPTION 'Minimum monthly dollar amount purchased parameter must be > $0.00';
    END IF;

    -- first day of previous month
    v_last_month_start := date_trunc('month', CURRENT_DATE - INTERVAL '1 month')::date;
    -- last day of previous month
    v_last_month_end   := (date_trunc('month', CURRENT_DATE) - INTERVAL '1 day')::date;

    RETURN QUERY
    WITH tmpCustomer AS (
        SELECT p.customer_id
        FROM payment p
        WHERE p.payment_date::date BETWEEN v_last_month_start AND v_last_month_end
        GROUP BY p.customer_id
        HAVING SUM(p.amount) > p_min_dollar_amount_purchased
           AND COUNT(*) > p_min_monthly_purchases
    ),
    cnt AS (
        SELECT COUNT(*) AS cnt FROM tmpCustomer
    )
    SELECT
        c.customer_id,
        c.store_id,
        c.first_name,
        c.last_name,
        c.email,
        c.address_id,
        c.active,
        c.create_date,
        c.last_update,
        cnt.cnt AS count_rewardees
    FROM tmpCustomer t
    JOIN customer c ON c.customer_id = t.customer_id
    CROSS JOIN cnt;
END;
$$ LANGUAGE plpgsql;