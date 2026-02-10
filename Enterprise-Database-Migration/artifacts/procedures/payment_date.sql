-- Fixed trigger: payment_date
-- Generated: 2026-01-23T17:04:08.154673

DROP TRIGGER IF EXISTS payment_date ON payment;

CREATE OR REPLACE FUNCTION payment_date_trigger()
RETURNS trigger AS $$
BEGIN
    NEW.payment_date := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER payment_date
BEFORE INSERT ON payment
FOR EACH ROW
EXECUTE FUNCTION payment_date_trigger();