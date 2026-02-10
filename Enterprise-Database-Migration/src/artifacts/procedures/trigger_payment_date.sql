-- Trigger: payment_date on payment
-- Generated: 2026-01-23T15:16:10.493566

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