-- CREATE A DIVISION FUNCTION (this will handle the division by zero error)
CREATE OR REPLACE FUNCTION safe_division(dividend double precision, divisor double precision)
RETURNS numeric AS $$
BEGIN
    IF divisor = 0 THEN
        RETURN NULL; -- or any default value you want
    ELSE
        RETURN dividend / divisor;
    END IF;
END;
$$ LANGUAGE plpgsql;