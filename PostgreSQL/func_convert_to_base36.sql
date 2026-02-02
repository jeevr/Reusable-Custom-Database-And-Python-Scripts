

drop FUNCTION opx.func_to_base36;
CREATE OR REPLACE FUNCTION opx.func_to_base36(num bigint) 
RETURNS TEXT AS $$
DECLARE
    base36 CHAR(36) := '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    result TEXT := '';
    remainder INT;
BEGIN
    IF num = 0 THEN
        RETURN '0';
    END IF;

    WHILE num > 0 LOOP
        remainder := num % 36;
        result := SUBSTRING(base36 FROM remainder + 1 FOR 1) || result;
        num := num / 36;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;



-- select opx.func_to_base36(149566)