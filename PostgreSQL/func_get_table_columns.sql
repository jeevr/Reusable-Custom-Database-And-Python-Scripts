
CREATE OR REPLACE FUNCTION func_get_table_columns(param_schema_name TEXT, param_table_name TEXT)
RETURNS TABLE(
    column_name TEXT, 
    data_type TEXT, 
    is_nullable TEXT,
    column_default TEXT,
    ordinal_position INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.column_name::TEXT, 
        c.data_type::TEXT,
        c.is_nullable::TEXT,
        c.column_default::TEXT,
        c.ordinal_position::INTEGER
    FROM 
        information_schema.columns c
    WHERE 
        c.table_schema = param_schema_name AND 
        c.table_name = param_table_name;
END;
$$ LANGUAGE plpgsql;


/*
    select *
    from func_get_table_columns('opx', 'tblemployees')
*/