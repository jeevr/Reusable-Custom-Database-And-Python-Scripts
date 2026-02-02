-- DROP FUNCTION fn_get_table_dependents;
CREATE OR REPLACE FUNCTION func_get_table_dependents(table_schema character varying(100), table_name character varying(100))
RETURNS TABLE(
    -- note that the data type 'name' is used since the original data type of the fields are 'name'
    source_schema name, 
    source_table name, 
    dependent_schema name, 
    dependent_view name
)  AS 
$$
BEGIN
    RETURN QUERY
    SELECT 
        DISTINCT 
        g.nspname as source_schema,
        d.relname as source_table,
        f.nspname as dependent_schema,
        c.relname as dependent_view
    FROM pg_depend as a
    JOIN pg_rewrite as b ON a.objid = b.oid 
    JOIN pg_class as c ON b.ev_class = c.oid 
    JOIN pg_class as d ON a.refobjid = d.oid 
    JOIN pg_attribute as e ON a.refobjid = e.attrelid 
        AND a.refobjsubid = e.attnum 
    JOIN pg_namespace as f ON f.oid = c.relnamespace
    JOIN pg_namespace as g ON g.oid = d.relnamespace
    WHERE 
        g.nspname = table_schema
        AND d.relname = table_name
        AND e.attnum > 0
    ORDER BY 1,2;

    return;

END;
$$ LANGUAGE plpgsql;


-- select * from func_get_table_dependents('opx', 'tblemployees')