drop function if exists func_get_duplicate_rows(table_name TEXT, column_name TEXT);
CREATE OR REPLACE FUNCTION func_get_duplicate_rows(table_name TEXT, column_name TEXT)
RETURNS TABLE (
    identifier varchar,
    row_count int
) 
AS $$
DECLARE
    qry TEXT;
BEGIN
    qry :=
        '
        SELECT ' || column_name || '::varchar AS identifier, row_count::int 
        FROM (
            SELECT *, COUNT(*) OVER (PARTITION BY ' || column_name || ') AS row_count
            FROM ' || table_name || '
        ) AS subquery
        WHERE row_count > 1';
    
    -- RAISE NOTICE 'Query: %', query;
    RETURN QUERY EXECUTE qry;
    

END;
$$ LANGUAGE plpgsql;


/*
    SELECT * FROM func_get_duplicate_rows('processing.tblworklogs_raw_temp_b1', 'id');
*/

