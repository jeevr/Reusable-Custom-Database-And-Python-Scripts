/*
    SELECT * FROM func_get_table_indexes('public', 'raw_breadcrumbdata1024');

*/
DROP FUNCTION IF EXISTS func_get_table_indexes(TEXT, TEXT);
CREATE OR REPLACE FUNCTION func_get_table_indexes(schema_name TEXT, table_name TEXT)
RETURNS TABLE(
    column_name TEXT, 
    index_name TEXT, 
    index_type TEXT
) AS 
$$
BEGIN
    RETURN QUERY
    SELECT
        a.attname::TEXT AS column_name,
        i.relname::TEXT AS index_name,
        CASE
            WHEN ix.indisunique THEN 'UNIQUE'::TEXT
            ELSE 'NON-UNIQUE'::TEXT
        END AS index_type
    FROM
        pg_class t
    JOIN
        pg_namespace n ON t.relnamespace = n.oid
    JOIN
        pg_index ix ON t.oid = ix.indrelid
    JOIN
        pg_class i ON i.oid = ix.indexrelid
    JOIN
        pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE
        n.nspname = schema_name
        AND t.relname = table_name
    ORDER BY
        a.attname, i.relname;

    RETURN;
END;
$$ LANGUAGE plpgsql;
