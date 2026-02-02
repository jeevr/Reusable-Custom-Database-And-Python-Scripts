
/*

    SELECT * FROM public.func_unpivot_table(
        'ugcables.tblasset_register_ug_cables',
        'asset_identifier', -- identifier column
        ARRAY['100030', '100037'] -- lists of fields applied to identifier column
    )

*/

DROP FUNCTION IF EXISTS public.func_unpivot_table(TEXT, TEXT, TEXT[]);
CREATE OR REPLACE FUNCTION public.func_unpivot_table(
    in_table TEXT,
    in_filter_column TEXT,
    filter_values TEXT[]
)
RETURNS TABLE(identifier TEXT, key_fields TEXT, field_values TEXT)
AS $$
DECLARE
    dyn_sql TEXT;
    literal_list TEXT;
BEGIN
    -- Convert the array into a safe SQL literal list: 'val1', 'val2', ...
    SELECT string_agg(format('%L', val), ', ') INTO literal_list
    FROM unnest(filter_values) AS val;
    
    -- Build dynamic SQL
    dyn_sql := '
    with filtered_asset as (
        select 
            xx.' || in_filter_column || ' as identifier,
            xx.*
        from ' || in_table || ' as xx 
        where ' || in_filter_column || ' in (' || literal_list || ')

    )
    , unpivot as (
        select 
            aa.identifier,
            bb.key as key_fields, 
            bb.value as field_values
        from filtered_asset as aa,
            json_each_text(to_json(aa.*)) as bb
    )
    , fnl_tbl as (
        select
            cc.identifier::text,
            cc.key_fields::text,
            cc.field_values::text
        from unpivot cc
        where cc.key_fields != ''identifier''
    )
    select *
    from fnl_tbl;
    ';

    RAISE NOTICE 'Executing SQL: %', dyn_sql;

    RETURN QUERY EXECUTE dyn_sql;
END;
$$ LANGUAGE plpgsql STABLE;


