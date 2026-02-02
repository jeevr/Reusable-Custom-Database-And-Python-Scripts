/*
    SELECT * FROM util_view_diff('public.vw_old', 'public.vw_new');
*/


CREATE OR REPLACE FUNCTION util_view_diff(v1 regclass, v2 regclass)
RETURNS TABLE(side text, row_json jsonb)
LANGUAGE plpgsql
AS $$
DECLARE
  cols text;
  sql  text;
BEGIN
  -- Build column list in view order
  SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
    INTO cols
  FROM pg_attribute a
  WHERE a.attrelid = v1 AND a.attnum > 0 AND NOT a.attisdropped;

  IF cols IS NULL THEN
    RAISE EXCEPTION 'Left relation % has no visible columns', v1;
  END IF;

  -- Ensure the right view has the same column set/order
  IF cols <> (
      SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
      FROM pg_attribute a
      WHERE a.attrelid = v2 AND a.attnum > 0 AND NOT a.attisdropped
    )
  THEN
    RAISE EXCEPTION 'Views % and % differ in visible columns/order/types', v1, v2;
  END IF;

  -- Produce symmetric difference, row by row, as JSONB
  sql := format($q$
    WITH a AS (SELECT %1$s FROM %2$s),
         b AS (SELECT %1$s FROM %3$s),
    only_a AS (SELECT %1$s FROM a EXCEPT ALL SELECT %1$s FROM b),
    only_b AS (SELECT %1$s FROM b EXCEPT ALL SELECT %1$s FROM a)
    SELECT 'left_only'::text AS side, to_jsonb(t.*) AS row_json FROM only_a t
    UNION ALL
    SELECT 'right_only'::text AS side, to_jsonb(t.*) AS row_json FROM only_b t
    ORDER BY side;
  $q$, cols, v1::text, v2::text);

  RETURN QUERY EXECUTE sql;
END
$$;
