/*

SELECT * FROM func_get_table_columns('public.tblregions');
SELECT * FROM func_get_table_columns('public.tblmanagementareas');
SELECT * FROM func_get_table_columns('public.tblservicecenters');
SELECT * FROM func_get_table_columns('public.tblsubstations');


SELECT *
FROM func_get_table_columns('public.tblservicecenters');

*/

DROP FUNCTION IF EXISTS public.func_get_table_columns(regclass);
CREATE OR REPLACE FUNCTION public.func_get_table_columns(
    p_table regclass   -- e.g. 'public.users'
)
RETURNS TABLE (
    table_schema     text,
    table_name       text,
    ordinal_position int,
    column_name      text,
    data_type        text,
    is_nullable      boolean,
    is_primary_key   boolean,
    is_unique        boolean,            -- TRUE if column participates in a single-column UNIQUE index
    column_default   text,               -- parsed expression (doesn't advance sequences)
    is_generated     text,               -- 'STORED' or NULL
    is_identity      boolean,
    identity_type    text,               -- 'ALWAYS' | 'BY DEFAULT' | NULL
    collation_name   text,
    sequence_schema  text,
    sequence_name    text,
    current_value    bigint,             -- sequence last_value (non-advancing)
    next_value       bigint              -- computed last_value + increment_by
)
LANGUAGE SQL
STABLE
AS $$
WITH tbl AS (
  SELECT c.oid AS relid, n.nspname AS table_schema, c.relname AS table_name
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_table
),
cols AS (
  SELECT
      t.table_schema,
      t.table_name,
      a.attnum                          AS ordinal_position,
      a.attname                         AS column_name,
      format_type(a.atttypid, a.atttypmod) AS data_type,
      NOT a.attnotnull                  AS is_nullable,
      pg_get_expr(ad.adbin, ad.adrelid) AS column_default_expr,
      a.attgenerated                    AS attgenerated,          -- 's' for STORED
      (a.attidentity <> '')             AS is_identity,
      CASE a.attidentity WHEN 'a' THEN 'ALWAYS'
                         WHEN 'd' THEN 'BY DEFAULT'
                         ELSE NULL END   AS identity_type,
      a.attcollation,
      a.attrelid,
      a.attnum
  FROM tbl t
  JOIN pg_attribute a ON a.attrelid = t.relid
  LEFT JOIN pg_attrdef ad
         ON ad.adrelid = a.attrelid
        AND ad.adnum   = a.attnum
  WHERE a.attnum > 0 AND NOT a.attisdropped
),
-- Primary-key membership (per-attnum)
pk AS (
  SELECT unnest(i.indkey)::int AS attnum
  FROM pg_index i
  WHERE i.indrelid = (SELECT relid FROM tbl)
    AND i.indisprimary
),
-- Single-column UNIQUE indexes (exclude PK)
uniq AS (
  SELECT unnest(i.indkey)::int AS attnum
  FROM pg_index i
  WHERE i.indrelid = (SELECT relid FROM tbl)
    AND i.indisunique
    AND NOT i.indisprimary
    AND array_length(i.indkey::int2[], 1) = 1
),
-- Sequence owned by the column (covers SERIAL & IDENTITY)
dep_seq AS (
  SELECT
    c.attnum,
    d.objid AS seq_oid
  FROM cols c
  JOIN pg_depend d
    ON d.refobjid    = c.attrelid
   AND d.refobjsubid = c.attnum
   AND d.deptype     IN ('a','i')     -- 'a'=serial, 'i'=identity
  JOIN pg_class sc ON sc.oid = d.objid AND sc.relkind = 'S'
),
-- Fallback: parse DEFAULT nextval('...') even if not OWNED BY
def_seq AS (
  SELECT
    c.attnum,
    to_regclass(
      regexp_replace(c.column_default_expr, '^nextval\(''(.*)''::regclass\)$', '\1')
    ) AS seq_oid
  FROM cols c
  WHERE c.column_default_expr ~ '^nextval\(''.*''::regclass\)$'
),
col_seq AS (
  SELECT * FROM dep_seq
  UNION ALL
  SELECT * FROM def_seq
),
col_seq_dedup AS (
  SELECT DISTINCT ON (attnum) attnum, seq_oid
  FROM col_seq
  WHERE seq_oid IS NOT NULL
  ORDER BY attnum
),
seq_info AS (
  SELECT
    d.attnum,
    ns.nspname  AS sequence_schema,
    sq.relname  AS sequence_name,
    ps.last_value,
    ps.increment_by
  FROM col_seq_dedup d
  JOIN pg_class sq      ON sq.oid = d.seq_oid AND sq.relkind = 'S'
  JOIN pg_namespace ns  ON ns.oid = sq.relnamespace
  JOIN pg_sequences ps  ON ps.schemaname = ns.nspname AND ps.sequencename = sq.relname
),
coll AS (
  SELECT c.oid AS collation_oid, c.collname AS collation_name
  FROM pg_collation c
)
SELECT
  c.table_schema,
  c.table_name,
  c.ordinal_position,
  c.column_name,
  c.data_type,
  c.is_nullable,
  (c.attnum IN (SELECT attnum FROM pk))   AS is_primary_key,
  (c.attnum IN (SELECT attnum FROM uniq)) AS is_unique,
  c.column_default_expr                   AS column_default,
  CASE c.attgenerated WHEN 's' THEN 'STORED' ELSE NULL END AS is_generated,
  c.is_identity,
  c.identity_type,
  coll.collation_name,
  s.sequence_schema,
  s.sequence_name,
  s.last_value::bigint                    AS current_value,
  CASE WHEN s.last_value IS NOT NULL
       THEN (s.last_value + s.increment_by)::bigint
       END                                AS next_value
FROM cols c
LEFT JOIN seq_info s
       ON s.attnum = c.attnum
LEFT JOIN coll
       ON coll.collation_oid = c.attcollation
ORDER BY c.ordinal_position;
$$;