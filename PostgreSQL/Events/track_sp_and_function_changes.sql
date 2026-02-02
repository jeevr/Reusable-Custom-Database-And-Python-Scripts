/*
    -- =========================================
    -- SAMPLE OF GETTING THE DEFINITION
    -- =========================================
    
    SELECT *
    FROM ddl_audit_log ORDER BY event_time DESC;


*/

-- Create logging table
-- DROP TABLE IF EXISTS ddl_audit_log;
CREATE TABLE IF NOT EXISTS ddl_audit_log (
    id            BIGSERIAL PRIMARY KEY,
    event_time    TIMESTAMP NOT NULL DEFAULT now(),
    command_tag   TEXT NOT NULL,
    object_type   TEXT NOT NULL,
    object_name   TEXT NOT NULL,
    schema_name   TEXT,
    definition    TEXT
);

-- Event trigger function
-- 2) Event trigger function
DROP FUNCTION IF EXISTS log_function_changes CASCADE;
CREATE OR REPLACE FUNCTION log_function_changes()
RETURNS event_trigger AS $$
DECLARE
    rec         RECORD;
    regproc     REGPROCEDURE;
    def_text    TEXT;
BEGIN
    FOR rec IN
        SELECT command_tag, object_type, object_identity, schema_name
        FROM pg_event_trigger_ddl_commands()
    LOOP
        def_text := NULL;

        -- Only try to fetch definition when the object exists
        IF rec.command_tag IN ('CREATE FUNCTION','ALTER FUNCTION','CREATE PROCEDURE','ALTER PROCEDURE') THEN
            -- Convert fully-qualified identity + arg types to a regprocedure safely
            regproc := to_regprocedure(rec.object_identity);

            IF regproc IS NOT NULL THEN
                -- regprocedure is a domain over oid; cast to oid for pg_get_functiondef
                def_text := pg_get_functiondef(regproc::oid);
            END IF;
        END IF;

        INSERT INTO ddl_audit_log (command_tag, object_type, object_name, schema_name, definition)
        VALUES (rec.command_tag, rec.object_type, rec.object_identity, rec.schema_name, def_text);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Event trigger for function/procedure changes
-- 3) Event trigger (CREATE/ALTER/DROP)
DROP EVENT TRIGGER IF EXISTS track_function_updates;
CREATE EVENT TRIGGER track_function_updates
ON ddl_command_end
WHEN TAG IN (
    'CREATE FUNCTION', 'ALTER FUNCTION', 'DROP FUNCTION',
    'CREATE PROCEDURE','ALTER PROCEDURE','DROP PROCEDURE'
)
EXECUTE FUNCTION log_function_changes();