/*

    Note: when workking with OUT & IN, need to put placeholder values when calling the SP
    CALL public.test_multi_out('inactive', '', 0, 0);
    CALL public.test_multi_out('inactive', '', NULL, NULL);


    select *
    from public.test_multi_out

*/

/*

    DROP TABLE IF EXISTS public.test_multi_out;
    CREATE TABLE public.test_multi_out (
        id SERIAL,
        status TEXT,
        message TEXT    
    )


*/


DROP PROCEDURE IF EXISTS public.test_multi_out();
CREATE OR REPLACE PROCEDURE public.test_multi_out(
    IN status_to_set TEXT,
    OUT message TEXT,
    OUT total_count INT,
    OUT active_count INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Set message
    message := format('Dummy run: Status set to %s', status_to_set);

    -- Simulated counts
    total_count := 100;
    active_count := 42;
END;
$$;
