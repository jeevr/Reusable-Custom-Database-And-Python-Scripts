/*
    select *
    from func_get_timekeeping_breakdown_data('A4V9', '2024-04-09')

    select count(*), min(work_date), max(work_date)
    from opx.tbltimekeeping

    select count(*), min(work_date), max(work_date)
    from opx.tbltimekeeping_processed

    select attendance_type, count(*)
    from opx.tbltimekeeping_processed
    group by attendance_type
    limit 10
*/

drop function if exists func_get_timekeeping_breakdown_data(param_employee_number VARCHAR, param_work_date DATE);

-- Create the function
CREATE OR REPLACE FUNCTION func_get_timekeeping_breakdown_data(param_employee_number VARCHAR, param_work_date DATE)
RETURNS TABLE (
    work_date date,
    employee_number varchar,
    employee_division varchar,
    ticket_division_concat varchar,
    start_time varchar,
    end_time varchar,
    hours double precision,
    receiver_order varchar,
    attendance_type varchar
) AS $$
BEGIN
    RETURN QUERY 

    SELECT 
        tkp.work_date::date,
        emp.employee_number::varchar,
        emp.employee_division::varchar,
        exc.ticket_division_concat::varchar,
        tkp.start_time::varchar,
        tkp.end_time::varchar,
        tkp.hours::double precision,
        tkp.receiver_order::varchar,
        -- coalesce(tkp.attendance_type::varchar, '')
        case 
            when tkp.attendance_type is null 
                then tkp.wage_type::varchar
            else tkp.attendance_type::varchar
        end
    -- FROM opx.tbltimekeeping tkp 
    FROM opx.tbltimekeeping_processed tkp 
    JOIN opx.tblemployees emp ON tkp.employee_id = emp.employee_id
    LEFT JOIN opx.tblexceptions exc ON tkp.employee_id = exc.employee_id AND tkp.work_date = exc.work_date
    WHERE 1=1 
        AND emp.employee_number = param_employee_number
        AND tkp.work_date = param_work_date
    ORDER BY tkp.start_time
    -- LIMIT 100
    ;
END;
$$ LANGUAGE plpgsql;




