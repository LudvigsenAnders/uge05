
CREATE OR REPLACE FUNCTION public.get_employee(p_firstname text)
RETURNS TABLE (
    employeeid int,
    firstname varchar(10),
    lastname varchar(20),
    title varchar(30)
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.employeeid,
        e.firstname,
        e.lastname,
        e.title
    FROM public.employees e
    WHERE e.firstname = p_firstname;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

