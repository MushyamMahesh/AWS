CREATE OR REPLACE FUNCTION public.insert_into_movies()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
begin
	insert into temp_table123 select 'KGF',1000,cast('2022-01-01' as date);
end
$function$
;
