SELECT * FROM public."CONSOLIDADO_FULL"
WHERE informacion = 'Casos' and 
id_semana = 'SEMANA 01/2025 (del 01/01/2025 al 05/01/2025)' and
"ZONA" = 'ZONA AYSÉN' and
delito = 'HOMICIDIOS'


-------------------------------------------------
SELECT count(distinct rut) FROM public.historial_personal;

----------------------------------------

SELECT count(distinct rut) FROM public.historial_personal
WHERE organismo_nombre_anterior = 'Sin organismo anterior'

-----------------------------