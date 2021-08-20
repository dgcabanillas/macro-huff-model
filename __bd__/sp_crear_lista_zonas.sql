CREATE OR REPLACE PROCEDURE intercorpretail_automatizaciones.sp_crear_lista_zonas (
    IN p_eval GEOGRAPHY, 
    IN p_radio FLOAT64,
    IN p_usuario STRING
)
begin
    delete intercorpretail_automatizaciones.auxtb_lista_zonas
    where usuario = p_usuario;
    insert into intercorpretail_automatizaciones.auxtb_lista_zonas
    with 
    base_z_1 as (
        select distinct cod_unico
        from intercorpretail_automatizaciones.Demanda_Manzanas
        where ( ST_DISTANCE(ST_GEOGPOINT(x_manzana, y_manzana), p_eval) / 1E3 ) < p_radio
    ),
    base_z_2 as (
        select 
            A.cod_unico,
            avg(x_manzana) as x_zona,
            avg(y_manzana) as y_zona
        from base_z_1 A
        inner join intercorpretail_automatizaciones.Demanda_Manzanas B
                on A.cod_unico = B.cod_unico
        group by A.cod_unico
    ),
    base_z_3 as (
        select 
            cod_unico,
            trunc(ST_DISTANCE(ST_GEOGPOINT(x_zona, y_zona), p_eval),2) as distancia
        from base_z_2
    ) 
    select 
        cod_unico,
        distancia,
        p_usuario as usuario
    from base_z_3 
    order by distancia asc;
end;