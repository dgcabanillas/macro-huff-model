CREATE OR REPLACE PROCEDURE intercorpretail_automatizaciones.sp_crear_lista_supermercados (
    IN p_eval       GEOGRAPHY, 
    IN p_radio      FLOAT64,
    IN p_usuario    STRING,
    IN p_formato    STRING
)
begin 
    delete intercorpretail_automatizaciones.auxtb_lista_supermercados
    where usuario = p_usuario;
    insert into intercorpretail_automatizaciones.auxtb_lista_supermercados
    with 
    base_formatos as (
        select distinct formato
        from intercorpretail_automatizaciones.auxtb_formatos_seleccionados
        where usuario = p_usuario
    ),
    base_nombres_super as (
        select distinct C.nombre
        from intercorpretail_automatizaciones.auxtb_lista_zonas A
        inner join intercorpretail_automatizaciones.Demanda_Manzanas B
                on A.cod_unico = B.cod_unico
        left join intercorpretail_automatizaciones.Demanda_Supermercados C 
               on B.ciudad = C.ciudad 
              -- acá se puede usar cod_unico de A y C si C lo tuviera
              and ST_DISTANCE(ST_GEOGPOINT(B.x_manzana, B.y_manzana), ST_GEOGPOINT(C.longitud, C.latitud)) < 500 
        where A.usuario = p_usuario
          and (
              C.formato in (select formato from base_formatos)
              or '--todos--' in (select formato from base_formatos)
          )
          and C.nombre is not null

        union distinct

        select distinct C.nombre
        from intercorpretail_automatizaciones.auxtb_lista_zonas A
        left join intercorpretail_automatizaciones.Demanda_Supermercado_Zs B 
            on A.cod_unico = B.cod_unico
        inner join intercorpretail_automatizaciones.Demanda_Supermercados C
                on B.nombre_de_la_tienda = C.nombre
        where A.usuario = p_usuario
        and C.nombre is not null
    ),
    base_distancia_a_eval as (
        select 
            A.nombre,
            ST_DISTANCE(ST_GEOGPOINT(B.longitud, B.latitud), p_eval) as distancia
        from base_nombres_super A
        inner join intercorpretail_automatizaciones.Demanda_Supermercados B
                on A.nombre = B.nombre
    )
    select 
        nombre,
        distancia,
        p_usuario as usuario
    from base_distancia_a_eval
    order by distancia asc;
end;