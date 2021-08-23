CREATE OR REPLACE PROCEDURE intercorpretail_automatizaciones.sp_calcular_Mk ( 
    IN p_usuario STRING
)
begin
    with 
    prebase_manzanas as (
        select distinct manzana
        from intercorpretail_automatizaciones.auxtb_filtro_manzanas
        where usuario = p_usuario
    ),
    base_zonas_parciales as (
        select distinct A.cod_unico
        from intercorpretail_automatizaciones.Demanda_Manzanas A
        inner join prebase_manzanas B
                on A.id_manzana = B.manzana
    ),
    aux_demanda_manzanas as (

        select A.* except(Poligono_Zs)
        from intercorpretail_automatizaciones.Demanda_Manzanas A
        left join base_zonas_parciales B
               on A.cod_unico = B.cod_unico
        where B.cod_unico is null

        union distinct

        select A.* except(Poligono_Zs)
        from intercorpretail_automatizaciones.Demanda_Manzanas A
        inner join prebase_manzanas B
                on A.id_manzana = B.manzana

    ),
    base as (
        select 
            zona,
            supermercado,
            min(porcentaje) as porcentaje
        from intercorpretail_automatizaciones.auxtb_superm_zona
        where usuario = p_usuario
        group by zona, supermercado
    ),
    base_supermercado_prev as (
        select nombre, latitud, longitud, sv,
        from intercorpretail_automatizaciones.Demanda_Supermercados
        where nombre not in (
            select nombre 
            from intercorpretail_automatizaciones.auxtb_datos_eval 
            where nombre is not null
              and usuario = p_usuario
        )
        and nombre in (select distinct supermercado from base)
        union all
        select nombre, latitud, longitud, sv
        from intercorpretail_automatizaciones.auxtb_datos_eval
        where nombre is not null
          and usuario = p_usuario
    ),
    base_supermercado as (
        select 
            A.*,
            B.factor
        from base_supermercado_prev A
        left join intercorpretail_automatizaciones.auxtb_factor_superm B
               on A.nombre = B.nombre
              and B.usuario = p_usuario
    ),
    base_coord_zonas as (
        select 
            A.zona,
            avg(B.x_manzana) as longitud,
            avg(B.y_manzana) as latitud
        from ( select distinct zona from base ) A
        inner join aux_demanda_manzanas B 
                on A.zona = B.cod_unico
        group by A.zona
    ),
    base_dist_zona_superm as (
        select 
            A.zona,
            A.supermercado,
            round(ST_DISTANCE(
                    ST_GEOGPOINT(B.longitud, B.latitud), 
                    ST_GEOGPOINT(C.longitud, C.latitud)) / 1E3, 3) as distancia
        from base A
        inner join base_supermercado B on A.supermercado = B.nombre
        inner join base_coord_zonas C on A.zona = C.zona
    ),
    base_Fk_Dk as (
        select 
            A.supermercado,
            A.zona,
            B.id_manzana,
            B.GF                      as G_k,
            C.factor * A.porcentaje   as F_k,
            round(
                ST_DISTANCE(
                    ST_GEOGPOINT(B.x_manzana, B.y_manzana), 
                    ST_GEOGPOINT(C.longitud, C.latitud)), 2) as D_k
        from base A
        left join aux_demanda_manzanas B
            on A.zona = B.cod_unico
        left join base_supermercado C
            on A.supermercado = C.nombre
    ),
    base_Pk as (
        select 
            supermercado,
            zona,
            id_manzana,
            G_k,
            F_k / D_k as P_k
        from base_Fk_Dk
    ),
    base_Pk_agrupado as (
        select id_manzana, sum(P_k) as SumaP_k
        from base_Pk 
        group by id_manzana
    ),
    base_Mk_manzana as (
        select 
            A.supermercado,
            A.zona,
            A.id_manzana,
            round(A.G_k * A.P_k / SumaP_k, 2) as M_k
        from base_Pk A
        inner join base_Pk_agrupado B 
                on A.id_manzana = B.id_manzana
    ),
    base_Mk_zona as (
        select 
            supermercado,
            zona,
            round(sum(M_k), 2) as M_k
        from base_Mk_manzana 
        group by supermercado, zona
    )
    select 
        A.supermercado,
        case when E.cod_unico is null then A.zona
             else A.zona || '"' end         as zona,
        A.M_k,
        B.distancia,
        C.sv                                as SV,
        round(C.factor * D.porcentaje, 2)   as FT
    from base_Mk_zona A
    left join base_dist_zona_superm B
           on A.zona = B.zona
          and A.supermercado = B.supermercado 
    left join base_supermercado C
           on A.supermercado = C.nombre
    inner join base D
            on A.supermercado = D.supermercado
           and A.zona = D.zona
    left join base_zonas_parciales E
           on A.zona = E.cod_unico
    order by A.zona, B.distancia;
end;
