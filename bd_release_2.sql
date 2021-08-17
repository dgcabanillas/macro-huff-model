alter table intercorpretail_automatizaciones.auxtb_datos_eval add column usuario string;
alter table intercorpretail_automatizaciones.auxtb_factor_superm add column usuario string;
alter table intercorpretail_automatizaciones.auxtb_lista_supermercados add column usuario string;
alter table intercorpretail_automatizaciones.auxtb_lista_zonas add column usuario string;
alter table intercorpretail_automatizaciones.auxtb_superm_zona add column usuario string;
alter table intercorpretail_automatizaciones.auxtb_superm_zona add column porcentaje float64;
create table intercorpretail_automatizaciones.auxtb_filtro_manzanas (
    usuario     string,
    manzana     string
)