create table intercorpretail_automatizaciones.auxtb_datos_eval (
    nombre      string,
    latitud     float64,
    longitud    float64,
    sv          float64
)

create table intercorpretail_automatizaciones.auxtb_factor_superm (
    nombre      string,
    factor      float64
)

create table intercorpretail_automatizaciones.auxtb_lista_supermercados (
    nombre      string,
    distancia   float64
)

create table intercorpretail_automatizaciones.auxtb_lista_zonas (
    cod_unico   string,
    distancia   float64
)

create table intercorpretail_automatizaciones.auxtb_superm_zona (
    supermercado    string,
    zona            string
)
