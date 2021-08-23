import os
import sys
import pandas as pd
from GCP import GCP
from funciones import get_inputs, get_param, validate_inputs, print_args

def insertar_formatos_seleccionados ( gcp, data_set, usuario ):
    df_formatos = pd.read_excel("__tmp__/tmp_formatos_seleccionados.xlsx")
    isEmpty = True
    data_to_insert = "insert into " + data_set + ".auxtb_formatos_seleccionados (formato, usuario) values"

    for i, row in df_formatos.iterrows():
        if i == 0: 
            isEmpty = False
        data_to_insert += ("\n\t\t\t('"+ row['Formatos'] + "', '" + usuario + "'),")

    if not isEmpty: 
        data_to_insert = data_to_insert[:-1] + ';'
        gcp.ejecutar_sp("""
            begin
                delete """ + data_set + """.auxtb_formatos_seleccionados
                where usuario = '"""+ usuario + """';

                """ + data_to_insert + """
            end;
        """, show=True)

def main( inputs ):
    # Cambiamos al directorio donde se encuentra este script
    os.chdir(sys.path[0])

    # parámetros de conexión a GCP
    gcp_project = get_param('gcp_project')
    data_set    = get_param('data_set')

    # Listamos los argumentos necesarios y validamos que no falte alguno
    args = ['latitud', 'longitud', 'radio', 'usuario']
    valid = validate_inputs(args, inputs)

    if valid: 
        print_args(inputs)

        try: 
            gcp = GCP(gcp_project)

            #Guardamos la información del eval en la tabla "auxtb_datos_eval"
            gcp.ejecutar_sp("""
                begin
                    delete """ + data_set + """.auxtb_datos_eval
                    where usuario = '"""+ inputs['usuario'] + """';

                    insert into """ + data_set + """.auxtb_datos_eval (latitud, longitud, usuario)
                         values ("""+ inputs['latitud'] +""", """+ inputs['longitud'] +""", '""" + inputs['usuario'] + """');
                end;
            """)
            
            # Ejecutamos el stored procedure que alimenta las tablas
            #   - auxtb_lista_zonas
            #   - auxtb_lista_supermercados
            gcp.ejecutar_sp("""
                begin 
                    declare v_eval GEOGRAPHY default ST_GEOGPOINT(""" + inputs['longitud'] + "," + inputs['latitud'] + """);
                    declare v_radio FLOAT64 default """ + inputs['radio'] +""";
                    declare v_usuario STRING default '""" + inputs['usuario'] + """';

                    call """ + data_set + """.sp_crear_lista_zonas (v_eval, v_radio, v_usuario);
                    call """ + data_set + """.sp_crear_lista_supermercados (v_eval, v_radio, v_usuario, '');
                end;
            """)

            # Obtenemos la lista de zonas
            df_zonas = gcp.query_to_df("""
                select 
                    cod_unico, 
                    distancia 
                from """ + data_set + """.auxtb_lista_zonas 
                where usuario = '"""+ inputs['usuario'] + """'
                order by distancia;
            """)

            # Obtenemos la lista de supermercados
            df_superm = gcp.query_to_df("""
                select 
                    A.nombre,
                    B.factor,
                    A.distancia
                from """ + data_set + """.auxtb_lista_supermercados A
                left join """ + data_set + """.Demanda_Supermercados B
                    on A.nombre = B.nombre
                where usuario = '"""+ inputs['usuario'] + """'
                order by A.distancia;
            """)
            df_superm[['nombre', 'factor']].to_excel('__tmp__/tmp_factores.xlsx', index=False)
            print("\nArchivo '__tmp__/tmp_factores.xlsx' creado y guardado")

            # Obtenemos las relaciones entre supermercados y zonas
            df_superm_zona = gcp.query_to_df("""
                select distinct
                    A.nombre_de_la_tienda as nombre,
                    A.cod_unico
                from """ + data_set + """.Demanda_Supermercado_Zs A
                inner join """ + data_set + """.auxtb_lista_zonas B
                        on A.cod_unico = B.cod_unico
                       and B.usuario = '"""+ inputs['usuario'] + """'
                inner join """ + data_set + """.auxtb_lista_supermercados C
                        on A.nombre_de_la_tienda = C.nombre
                       and C.usuario = '"""+ inputs['usuario'] + """'
                where A.nombre_de_la_tienda is not null 
                  and A.cod_unico is not null;
            """)
        except:
            print('\nAlgo salió mal.')
            print('\tRevise el id de proyecto')
            print('\tRevise la estructura de la consulta o procedure')
            print('\tRevise la key para conectar a GCP (json) [Se genera en GCP/IAM/Service Account]')
            print('\tCierre el archivo "tmp_factores.xlsx"')
            return

        try:
            # Creamos el tablero que se va a exportar
            df_matrix = pd.DataFrame( 0, 
                columns = df_superm['nombre'].to_list(), 
                index   = df_zonas['cod_unico'].to_list()
            )
            for i, row in df_superm_zona.iterrows():
                df_matrix[row['nombre']][row['cod_unico']] = 1
            df_matrix.to_excel('__tmp__/tmp_output.xlsx')
            print("\nArchivo '__tmp__/tmp_output.xlsx' creado y guardado")
        except:
            print('\nAlgo salió mal en la creación del tablero.')
            print('\tCierre el archivo "tmp_output.xlsx"')
            return

        try:
            insertar_formatos_seleccionados( gcp, data_set, inputs['usuario'] )
        except:
            print('\nAlgo salió mal en la inserción de formatos.')
            print('\tCierre el archivo "tmp_formatos_seleccionados.xlsx"')
            return

    return

# Validamos los parámetros necesarios para ejecutar el script
if __name__  == '__main__':
    inputs = get_inputs(sys.argv)
    main(inputs)
    input('\n\nPresione enter para terminar ...')