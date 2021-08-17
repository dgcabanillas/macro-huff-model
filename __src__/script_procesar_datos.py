import os
import sys
import pandas as pd
from GCP import GCP
from funciones import get_inputs, get_param, validate_inputs, print_args

def insertar_filtros_manzanas ( gcp, data_set, usuario ):
    df_filtro_manzanas = pd.read_excel("__tmp__/tmp_filtro_manzanas.xlsx")
    isEmpty = True
    data_to_insert = "insert into " + data_set + ".auxtb_filtro_manzanas (manzana, usuario) values"

    for i, row in df_filtro_manzanas.iterrows():
        if i == 0: 
            isEmpty = False
        data_to_insert += ("\n\t\t\t('"+ row['ID_MANZANAS'] + "', '" + usuario + "'),")

    if not isEmpty: 
        data_to_insert = data_to_insert[:-1] + ';'
        gcp.ejecutar_sp("""
            begin
                delete """ + data_set + """.auxtb_filtro_manzanas
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
    args = ['sv', 'usuario']
    valid = validate_inputs(args, inputs)

    if valid: 
        print_args(inputs)

        try: 
            # Leemos el dataframe del archivo "__tmp__/tmp_input.xlsx"
            original_df = pd.read_excel("__tmp__/tmp_input.xlsx", index_col=0)

            # Filtramos los casos donde el eval no está seleccionado
            supermercados = original_df.columns.tolist()
            eval_nombre = supermercados[0]          # el eval debe ser el nombre de la primera columna del dataframe
            filter = original_df[eval_nombre] > 0
            filtered_df = original_df[filter]
            zonas = filtered_df.index.tolist()

            # generamos la tabla de relaciones (superm - zonas)
            if(len(zonas) >= 1):
                relation_to_insert = "insert into " + data_set + ".auxtb_superm_zona values"
                for superm in supermercados:
                    for zona in zonas:
                        if filtered_df[superm][zona] > 0:
                            relation_to_insert += ("\n\t\t\t('"+ superm + "', '" + zona + "', '" + inputs['usuario'] + "', " + str(filtered_df[superm][zona]) + "),") 
                relation_to_insert = relation_to_insert[:-1] + ';'
        except:
            print('\nAlgo salió mal.')
            print('\tRevise que exista el archivo "__tmp__/tmp_input.xlsx"')
            return


        try: 
            # Leemos el dataframe del archivo "__tmp__/tmp_factores.xlsx"
            factor_df = pd.read_excel("__tmp__/tmp_factores.xlsx")
            factor_to_insert = "insert into " + data_set + ".auxtb_factor_superm values"
            for i, row in factor_df.iterrows():
                factor_to_insert += ("\n\t\t\t('"+ row['nombre'] + "', " + str(row['factor']) + ", '" + inputs['usuario'] + "'),") 
            factor_to_insert = factor_to_insert[:-1] + ';'
        except:
            print('\nAlgo salió mal.')
            print('\tRevise que exista el archivo "__tmp__/tmp_factores.xlsx"')
            return

        
        gcp = GCP(gcp_project) 

        try:
            insertar_filtros_manzanas( gcp, data_set, inputs['usuario'] )
        except:
            print('\nAlgo salió mal con el filtro de manzanas.')
            print('\tRevise que exista el archivo "__tmp__/tmp_filtro_manzanas.xlsx"')
            return

        try:
            # actualizamos el nombre en la tabla de "auxtb_datos_eval" 
            gcp.ejecutar_sp("""
                begin
                    update """ + data_set + """.auxtb_datos_eval 
                       set nombre='"""+ eval_nombre +"""',
                           sv="""+ inputs['sv'] +""" 
                     where usuario = '"""+ inputs['usuario'] +"""' ;
                end;
            """)

            # Truncamos y llenamos la tabla "auxtb_superm_zona" en la base de datos
            gcp.ejecutar_sp("""
                begin
                    delete """ + data_set + """.auxtb_superm_zona
                    where usuario = '"""+ inputs['usuario'] + """';

                    """ + relation_to_insert + """
                end;
            """, show=True)

            # Truncamos y llenamos la tabla "auxtb_factor_superm" en la base de datos
            gcp.ejecutar_sp("""
                begin
                    delete """ + data_set + """.auxtb_factor_superm
                    where usuario = '"""+ inputs['usuario'] + """';

                    """ + factor_to_insert + """
                end;
            """, show=True)
        except:
            print('\nAlgo salió mal.')
            print('\tRevise el id de proyecto')
            print('\tRevise la estructura de la consulta o procedure')
            print('\tRevise la key para conectar a GCP (json) [Se genera en GCP/IAM/Service Account]')
            return


        
        # Desde acá empieza el cálculo de los resultados
        try:    
            df_mk = gcp.query_to_df("call " + data_set + ".sp_calcular_Mk('" + inputs['usuario'] + "')")

            df_tabla_post = pd.DataFrame( 0.0, 
                columns = original_df.columns.tolist(), 
                index   = original_df.index.tolist()
            )
            for i, row in df_mk.iterrows():
                df_tabla_post[row['supermercado']][row['zona']] = row['M_k']
            
            df_tabla_post.to_excel("__tmp__/tmp_tabla_post.xlsx")
        except:
            print('\nAlgo salió mal.')
            print('\tRevise el id de proyecto')
            print('\tRevise la estructura de la consulta o procedure')
            print('\tRevise la key para conectar a GCP (json) [Se genera en GCP/IAM/Service Account]')
            return

        
        try:
            # Generamos el cuadro final con los resultados
            df_cuadro_final = pd.DataFrame(columns=['ZONA', 'Tiendas', 'SV', 'Total SV', '% SV', 'Factor', 'Dis (Km)', 'Captura Final', '% Captura Final'])
            eval = original_df.columns.tolist()[0]
            l_zonas = original_df.index.tolist()

            for zona in l_zonas:
                df_tmp_zona = df_mk[ df_mk['zona'] == zona ]
                if len(df_tmp_zona.index.tolist()) > 0:
                    row_eval = df_tmp_zona[ df_tmp_zona['supermercado'] == eval ].iloc[0]
                    # variables totales
                    total_SV = df_tmp_zona['SV'].sum() 
                    total_Mk = df_tmp_zona['M_k'].sum()

                    new_row = {
                        'ZONA':             row_eval['zona'],
                        'Tiendas':          row_eval['supermercado'],
                        'SV':               row_eval['SV'],
                        'Total SV':         total_SV,
                        '% SV':             row_eval['SV'] / (total_SV if total_SV > 0 else 1),
                        'Factor':           float(row_eval['FT']),
                        'Dis (Km)':         row_eval['distancia'],
                        'Captura Final':    row_eval['M_k'],
                        '% Captura Final':  row_eval['M_k'] / (total_Mk if total_Mk > 0 else 1)
                    }
                    df_cuadro_final = df_cuadro_final.append(new_row, ignore_index=True)
                    #añadiendo el resto de tiendas
                    df_tmp_superm = df_tmp_zona[ df_tmp_zona['supermercado'] != eval ].sort_values('distancia')
                    if len(df_tmp_superm.index.tolist()) > 0:
                        for i, row in df_tmp_superm.iterrows():
                            new_row = {
                                'ZONA':             row['zona'],
                                'Tiendas':          row['supermercado'],
                                'SV':               row['SV'],
                                'Total SV':         total_SV,
                                '% SV':             row['SV'] / (total_SV if total_SV > 0 else 1),
                                'Factor':           float(row['FT']),
                                'Dis (Km)':         row['distancia'],
                                'Captura Final':    row['M_k'],
                                '% Captura Final':  row['M_k'] / (total_Mk if total_Mk > 0 else 1)
                            }
                            df_cuadro_final = df_cuadro_final.append(new_row, ignore_index=True)
            
            df_cuadro_final.to_excel("__tmp__/tmp_cuadro_final.xlsx", index=False)
            print("Se ha creado el archivo tmp_cuadro_final.xlsx")
        except:
            print('\n4) Algo salió mal en la generación del cuadro final.')
            print('\tPuede que tenga abierto el libro "__tmp__/tmp_cuadro_final.xlsx", ciérrelo por favor.')
            return

    return

# Validamos los parámetros necesarios para ejecutar el script
if __name__  == '__main__':
    inputs = get_inputs(sys.argv)
    main(inputs)
    
    input('\n\nPresione enter para terminar ...')