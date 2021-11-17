import os
import sys
from GCP import GCP
from funciones import get_param

def main():
    # Cambiamos al directorio donde se encuentra este script
    os.chdir(sys.path[0])
    print(os.getcwd())

    # parámetros de conexión a GCP
    gcp_project = get_param('gcp_project')
    data_set    = get_param('data_set')

    try: 
        gcp = GCP(gcp_project)

        df_formato = gcp.query_to_df("""
            select distinct formato 
            from """ + data_set + """.Demanda_Supermercados
            where trim(formato) <> '';
        """)

        df_formato.to_excel('__src__/__tmp__/tmp_lista_formatos.xlsx')
        print("\nArchivo '__src__/__tmp__/tmp_lista_formato.xlsx' creado y guardado")
    except:
        print('\nAlgo salió mal.')
        print("Tal vez tenga el archivo '__src__/__tmp__/tmp_lista_formatos.xlsx' abierto.")
        return

# Validamos los parámetros necesarios para ejecutar el script
if __name__  == '__main__':
    main()
    input('\n\nPresione enter para terminar ...')
