import os
import sys
from google.cloud import bigquery

class GCP:
    def __init__ (self, gcp_project):
        os.chdir(sys.path[0])
        os.chdir('..')
        key_route = os.getcwd().replace('\\', '/') + '/__key__/key.json'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_route
        self.client = bigquery.Client(project=gcp_project)

    def ejecutar_sp (self, sp, show=True):
        print("\nEjecutando Stored procedure: ")
        if show:
            print(sp)
        instance = self.client.query(sp)
        instance.result()
        print("Ejecución terminada.\n\n")

    def query_to_df (self, query, show=True):
        print("\nEjecutando consulta: ")
        if show:
            print(query)
        instance = self.client.query(query)
        results = instance.result()
        print("Ejecución terminada.\n\n")
        return results.to_dataframe()