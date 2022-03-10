#para cargar datos de gcs a bq

from google.cloud import bigquery
from google.cloud import storage

# Construct a BigQuery client object.
client = bigquery.Client()

def load_gcs_bq_parquet(ds_id, gs_uri):
    print("Cargando tabla " + ds_id + "\n")

    table_id = "datasets-331519.becade_ralvaradoc." + ds_id
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)
    uri = gs_uri

    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

clientes = "gs://amazon-rebeca/amazon-stg/public/stg_clients/2022_03_07_1646612657978_0.parquet"
compras = "gs://amazon-rebeca/amazon-stg/public/stg_compras/2022_03_07_1646612657978_0.parquet"
#external_products =
#products = 
#tasas_pais =


load_gcs_bq_parquet('clientes', clientes)
load_gcs_bq_parquet('compras', compras)