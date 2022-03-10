
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

bq_client = bigquery.Client(project='datasets-331519')
gcs_bucket = "amazon-rebeca"

def hello_gcs():
    prefix_folder = "amazon-stg/public/"
    bucket = storage.Client().bucket(gcs_bucket)
    for blob in bucket.list_blobs(prefix = prefix_folder):
        print("Blob: {}".format(blob.name)) #Checking for csv blobs as list_blobs also returns folder_name

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,)

        #encontrar el filename
        table_name = (blob.name).split('/')[-2]
        bq_table_id = "becade_ralvaradoc." + table_name

        #intentamos buscar si está la tabla
        try:
            bq_client.get_table(bq_table_id)
            print("La tabla {} ya esta, ya no se cargara\n".format(table_name))
        except NotFound:
            uri = "gs://{bucket}/{blob}".format(bucket = gcs_bucket, blob = blob.name)
            print("URI: " + uri)

            load_job = bq_client.load_table_from_uri(
                   uri, bq_table_id, job_config=job_config
               )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            destination_table = bq_client.get_table(bq_table_id)  # Make an API request.
            print("Tabla {}, se cargaron {} filas\n".format(bq_table_id, destination_table.num_rows))

hello_gcs()


# El problema era que en el bq_table_id estaba poniendo el nombre del proyecto, y luego intente no ponerselo pero me decía que se lo pusiera. Se lo puse al momneto de definir el cliente y ya con eso funcionó