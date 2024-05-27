import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging

class TransformFactTableRow(beam.DoFn):
    def process(self, element):
        transformed_element = {
            'fact_field1': element['trip_ID']
        }
        yield transformed_element

class TransformDimTableRow(beam.DoFn):
    def process(self, element):
        transformed_element = {
            'dim_field1': element['VendorID'],
        }
        yield transformed_element

def run(argv=None):
    pipeline_options = PipelineOptions(flags=argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'nyc-taxi-project-423502'
    google_cloud_options.job_name = 'dataflow_test'
    google_cloud_options.staging_location = 'gs://dataflow_testing_pc/staging'
    google_cloud_options.temp_location = 'gs://dataflow_testing_pc/temp'
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as p:
        parquet_data = (
            p
            | 'ReadFromParquet' >> ReadFromParquet('gs://dataflow_testing_pc/test_data.parquet')
        )

        fact_table_data = (
            parquet_data
            | 'TransformFactTableRow' >> beam.ParDo(TransformFactTableRow())
        )

        dim_table_data = (
            parquet_data
            | 'TransformDimTableRow' >> beam.ParDo(TransformDimTableRow())
        )

        fact_table_data | 'WriteToFactTable' >> WriteToBigQuery(
            'your-dataset.fact_table',
            schema='SCHEMA_AUTODETECT',  # Replace with your schema if necessary
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        dim_table_data | 'WriteToDimTable' >> WriteToBigQuery(
            'your-dataset.dim_table',
            schema='SCHEMA_AUTODETECT',  # Replace with your schema if necessary
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


