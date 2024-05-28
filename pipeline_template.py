import apache_beam as beam

def run_pipeline(project_id, bucket_name, input_path, output_table):
  """
  A Dataflow pipeline to read parquet files, split a field, and write to BigQuery.

  Args:
    project_id: Your GCP project ID.
    bucket_name: The name of the Cloud Storage bucket containing the parquet files.
    input_path: The path to the folder containing parquet files within the bucket.
    output_table: The destination BigQuery table (full table reference).
  """

  # Define pipeline options
  options = beam.pipeline.PipelineOptions(
      runner='DataflowRunner',
      project=project_id,
      region='us-central1'  
  )

  with beam.Pipeline(options=options) as p:
    # Read parquet data
    data = (p
            | 'ReadParquet' >> beam.io.ReadFromParquet(
                f'gs://{bucket_name}/{input_path}/*.parquet')
            )

    # Split a string field (replace 'name_field' with your actual field name)
    def split_name(row):
      name_parts = row['name'].split()
      return {
          'first_name': name_parts[0],
          'last_name': name_parts[1],
          # Add other fields from your row as needed
      }
    split_data = data | 'SplitName' >> beam.Map(split_name)

    # Define BigQuery schema
    table_schema = 'first_name:STRING,last_name:STRING' 

    # Write to BigQuery
    split_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        table=output_table,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

# Replace with your project ID, bucket name, input path and BigQuery table details
run_pipeline('nyc-taxi-project-423502', 'pipeline_test_pc', 'data', 'pipeline_test.test_1_table')





# CMD line  that works
# python pipeline_template.py \
#   --project nyc-taxi-project-423502 \
#   --region us-central1 \
#   --staging_location gs://dataflow_testing_pc/staging \
#   --temp_location gs://dataflow_testing_pc/temp \
#   --runner DataflowRunner

