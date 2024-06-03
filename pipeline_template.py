import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.util import BatchElements
from apache_beam import pvalue
import itertools

payment_type_name_map = {1:'Credit Card', 2:'Cash', 3:'No charge', 
                         4:'Dispute', 5:'Unknown',  6:'Voidedtrip'}

def create_datetim_dim(row):
    index = row['index']
    tpep_pickup_datetime = row['tpep_pickup_datetime']
    PU_year = tpep_pickup_datetime.year
    PU_month = tpep_pickup_datetime.month
    PU_day = tpep_pickup_datetime.day
    #PU_hour = tpep_pickup_datetime.hour
    tpep_dropoff_datetime = row['tpep_dropoff_datetime']
    DO_year = tpep_dropoff_datetime.year
    DO_month = tpep_dropoff_datetime.month
    DO_day = tpep_dropoff_datetime.day
        
        

    new_row = {'index' : index,
                'tpep_pickup_datetime' : tpep_pickup_datetime,
                'PU_day' : PU_day,
                'PU_month' : PU_month,
                'PU_year' : PU_year,
                'tpep_dropoff_datetime' : tpep_pickup_datetime,
                'DO_year' : DO_year,
                'DO_month' : DO_month,
                'DO_day' : DO_day
                }
        
    return new_row 

def create_payment_type_dim(row):   
        payment_type_ID = row['index']
        fare_amount = row['fare_amount']
        payment_type = row['payment_type']

        new_row = {'payment_type_ID' : payment_type_ID,
                   'fare_amount' : fare_amount,
                   'payment_type' : payment_type
                   }
        
        return new_row

def map_payment_type(row):
    row['payment_type_name'] = payment_type_name_map.get(row['payment_type'], 'Unknown')
    return row


def create_ratecode_dim(row):
    rate_code_ID = row['index']
    rate_code = row['RateCodeID']
    rate_code_name = row['RateCode']

    new_row = {'rate_code_ID' : rate_code_ID,
               'rate_code' : rate_code,
               'rate_code_name' : rate_code_name
               }
    
    return new_row
    
def create_cost_dim(row):
     
    return None
    
       


def run_pipeline(project_id, bucket_name, input_path, output_table):
    
    options = beam.pipeline.PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region='us-central1'  
    )


    with beam.Pipeline(options=options) as p:
    # Read parquet data
        data = (p | 'ReadParquet' >> beam.io.ReadFromParquet(f'gs://{bucket_name}/{input_path}/*.parquet'))

    # split_data = data | 'SplitName' >> beam.Map(split_name)
    # Define BigQuery schema
    # table_schema = 'first_name:STRING,last_name:STRING' 
    datetime_dim_schema = '''index:INTEGER, tpep_pickup_datetime:DATETIME, PU_day:INTEGER, PU_month:INTEGER, 
                             PU_year:INTEGER, tpep_dropoff_datetime:DATETIME '''
    datetime_dim = data | 'CreateDateTimeDim' >> beam.Map(create_datetim_dim)
    
    datetime_dim | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        table=output_table,
        schema=datetime_dim_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )


# Replace with your project ID, bucket name, input path and BigQuery table details
run_pipeline('nyc-taxi-project-423502', 'pipeline_test_pc', 'nyc_data', 'nyc_pipeline_test.test_table')





# CMD line  that works
# python pipeline_template.py \
#   --project nyc-taxi-project-423502 \
#   --region us-central1 \
#   --staging_location gs://pipeline_test_pc/staging \
#   --temp_location gs://pipeline_test_pc/temp \
#   --runner DataflowRunner

