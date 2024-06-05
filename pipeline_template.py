import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.util import BatchElements
from apache_beam import pvalue
import itertools

payment_type_name_map = {1:'Credit Card', 2:'Cash', 3:'No charge', 
                         4:'Dispute', 5:'Unknown',  6:'Voidedtrip'}


datetime_dim_schema = '''datetime_index:INTEGER, tpep_pickup_datetime:DATETIME, PU_year:INTEGER, PU_month:INTEGER, PU_day:INTEGER,
                        'PU_hour':INTEGER, tpep_dropoff_datetime:DATETIME, DO_year:INTEGER, DO_month:INTEGER, DO_day:INTEGER, DO_hour:INTEGER '''

payment_type_dim_schema = '''payment_type_ID:INTEGER, payment_type:INTEGER, payment_type_name:STRING'''

ratecode_dim_schema = '''rate_code_ID:INTEGER, rate_code:INTEGER, rate_code_name:STRING'''

cost_dim_schema = '''cost_ID:INTEGER, fare_amount:FLOAT, tolls_amount:FLOAT, airport_fee:FLOAT,
                    extra:FLOAT, improvement_surcharge:FLOAT, congestion_surcharge:FLOAT,
                    tip_amount:FLOAT, total_amount:FLOAT'''

pickup_location_dim_schema = '''PU_ID:INTEGER, PU_locationID:INTEGER, PU_borough:STRING, PU_zone:STRING, PU_service_zone:STRING'''

dropoff_location_dim_schema = '''DO_ID:INTEGER, DO_locationID:INTEGER, DO_borough:STRING, DO_zone:STRING, DO_service_zone:STRING'''



def create_datetim_dim(row):
    index = row['index']
    tpep_pickup_datetime = row['tpep_pickup_datetime']
    PU_year = tpep_pickup_datetime.year
    PU_month = tpep_pickup_datetime.month
    PU_day = tpep_pickup_datetime.day
    PU_hour = tpep_pickup_datetime.hour
    tpep_dropoff_datetime = row['tpep_dropoff_datetime']
    DO_year = tpep_dropoff_datetime.year
    DO_month = tpep_dropoff_datetime.month
    DO_day = tpep_dropoff_datetime.day
    DO_hour = tpep_dropoff_datetime.hour
        
        

    new_row = {'datetime_ID' : index,
                'tpep_pickup_datetime' : tpep_pickup_datetime,
                'PU_year' : PU_year,
                'PU_day' : PU_day,
                'PU_month' : PU_month,
                'PU_hour' : PU_hour,
                'tpep_dropoff_datetime' : tpep_pickup_datetime,
                'DO_year' : DO_year,
                'DO_month' : DO_month,
                'DO_day' : DO_day,
                'DO_hour' : DO_hour
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
    cost_ID = row['index']
    fare_amount = row['fare_amount']
    tolls_amount = row['tolls_amount']
    airport_fee = row['Airport_fee']
    extra = row['extra']
    improvement_surcharge = row['improvement_surcharge']
    congestion_surcharge = row['congestion_surcharge']
    tip_amount = row['tip']
    total_amount = row['total_amount']

    new_row = {'cost_ID' : cost_ID, 'fare_amount' : fare_amount,
               'tolls_amount' :  tolls_amount, 'airport_fee' : airport_fee, 
               'extra' : extra, 'improvement_surcharge' : improvement_surcharge, 
               'congestion_surcharge' : congestion_surcharge,
               'tip_amount' : tip_amount, 'total_amount' : total_amount}

    return new_row

def create_initial_pickup_location_dim(row):

    PU_ID = row['index']
    PU_LocationID = row['PULocationID']

    new_row = {'PU_ID' : PU_ID, 'PU_LocationID' : PU_LocationID}

    return new_row

def create_initial_dropoff_location_dim(row):

    DO_ID = row['index']
    DO_LocationID = row['DOLocationID']

    new_row = {'DO_ID' : DO_ID, 'DO_LocationID' : DO_LocationID}

    return new_row
     
def run_pipeline(project_id, bucket_name, input_path, output_table):
    
    options = beam.pipeline.PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region='us-central1'  
    )


    with beam.Pipeline(options=options) as p:
    # Read parquet data
        data = (p | 'ReadParquet' >> beam.io.ReadFromParquet(f'gs://{bucket_name}/{input_path}/*.parquet'))
        #TaxiZoneLookup = (p | 'ReadParquet' >> beam.io.ReadFromCsv(f'gs://{bucket_name}/{input_path}/*.csv'))
    # split_data = data | 'SplitName' >> beam.Map(split_name)
    # Define BigQuery schema
    # table_schema = 'first_name:STRING,last_name:STRING' 

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
