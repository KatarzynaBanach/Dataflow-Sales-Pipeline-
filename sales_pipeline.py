import apache_beam as beam
import google.cloud.storage as gcs
from apache_beam.runners.runner import PipelineState
from apache_beam.options.pipeline_options import PipelineOptions
import google.cloud.storage as gcs
from helpers import *


# Split rows by choosen delimiter.
class SplitWords(beam.DoFn):
  def __init__(self, delimiter=','):
    self.delimiter = delimiter

  def process(self, element):
    return [element.split(self.delimiter)]

# Filter out rows with empty id's columns.
def is_non_empty(cols):
  if cols[0] == '' or cols[9] == '':
    return False
  else:
    return True

# Clean data out of redundant symbols.
class SymbolsCleaning(beam.DoFn):
  def process(self, element):
    import re
    row = []
    for idx, col in enumerate(element):
        # exeption for date column and price
        if idx in (4,10,12):
            row.append(col)
        else:
            row.append(re.sub(r'[#$%&.?]','',col))

    return [row]

# Join names into one column and capitalize.
def join_names(cols):
  names = ' '.join([c.capitalize() for c in cols[1:4] if c != ''])
  return [cols[0], names, *cols[4:]]

def unify_date(cols):
    import pandas as pd
    cols[2] = str(pd.to_datetime(cols[2], dayfirst=False).date())
    cols[8] = str(pd.to_datetime(cols[8], dayfirst=False).date())
    if cols[2] == 'NaT':
      cols[2] = ''
    if cols[8] == 'NaT':
      cols[8] = ''
    return cols

def sex_map(cols):
  mapping = {'f': 'Female', 'm': 'Male', '-': 'Unknown'}
  cols[4] = mapping.get(cols[4], 'Unknown')
  return cols

def phone_number_extract(cols):
  cols[5] = ''.join([c for c in cols[5] if c.isdigit()])
  return cols

def currency_extract(cols):
  import re
  price = re.search(r'([\d.,]+)([a-zA-Z]+)', cols[10])
  value = price.group(1).replace(',','.')
  currency = price.group(2)
  return [*cols[:10], value, currency, *cols[11:]]

def add_created_at(cols):
    from datetime import datetime
    _created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return [*cols, _created_at]

def generate_uuid():
  import uuid
  x = uuid.uuid4()
  return [x]

def split_words(text):
  return text.split(',')

def parse_to_dict(pair):
  key = pair.strip('()').split(';')[0]
  value = pair.strip('()').split(';')[1]
  return (key, value)

def replace_country_code(sales, countries):
  sales[3] = countries[sales[3]]
  return sales

def to_json_sales(cols):
  json_str = {'client_id': cols[0],
              'birthday': cols[1],
              'nationality': cols[2],
              'sex': cols[3],
              'source': cols[4],
              'transaction_id': cols[5],
              'transation_date': cols[6],
              'item_id': cols[7],
              'price': cols[8],
              'currency': cols[9],
              '_created_at': cols[10],
              '_process_id': cols[11]
              }
  return json_str

def to_json_customers(cols):
  cols = cols.split(',')
  json_str = {'client_id': cols[0],
              'name': cols[1],
              'phone_number': cols[2]
              }
  return json_str

def to_str_customers(json_line):
  client_id = json_line['client_id']
  name = json_line['name']
  phone_number = json_line['phone_number']
  return f'{client_id},{name},{phone_number}'


def brands_to_bq(pcoll, brand, table_prefix, schema, bucket):
  return (
          pcoll
          | f'Filter by {brand}' >> beam.Filter(lambda cols: cols[12] == brand)
          | f'{brand} Remove name, phone numbers and brand' >> beam.Map(lambda cols: [cols[0], *cols[2:5], *cols[6:12], *cols[13:]])
          | f'{brand} to json' >> beam.Map(to_json_sales)
          | f'{brand} write to BQ' >> beam.io.WriteToBigQuery(
              table=f'{table_prefix}sales_{brand}',
              schema=schema,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              custom_gcs_temp_location=f'gs://{bucket}/staging',
              additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
              )
        )

def run():

    # Loading configuration from yaml.
    config = load_from_yaml('config.yaml')
    brands_list = config['brands']
    input_bucket = config['buckets']['from_client']
    output_bucket = config['buckets']['processed_data']
    dataset_name = config['dataset']['name']
    dataset_location = config['dataset']['location']
    dataset_desc = config['dataset']['description']
    schema_definition_sales = schema_definition_str(load_from_yaml('schemas.yaml')['schema_sales'])
    schema_definition_customer = schema_definition_str(load_from_yaml('schemas.yaml')['schema_customers'])

    # Clear bucket with results.
    bucket_out = gcs.Client().get_bucket(output_bucket)
    clear_bucket(bucket_out, 'status/')

    # Create dataset if not exits.
    created_dataset_if_needed(dataset_name, dataset_location, dataset_desc)

    # Get and pass options for Pipeline.
    args = get_arguments()
    options = PipelineOptions(
        project=args['project'],
        region=args['region'],
        job_name='salesjob',
        temp_location=f'gs://{output_bucket}/staging',
        staging_location=f'gs://{output_bucket}/staging',
        runner=args['runner'],
        worker_machine_type='e2-standard-2'   # all that options to config?
    )

    p = beam.Pipeline(options=options)

    countries = (
    p
    | beam.io.ReadFromText('gs://data-from-client-123123/country_codes.txt')
    | beam.FlatMap(split_words)
    | beam.Map(parse_to_dict)
    )

    process_id = p | 'Create process identifier' >> beam.Create(generate_uuid())

    cleaned_data = (
        p
        | 'Read from File' >> beam.io.textio.ReadFromText('gs://data-from-client-123123/sales_data*.csv', skip_header_lines=True)
        | 'Get rid of duplicates' >> beam.Distinct()
        | 'Split rows' >> beam.ParDo(SplitWords(delimiter=';'))
        | 'Filter out rows with empty ids' >> beam.Filter(is_non_empty)
        | 'Clean out of unwanted symbols' >> beam.ParDo(SymbolsCleaning()) 
        | 'Join and capitalize names' >> beam.Map(join_names)
        | 'Unify date' >> beam.Map(unify_date)
        | 'Sex mapping' >> beam.Map(sex_map)
        | 'Extract phone number' >> beam.Map(phone_number_extract)
        | 'Split price into value and currency' >> beam.Map(currency_extract)
        | 'Add created_at column' >> beam.Map(add_created_at)
        | 'Add id' >> beam.Map(lambda cols, id: [*cols, str(id)], id=beam.pvalue.AsSingleton(process_id))
        | 'Full country name' >> beam.Map(replace_country_code, countries=beam.pvalue.AsDict(countries))
    )

    total_count = (
        cleaned_data
        | 'Total count' >> beam.combiners.Count.Globally()
        | 'Total map' >> beam.Map(lambda x: f'Total rows: '+ str(x)) 
        )

    count_per_source = (
        cleaned_data
        | 'Extract brands' >> beam.Map(lambda cols: (cols[12], 1))
        | 'Rows by brand' >> beam.CombinePerKey(sum)
        | 'Format' >> beam.Map(lambda x: f'{x[0]} rows: {x[1]}')
        )

    (
        (total_count, count_per_source)
        | beam.Flatten()
        | beam.io.WriteToText(f'gs://{output_bucket}/status/processed_rows',file_name_suffix='.txt')
    )

    # Get customer data and write to bq.
    new_customer_data = (
        cleaned_data
        | 'Get customer id, name and phone number' >> beam.Map(lambda cols: f'{cols[0]},{cols[1]},{cols[5]}')
    )

    old_customer_data = (
        p
        | beam.io.ReadFromBigQuery(table='phonic-altar-416918.sales.customer_data')
        | beam.Map(to_str_customers)
    )

    (
        (new_customer_data, old_customer_data)
        | 'Join new and old data together' >> beam.Flatten()
        | 'Deduplicate' >> beam.Distinct()
        | 'Customer to json' >> beam.Map(to_json_customers)
        | 'Customers write to BQ' >> beam.io.WriteToBigQuery(
            table=f"{args['project']}:{dataset_name}.customer_data",
            schema=schema_definition_customer,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location=f'gs://{output_bucket}/staging'
            )
    )

    # Filter by brand and write to separate tables.
    for brand in brands_list:
      brands_to_bq(cleaned_data, brand, f"{args['project']}:{dataset_name}.", schema_definition_sales, output_bucket)

    # Running pipeline, getting its state and writing it into Cloud Storage.
    pipeline_state = p.run().wait_until_finish()
    write_state_to_bq(pipeline_state, bucket_out, 'status/')

    # Move files into processed folder.
    # bucket_in = gcs.Client().get_bucket(input_bucket)
    # move_processed_files(bucket_in, bucket_out, 'sales_data')

if __name__ == '__main__':
  run()