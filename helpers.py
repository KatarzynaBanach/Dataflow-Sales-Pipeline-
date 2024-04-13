import yaml
import argparse
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState
import os

def load_from_yaml(file_path):
  with open(file_path, 'r') as f:
    config = yaml.safe_load(f)
  return config

def get_arguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', required=True, help='GCP project')
  parser.add_argument('--region', required=True, help='GCP region')
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--DirectRunner', action='store_true')
  group.add_argument('--DataflowRunner', action='store_true')

  args = parser.parse_args()

  runner='DirectRunner'
  if args.DataflowRunner:
      runner='DataflowRunner'

  return dict(project=args.project,
              region=args.region,
              runner=runner)

def created_dataset_if_needed(name, location, description):
    client = bigquery.Client()
    dataset_name = name
    dataset_id = f'{client.project}.{dataset_name}'

    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset.description = description
        dataset_ref = client.create_dataset(dataset, timeout=60)

def schema_definition_str(schema_json):
    return ', '.join([f"{key}:{value}" for key, value in schema_json.items()])

def clear_bucket(bucket, prefix):
  for blob in bucket.list_blobs(prefix=prefix):
    blob.delete()

def write_state_to_bq(pipeline_state, bucket, prefix):
  pipeline_result = ''
  file_name='pipeline_status.txt'

  if pipeline_state == PipelineState.DONE:
      pipeline_result = 'Pipeline has completed successfully.'
  elif pipeline_state == PipelineState.FAILED:
      pipeline_result = 'Pipeline has failed.'
  else:
      pipeline_result = 'Pipeline in unknown state.'
  
  with open(file_name, 'w') as f:
      f.write(pipeline_result)
  bucket.blob(f'{prefix}{file_name}').upload_from_filename(file_name)
  if os.path.exists(file_name):
      os.remove(file_name)

def move_processed_files(bucket_in, bucket_out, prefix):
    blobs_in = bucket_in.list_blobs(prefix=prefix)
    for blob in blobs_in:
        file_name = blob.name.split('/')[-1]
        destination_blob_name = f'processed_data/{file_name}'
        bucket_in.copy_blob(blob, bucket_out, destination_blob_name)
        blob.delete()