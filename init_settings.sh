CLIENT_BUCKET_NAME=data-from-client-123123  # TOCHANGE to the same name as given in main.tf to bucket client_data

gcloud services disable dataflow.googleapis.com --force
gcloud services enable dataflow.googleapis.com

gcloud storage cp sales_data/*.csv gs://$CLIENT_BUCKET_NAME
gcloud storage cp country_codes.txt gs://$CLIENT_BUCKET_NAME
