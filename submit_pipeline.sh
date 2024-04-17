PROJECT_ID="phonic-altar-416918"  #TOCHANGE
REGION="europe-central2"

# python3 sales_pipeline.py beam_test.py --project=$PROJECT_ID --region=$REGION --DataflowRunner
python3 sales_pipeline.py --project=$PROJECT_ID --region=$REGION --DirectRunner
