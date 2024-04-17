# Dataflow Sales Pipeline

### **STACK:**

_python, Apache Beam, GCP, Dataflow, BigQuery, Cloud Storage, Terraform, bash, yaml_

### **RATIONALE:**

Practice and mistakes are the best teachers. Therefore building my own pipeline, facing various challenges and requirements, coming up with 'extra' features, and then trying to implement them as well as looking through the GCP documentation with Eureka moments was a great deep-dive into the Cloud Data Engineering world.

### **PROJECT RESOURCES:**
![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/fa6c674c-e16b-4f00-8f39-fb4b86d46d00)

### **FLOW & FEATURES OF THE SALES PIPELINE:**
* Buckets are created using **Terraform** and raw data is **loaded using cloud shell**.
* Parameters such as **project, region** and type of **runner** are passed **as command line arguments**. 
* **Configuration** and **table schemas** are taken **from .yaml** file.
* Files with row count and status from the previously run pipeline are **removed from bucket**. 
* **Dataset is created** (in not exists) 
* **Raw data** in the form of .**csv and .txt is loaded** from the client's bucket.
* Sales data is cleaned: some **columns are joined** (customer's names), other are split (**the extraction of value and currency** from price), rows with **no id-s are dropped**, deduplication is done.
* Unwanted **symbols are removed** (using regex), **dates are unified**, mapping of sex takes place, **phone numbers** in various formats **are extracted**.
* **System columns**: _created_at and _process_id are created.
* Country codes and names are taken **from different file, split and parsed as dictionary**. Then used as the **side input to replace country code** with full names.
* All rows and **rows per each brand are counted** and written **to a file in a bucket**.
* **Personal customer data** (such as names and phone numbers) that is unnecessary to analysis, is loaded **into BigQuery** (since some customers may already have appeared in BigQuery, firstly **data from BigQuery** is taken -> then **old and new data is merged** -> **deduplication** takes place -> data is **loaded into BigQuery** with the **override** mode).
* Data is **filtered by brands** (a list of brands is taken from .yaml file) and loaded **into separate tables in BigQuery** (with the **append** mode). In the case of **a new brand it is enough to add the name to config.file** and during the next run of a pipeline **a new table** will be **automatically** created and filled with data.
* A **status of pipeline** is written **into bucket**.
* Files with **data that have been processed are moved** to another bucket.

### **DATAFLOW JOB:**
![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/06c613a4-3058-43c1-bb59-b563a51ac5b2)

### **SET UP:**
Open Cloud Shell and choose project:
```
gcloud config set project <PROJECT_ID>
```
Copy repository:
```
git -C ~ clone https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline
```
Change directory:
```
cd Dataflow-Sales-Pipeline/Terraform/
```
Change variables in files:
* _main.tf_ (names of buckets - they have to be unique globally)
* _variables.tf_ (if needed)
* _terraform.tfvars_ (project name and a path of file with service account keys)
Initialize terraform, check possible changes and apply them:
```
terraform init
terraform plan
terraform apply
```
Come back to _Dataflow-Sales-Pipeline/_ directory.
Change variables in files: 
* _init_settings.sh_ (CLIENT_BUCKET_NAME - as the same name as given in _main.tf_ to bucket _client_data_)
* _config.yaml_ (buckets' names as the same names as given in _main.tf_)
(You can changes also other variables if needed - remember that it the best practice to stick to the same region for all resouces within one project)
(In the _sales_pipeline.py_, two lines that move data files from input bucket into output bucket at the end of each pipeline are commented - if necessary you should uncomment them - you can find them looking for _# UNNCOMMENTME_)

Now when all infrastructure  is set and variables are appropriate, the pipeline can be run:
* either directly in cloud shell with proper parameters:
```
python3 sales_pipeline.py --project=<PROJECT_ID> --region=<REGION> <--DirectRunner or --DataflowRunner or empty - default is DirectRunner>
```
* or executing file submit_pipeline.sh (with the same command as above inside it, which I find more convenient). In that case, variables in that file need to be changed and the proper line of code commented.
```
sh submit_pipeline.sh
```
It takes from 5 to 15 minutes (local running is faster then running it with Dataflow). 
If --DataflowRunner is choosen you can check job progress and possible errors in tab Dataflow -> Jobs.

<img src="https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/02b134df-e0ef-4379-bda1-53737223822e" width="300">

If the job status is 'Succeeded' that job was run properly! Congrats!
Details can be found in <output_bucket>/status/pipeline_status.txt and <output_bucket>/status/processed_rows*.txt.
Results will be visible in BigQuery in dataset 'sales'.

**Possible causes of errors:**
* A required API is not allowed.
* Inconsistencies in variable between terraform, init_setup.py and command line arguments.
* Inconsistencies in regions and zones.
* Lack of appropriate permissions for used service account.
* Resource pool exhausted for Dataflow.
* Lack of resources in a choosen region at the moment -> try another region / zone.

_( Waiting to be done:_
* _configuration of Cloud Scheduler - so that it job could be run automatically on daily basis_
* _seting a VM - to execute that files, instead of the cloud shell)_
