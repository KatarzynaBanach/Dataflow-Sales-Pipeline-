# Dataflow Sales Pipeline
**_IN PROGRESS_**

### **STACK:**

_python, Apache Beam, GCP, Dataflow, BigQuery, Cloud Storage, Terraform, bash, yaml_

### **PROJECT RESOURCES:**
![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/fa6c674c-e16b-4f00-8f39-fb4b86d46d00)

### **RATIONALE:**

Practice  and mistakes are the best teacher. Therefore building my own pipeline, facing various challenges and requirements, coming up with 'extra' features and then trying to implement them as well as looking through the GCP documentation with Eureka moments was a great deep-dive into Cloud Data Engineering world.

### **Flow & features of the Sales Pipeline:**
* Buckets are created using Terraform and raw data is loaded using cloud shell.
* Parameters such as project, region and type of runner are passed as command line arguments. 
* Configuration and table schemas are taken from .yaml file.
* Files with rows count and status from previosly run pipeline are removed from bucket. 
* Dataset is created (in not exists) 
* Raw data in form of .csv and .txt is loaded from the client's bucket.
* Sales data is cleaned: some columns are joined (customer's names), other are split (the extraction of value and currency from price), rows with no id-s are dropped, deduplication is done.
* Unwanted symbols are removed (using regex), dates are unified, mapping of sex takes place, phone numbers in various formats are extracted.
* System columns: _created_at and _process_id are created.
* Country codes and names are taken from different file, split and parsed as dictionary. Then used as the side input to replace country code with a full names.
* All rows and rows per each brand are counted and written to a file in a bucket.
* Personal customer data (such as names and phonenumbers) that is unnecessary to analysis, is loaded into BigQuery (since some customers may already have appeard in BigQuery, firstly data from BigQuery is taken -> then old and new data is merged -> dedupliacation takes place -> data is loaded into BigQuery with the override mode).
* Data is filtered by brands (list of brands is taken from .yaml file) and loaded into separate tables in BigQuery (with the append mode).
* A status of pipeline is written into bucket.
* Files with data that have been processed are moved to another bucket.

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
* _main.tf_ (names of buckets - the have to be unique globally)
* _variables.tf_ (if needed)
* _terraform.tfvars_ (project name and path of file with service account keys)
Initialize terraform, check possible changes and apply them::
```
terraform init
terraform plan
terraform apply
```
Come back to _Dataflow-Sales-Pipeline/_ directory.
Change variables in files: init_settings.sh (the same name as give in _main.tf_ to bucket _client_data_)  



