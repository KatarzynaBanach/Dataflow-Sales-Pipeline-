# Dataflow Sales Pipeline
**_IN PROGRESS_**

**STACK**

**_Apache Beam, python, Dataflow, GCP, bash, BigQuery, Cloud Storage, Terraform_**

**RATIONALE**
Practice and mistakes are the best teacher. Therefore building my own pipeline, facing various challenges and requirements, coming up with 'extra' features and then trying to implement them as well as looking through the GCP documentation with Eureka moments was a great deep-dive into Cloud Data Engineering world.



**PROJECT RESOURCES:**
![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/78d581b8-8c7f-4220-a540-698e90a05e9d)


**DATAFLOW JOB:**
![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/06c613a4-3058-43c1-bb59-b563a51ac5b2)


**SET UP:**
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



