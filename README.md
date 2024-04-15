# Dataflow Sales Pipeline
GCP | Apache Beam | Dataflow

**_IN PROGRESS_**


SET UP:
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
* main.tf (names of buckets - the have to be unique globally)
* variables.tf (if needed)
* terraform.tfvars (project name and path of file with service account keys)
Initialize terraform, check possible changes and apply them::
```
terraform init
terraform plan
terraform apply
```



![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/78d581b8-8c7f-4220-a540-698e90a05e9d)



![obraz](https://github.com/KatarzynaBanach/Dataflow-Sales-Pipeline/assets/102869680/06c613a4-3058-43c1-bb59-b563a51ac5b2)
