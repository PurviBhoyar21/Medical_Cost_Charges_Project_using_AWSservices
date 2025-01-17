Cloud-Based Medical Dataset Analysis Project

Overview

This project leverages a cloud-based approach to analyze medical datasets with distributed computing and visualization techniques. It will process big data and extract meaningful information; based on that, create some visualizations that could make it easier to understand. Key AWS services used include EMR, Glue, S3, QuickSight, VPC, and EC2.

Features

Distributed data processing is done using PySpark on AWS EMR.

Manifests are generated automatically to ease integration with AWS QuickSight.

Scalable and reliable storage of datasets and outputs in AWS S3. 

Creation of interactive dashboards and visualizations in AWS QuickSight. 

Securing infrastructure via AWS VPC and EC2. 

AWS Glue: to validate and test. 

Steps to Execute the Project

1. Setup Environment

AWS S3 Bucket:

Create a bucket named medicalcharges.

Create folders: Input_data/ for the raw dataset; Output_data/ for the processed output, and Manifest_data/ for QuickSight manifest files. 

Dataset Upload:

Upload your medical dataset (e.g., Charges.csv) into s3://medicalcharges/MedicalCharges/Input_data/.

Establish VPC for secure networking. Make sure subnets and security groups allow required access, such as port 22 access for SSH. Launch the EMR Cluster: Master and core nodes to use EC2 instances. Activate PySpark and S3. 2. Data Processing Using PySpark Upload PySpark Script Upload the given PySpark script to the cluster. Run the Script The script will execute to process the dataset and generate the outputs. Key analyses conducted: Average, maximum, and minimum charges by region.

Standard deviation of charges depending on the category of BMI.

Average charges depending on the age.

Count of smokers and nonsmokers in each region.

Comparing charges for gender depending on smoking status.
Output:
Transacted CSVs are persisted in s3://medicalcharges/MedicalCharges/Output_data/.

3. Generating Manifest

Automatic creation of manifests:

The python script elaborates manifest files for all the output datasets

Manifest files are uploaded to s3://medicalcharges/MedicalCharges/Manifest_data/.

Validate Manifest file

This step checks that the manifest files correctly link to output CSVs.
4. Visualization in QuickSight

Connect datasets

Create, in QuickSight, new datasets using the manifest files from the Manifest_data/ folder.

Create Dashboards:

Using the imported datasets, create visualizations such as:

Bar charts for regional charge comparisons.

Pie charts for BMI category distributions.

Line charts for age-based trends.

Publish and Share:

Save dashboards and share them with stakeholders.

5. Validation and Testing

AWS Glue:

Use AWS Glue to test the PySpark script and validate outputs.

Verify Outputs:

Ensure that the processed outputs and manifests are correct.

Validate QuickSight visualizations against expectations.

Project Workflow

Input Data: S3-hosted medical dataset.

Processing: PySpark on EMR.

Storage: Outputs and manifests in S3.

Visualization: Dashboards in QuickSight.

Validation: Glue testing script.

Dependencies

PySpark

AWS CLI

Boto3

AWS Services: EMR, S3, QuickSight, Glue, VPC, EC2

Conclusion

This project showcases how to use AWS cloud services for effective analysis and visualization of a medical dataset. The application of distributed computing and the use of interactive dashboards provide scalable and actionable insights into healthcare data.

References

AWS Documentation:

Amazon EMR

Amazon S3

Amazon Glue

Amazon QuickSight

PySpark Documentation:

PySpark

Dataset Source:

Medical Dataset

