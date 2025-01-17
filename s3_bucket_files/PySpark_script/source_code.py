import sys
import json
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, stddev, when

# Initialize Spark
spark = SparkSession.builder.appName("ExtendedMedicalChargesAnalysisWithManifest").getOrCreate()

# Initialize S3 client
s3_client = boto3.client('s3')

# Define S3 paths
input_path = "s3://clouddump/Input_data/Charges.csv"
output_path_base = "s3://clouddump/Output_data/"
manifest_base_path = "s3://clouddump/Manifest_data/"

def generate_and_upload_manifest(s3_output_path, manifest_s3_path):
    """
    Generates a JSON manifest file for QuickSight visualization and uploads it to S3.
    """
    manifest_data = {
        "fileLocations": [{"URIs": [s3_output_path]}],
        "globalUploadSettings": {
            "format": "CSV", 
            "delimiter": ",",
            "textQualifier": "\"",
            "containsHeader": True
        }
    }
    
    # Create a local manifest file
    local_manifest_path = "/tmp/manifest.json"
    with open(local_manifest_path, 'w') as manifest_file:
        json.dump(manifest_data, manifest_file, indent=4)
    
    # Parse bucket and key from manifest_s3_path
    bucket_name = manifest_s3_path.split("/")[2]
    manifest_key = "/".join(manifest_s3_path.split("/")[3:])
    
    # Upload the manifest file to S3
    s3_client.upload_file(local_manifest_path, bucket_name, manifest_key)
    print(f"Manifest file uploaded to: {manifest_s3_path}")

# Load data into a DataFrame
data = spark.read.csv(input_path, header=True, inferSchema=True)

# Filter out null or invalid rows for required columns
data_filtered = data.filter((col("region").isNotNull()) & 
                            (col("smoker").isNotNull()) & 
                            (col("charges").isNotNull()) & 
                            (col("sex").isNotNull()) & 
                            (col("bmi").isNotNull()) &
                            (col("age").isNotNull()))

def process_and_generate_manifest(output_folder, dataset_name):
    """
    Define paths for output data and corresponding manifest file.
    """
    output_path = f"{output_path_base}{output_folder}/"
    manifest_path = f"{manifest_base_path}{dataset_name}_manifest.json"
    return output_path, manifest_path

# 1. Average, max, and min charges by region
region_stats_output, region_stats_manifest = process_and_generate_manifest("region_stats", "region_stats")
data_filtered.groupBy("region").agg(
    avg("charges").alias("average_charge"),
    max("charges").alias("max_charge"),
    min("charges").alias("min_charge")
).write.csv(region_stats_output, mode="overwrite", header=True)
generate_and_upload_manifest(region_stats_output, region_stats_manifest)

# 2. Standard deviation of charges by BMI category
bmi_stddev_output, bmi_stddev_manifest = process_and_generate_manifest("bmi_stddev_analysis", "bmi_stddev")
data_filtered.withColumn(
    "bmi_category", 
    when(col("bmi") < 18.5, "Underweight")
    .when((col("bmi") >= 18.5) & (col("bmi") < 25), "Normal weight")
    .when((col("bmi") >= 25) & (col("bmi") < 30), "Overweight")
    .otherwise("Obese")
).groupBy("bmi_category").agg(
    stddev("charges").alias("stddev_charge")
).write.csv(bmi_stddev_output, mode="overwrite", header=True)
generate_and_upload_manifest(bmi_stddev_output, bmi_stddev_manifest)

# 3. Average charges by age range
age_range_output, age_range_manifest = process_and_generate_manifest("age_range_analysis", "age_range")
data_filtered.withColumn(
    "age_range",
    when(col("age") < 20, "Below 20")
    .when((col("age") >= 20) & (col("age") < 30), "20-29")
    .when((col("age") >= 30) & (col("age") < 40), "30-39")
    .when((col("age") >= 40) & (col("age") < 50), "40-49")
    .otherwise("50 and above")
).groupBy("age_range").agg(
    avg("charges").alias("average_charge")
).write.csv(age_range_output, mode="overwrite", header=True)
generate_and_upload_manifest(age_range_output, age_range_manifest)

# 4. Count of smokers and non-smokers by region
smoker_region_output, smoker_region_manifest = process_and_generate_manifest("smoker_region_analysis", "smoker_region")
data_filtered.groupBy("region", "smoker").agg(
    count("*").alias("count")
).write.csv(smoker_region_output, mode="overwrite", header=True)
generate_and_upload_manifest(smoker_region_output, smoker_region_manifest)

# 5. Charges comparison between males and females, grouped by smoker status
gender_smoker_output, gender_smoker_manifest = process_and_generate_manifest("gender_smoker_analysis", "gender_smoker")
data_filtered.groupBy("sex", "smoker").agg(
    avg("charges").alias("average_charge")
).write.csv(gender_smoker_output, mode="overwrite", header=True)
generate_and_upload_manifest(gender_smoker_output, gender_smoker_manifest)

# 6. Charges distribution by region
charges_distribution_output, charges_distribution_manifest = process_and_generate_manifest("charges_distribution", "charges_distribution")
data_filtered.groupBy("region").agg(
    count("charges").alias("charges_count")
).write.csv(charges_distribution_output, mode="overwrite", header=True)
generate_and_upload_manifest(charges_distribution_output, charges_distribution_manifest)

# 7. Gender distribution by region
gender_distribution_output, gender_distribution_manifest = process_and_generate_manifest("gender_distribution", "gender_distribution")
data_filtered.groupBy("region", "sex").agg(
    count("*").alias("count")
).write.csv(gender_distribution_output, mode="overwrite", header=True)
generate_and_upload_manifest(gender_distribution_output, gender_distribution_manifest)

# 8. Charges by smoker status
smoker_charges_output, smoker_charges_manifest = process_and_generate_manifest("smoker_charges", "smoker_charges")
data_filtered.groupBy("smoker").agg(
    avg("charges").alias("average_charge"),
    max("charges").alias("max_charge"),
    min("charges").alias("min_charge")
).write.csv(smoker_charges_output, mode="overwrite", header=True)
generate_and_upload_manifest(smoker_charges_output, smoker_charges_manifest)

# 9. BMI distribution by gender
bmi_gender_output, bmi_gender_manifest = process_and_generate_manifest("bmi_gender", "bmi_gender")
data_filtered.groupBy("sex").agg(
    avg("bmi").alias("average_bmi"),
    max("bmi").alias("max_bmi"),
    min("bmi").alias("min_bmi")
).write.csv(bmi_gender_output, mode="overwrite", header=True)
generate_and_upload_manifest(bmi_gender_output, bmi_gender_manifest)

# 10. Charges by age and gender
age_gender_output, age_gender_manifest = process_and_generate_manifest("age_gender_charges", "age_gender_charges")
data_filtered.groupBy("age_range", "sex").agg(
    avg("charges").alias("average_charge")
).write.csv(age_gender_output, mode="overwrite", header=True)
generate_and_upload_manifest(age_gender_output, age_gender_manifest)

# Stop the Spark session
spark.stop()
