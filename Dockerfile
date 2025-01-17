# Use the official PySpark base image
FROM jupyter/pyspark-notebook:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to install dependencies
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the PySpark script and necessary files to the container
COPY s3_bucket_files/PySpark_script/source_code.py /app/source_code.py

# Specify the entry point for the container
CMD ["spark-submit", "/app/source_code.py"]

