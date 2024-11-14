# Import necessary libraries from PySpark
from pyspark.sql import SparkSession, DataFrame  # For creating a Spark session
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType, LongType, BinaryType  # For defining schemas
from pyspark.sql.functions import udf, regexp_replace, lit  # For creating user-defined functions and manipulating data
from config.config import configuration  # To get configuration settings like AWS keys
from udf_utils import *  # Import custom UDFs (User  Defined Functions)
import fitz  # PyMuPDF for handling PDF files

# Function to extract text from a PDF file
def extract_pdf_text(binary_data: bytes) -> str:
    # Open the PDF file from binary data using fitz
    doc = fitz.open(stream=binary_data, filetype="pdf")  # Open the PDF
    text = ""  # Initialize an empty string to hold the extracted text
    for page in doc:  # Loop through each page in the PDF
        text += page.get_text()  # Extract text from each page and add it to the text variable
    return text  # Return the extracted text

# Function to define all user-defined functions (UDFs) for data processing.
def define_udfs():
    # Return a dictionary of UDFs that can be used for data extraction
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),  # UDF to extract file name
        'extract_position_udf': udf(extract_position, StringType()),  # UDF to extract job position
        'extract_salary_udf': udf(extract_salary, StructType([  # UDF to extract salary as a structure with start and end
            StructField('salary_start', DoubleType(), True),
            StructField('salary_end', DoubleType(), True),
        ])),
        'extract_start_date_udf': udf(extract_start_date, DateType()),  # UDF to extract start date
        'extract_end_date_udf': udf(extract_end_date, DateType()),  # UDF to extract end date
        'extract_classcode_udf': udf(extract_classcode, StringType()),  # UDF to extract class code
        'extract_requirments_udf': udf(extract_requirments, StringType()),  # UDF to extract requirements
        'extract_notes_udf': udf(extract_notes, StringType()),  # UDF to extract notes
        'extract_duties_udf': udf(extract_duties, StringType()),  # UDF to extract duties
        'extract_req_udf': udf(extract_req, StringType()),  # UDF to extract additional requirements
        'extract_selection_udf': udf(extract_selection, StringType()),  # UDF to extract selection criteria
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),  # UDF to extract experience length
        'extract_job_type_udf': udf(extract_job_type, StringType()),  # UDF to extract job type
        'extract_education_length_udf': udf(extract_education_length, StringType()),  # UDF to extract education length
        'extract_school_type_udf': udf(extract_school_type, StringType()),  # UDF to extract school type
        'extract_application_location_udf': udf(extract_application_location, StringType()),  # UDF to extract application location
        'extract_pdf_text_udf': udf(extract_pdf_text, StringType()),  # UDF to extract text from PDF
    }

# This block of code ensures that the script runs only if it is executed directly, not imported as a module.
if __name__ == "__main__":

    # Create a Spark session with specific configurations for AWS
    spark = (
        SparkSession.builder
            .appName('DataStreamIQ')  # Name of the Spark application
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.1,'  # Include necessary AWS libraries
                    'com.amazonaws:aws-java-sdk:1.11.469')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')  # Set S3A as the file system
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))  # Get AWS access key from configuration
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))  # Get AWS secret key from configuration
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider ')  # Set the credentials provider for AWS
            .getOrCreate()  # Create the Spark session
    )

    # Define input directories for different types of data
    json_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_json'  # Directory for JSON files
    text_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_text'  # Directory for text files
    pdf_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_pdf'  # Directory for PDF files
    img_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_img'  # Directory for image files

    # Define the schema for incoming data
    data_schema = StructType([
        StructField('file_name', StringType(), True),  # File name as a string
        StructField('position', StringType(), True),  # Job position as a string
        StructField('classcode', StringType(), True),  # Class code as a string
        StructField('salary_start', DoubleType(), True),  # Starting salary as a double
        StructField('salary_end', DoubleType(), True),  # Ending salary as a double
        StructField('start_date', DateType(), True),  # Start date as a date
        StructField('end_date', DateType(), True),  # End date as a date
        StructField('req', StringType(), True),  # Requirements as a string
        StructField('notes', StringType(), True),  # Notes as a string
        StructField('duties', StringType(), True),  # Duties as a string
        StructField('selection', StringType(), True),  # Selection criteria as a string
        StructField('experience_length', StringType(), True),  # Experience length as a string
        StructField('job_type', StringType(), True),  # Job type as a string
        StructField('education_length', StringType(), True),  # Education length as a string
        StructField('school_type', StringType(), True),  # School type as a string
        StructField('application_location', StringType(), True),  # Application location as a string
    ])

    # Define user-defined functions (UDFs)
    udfs = define_udfs()  # Call the function to get UDFs

    # Read JSON data from the input directory with multiline option
    json_df = spark.readStream \
        .option("multiline", "true") \
        .json(json_input_dir, schema=data_schema)  # Load JSON data with the defined schema

    # Read text data and process it with UDFs
    job_bulletine_df = (spark.readStream
                        .format('text')  # Specify the format as text
                        .option('wholetext', 'true')  # Read the whole text of each file
                        .load(text_input_dir)  # Load text data from the input directory
                    )
    
    # Transform `job_bulletine_df` to have the same columns as `json_df`
    job_bulletins_df = job_bulletine_df.withColumn('file_name', regexp_replace(udfs['extract_file_name_udf']('value'), r'\r', ' '))  # Extract file name
    job_bulletins_df = job_bulletins_df.withColumn('position', regexp_replace(udfs['extract_position_udf']('value'), r'\r', ' '))  # Extract position
    job_bulletins_df = job_bulletins_df.withColumn('salary_start', udfs['extract_salary_udf']('value').getField('salary_start'))  # Extract starting salary
    job_bulletins_df = job_bulletins_df.withColumn('salary_end', udfs['extract_salary_udf']('value').getField('salary_end'))  # Extract ending salary
    job_bulletins_df = job_bulletins_df.withColumn('start_date', udfs['extract_start_date_udf']('value'))  # Extract start date
    job_bulletins_df = job_bulletins_df.withColumn('end_date', udfs['extract_end_date_udf']('value'))  # Extract end date
    job_bulletins_df = job_bulletins_df.withColumn('classcode', udfs['extract_classcode_udf']('value'))  # Extract class code
    job_bulletins_df = job_bulletins_df.withColumn('req', udfs['extract_requirments_udf']('value'))  # Extract requirements
    job_bulletins_df = job_bulletins_df.withColumn('notes', udfs['extract_notes_udf']('value'))  # Extract notes
    job_bulletins_df = job_bulletins_df.withColumn('duties', udfs['extract_duties_udf']('value'))  # Extract duties
    job_bulletins_df = job_bulletins_df.withColumn('selection', udfs['extract_selection_udf']('value'))  # Extract selection criteria
    job_bulletins_df = job_bulletins_df.withColumn('experience_length', udfs['extract_experience_length_udf']('value'))  # Extract experience length
    job_bulletins_df = job_bulletins_df.withColumn('job_type', udfs['extract_job_type_udf']('value'))  # Extract job type
    job_bulletins_df = job_bulletins_df.withColumn('education_length', udfs['extract_education_length_udf']('value'))  # Extract education length
    job_bulletins_df = job_bulletins_df.withColumn('school_type', udfs['extract_school_type_udf']('value'))  # Extract school type
    job_bulletins_df = job_bulletins_df.withColumn('application_location', udfs['extract_application_location_udf']('value'))  # Extract application location

    # Read PDF files as binary files
    pdf_df = (spark.readStream
        .format('binaryFile')  # Specify the format as binary file
        .schema(StructType([  # Define the schema for binary content
            StructField('path', StringType(), False),  # Path of the file as a string (not nullable)
            StructField('modificationTime', TimestampType(), False),  # Modification time as a timestamp (not nullable)
            StructField('length', LongType(), False),  # Length of the file as a long integer (not nullable)
            StructField('content', BinaryType(), True)  # Content of the file as binary data (nullable)
        ]))
        .option('pathGlobFilter', '*.pdf')  # Filter to only include PDF files
        .load(pdf_input_dir)  # Load PDF data from the input directory
    )

    # Apply the UDF to extract text from the binary content of the PDF files
    pdf_bulletins_df = pdf_df.withColumn('file_text', udfs['extract_pdf_text_udf']('content'))  # Extract text from PDF content

    # Apply other transformations to the PDF data
    pdf_bulletins_df = pdf_bulletins_df.withColumn('file_name', pdf_df['path'])  # Get the file name from the path
    pdf_bulletins_df = pdf_bulletins_df.withColumn('position', regexp_replace(udfs['extract_position_udf']('file_text'), r'\r', ' '))  # Extract position from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('salary_start', udfs['extract_salary_udf']('file_text').getField('salary_start'))  # Extract starting salary from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('salary_end', udfs['extract_salary_udf']('file_text').getField('salary_end'))  # Extract ending salary from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('start_date', udfs['extract_start_date_udf']('file_text'))  # Extract start date from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('end_date', udfs['extract_end_date_udf']('file_text'))  # Extract end date from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('classcode', udfs['extract_classcode_udf']('file_text'))  # Extract class code from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('req', udfs['extract_requirments_udf']('file_text'))  # Extract requirements from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('notes', udfs['extract_notes_udf']('file_text'))  # Extract notes from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('duties', udfs['extract_duties_udf']('file_text'))  # Extract duties from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('selection', udfs['extract_selection_udf']('file_text'))  # Extract selection criteria from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('experience_length', udfs['extract_experience_length_udf']('file_text'))  # Extract experience length from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('job_type', udfs['extract_job_type_udf']('file_text'))  # Extract job type from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('education_length', udfs['extract_education_length_udf']('file_text'))  # Extract education length from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('school_type', udfs['extract_school_type_udf']('file_text'))  # Extract school type from PDF text
    pdf_bulletins_df = pdf_bulletins_df.withColumn('application_location', lit(None))  # Set application location to None for PDFs

    # Select the same columns as in the JSON DataFrame for consistency
    job_bulletins_df = job_bulletins_df.select(
        'file_name', 'salary_start', 'salary_end', 'start_date', 'end_date', 
        'req', 'classcode', 'notes', 'duties', 'selection', 'experience_length', 
        'job_type', 'education_length', 'school_type', 'application_location'
    )

    json_df = json_df.select(
        'file_name', 'salary_start', 'salary_end', 'start_date', 'end_date', 
        'req', 'classcode', 'notes', 'duties', 'selection', 'experience_length', 
        'job_type', 'education_length', 'school_type', 'application_location'
    )

    pdf_bulletins_df = pdf_bulletins_df.select(
        'file_name', 'salary_start', 'salary_end', 'start_date', 'end_date', 
        'req', 'classcode', 'notes', 'duties', 'selection', 'experience_length', 
        'job_type', 'education_length', 'school_type', 'application_location'
    )

    # Combine (union) the DataFrames from JSON, text, and PDF sources into one DataFrame
    union_dataframe = job_bulletins_df.union(json_df).union(pdf_bulletins_df)
    
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream 
            .format('parquet')
            .option('checkpointLocation', checkpointFolder)
            .option('path', output)
            .outputMode('append')
            .trigger(processingTime="5 seconds")
            .start()
            )

 
    # Write the resulting DataFrame to the console for monitoring
    # query = (union_dataframe   # Use the combined DataFrame
    #          .writeStream  # Start writing the stream
    #          .outputMode("append")  # Append new rows to the output
    #          .format("console")  # Output format is console
    #          .option('truncate', False)  # Do not truncate long output
    #          .start()  # Start the streaming query
    #     )
    
    query = streamWriter(union_dataframe, 's3a://datastreamiqbucket/checkpoints',
                        's3a://datastreamiqbucket/data/spark_unstructured' )

    # Wait for the streaming query to finish
    query.awaitTermination()  # Keep the application running until terminated

    spark.stop()