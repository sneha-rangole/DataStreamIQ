from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import udf,regexp_replace
from config.config import configuration
from udf_utils import *

# Function to define all user-defined functions (UDFs) for data processing.
def define_udfs():
    # UDFs are functions we define to apply custom transformations on data. 
    # Here, each UDF transforms specific fields, such as extracting details from raw data, using custom logic.
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
         # Extracts salary range from text into start and end fields within a structured format
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField('salary_start', DoubleType(),True),
            StructField('salary_end', DoubleType(),True),
        ])),
        # Other UDFs are defined similarly for various data fields (e.g., start/end dates, requirements)
        # This helps maintain code modularity, reusability, and easier testing.
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_classcode_udf': udf(extract_classcode, StringType()),
        'extract_requirments_udf': udf(extract_requirments, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_req_udf': udf(extract_req, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_job_type_udf': udf(extract_job_type, StringType()),
        'extract_education_length_udf': udf(extract_education_length, StringType()),
        'extract_school_type_udf': udf(extract_school_type, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType()),
    }

# The following block of code ensures that the script is only executed if it is run directly (not imported as a module).
if __name__ == "__main__":

    # Creating a SparkSession. The SparkSession is the entry point to use Spark functionality.
    # It's necessary to configure this session to interact with AWS S3 for storage.
    spark = (
        # Build a SparkSession using the builder pattern
        SparkSession.builder
            # Assign a name to the Spark application (this will appear in Spark's UI)
            .appName('DataStreamIQ')
            
            # Configure Spark to include the necessary AWS dependencies (Hadoop AWS & AWS Java SDK)
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.1,'  # Hadoop AWS package for S3 integration
                    'com.amazonaws:aws-java-sdk:1.11.469')  # AWS SDK for Java for S3 access

            # Configure Spark to use the S3A protocol for accessing Amazon S3
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')  # S3A file system implementation
            
            # Provide the AWS access key and secret key, which are used for authenticating with AWS S3.
            # These values are retrieved from a configuration (e.g., environment variables or a config file).
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))  # Fetch AWS Access Key
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))  # Fetch AWS Secret Key

            # Specify the AWS credentials provider, which is responsible for supplying the access keys to Spark
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')  # Using simple credentials provider for AWS

            # Get the configured SparkSession or create one if it doesn't already exist
            .getOrCreate()  # SparkSession is initialized here
    )

    # Define paths to different input directories (for text, JSON, CSV, PDF, video, and image data)
    text_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_text'
    json_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_json'
    csv_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_csv'
    pdf_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_pdf'
    video_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_video'
    img_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_img'
   
    # Define a schema for the input data, specifying data types and structure of each column.
    # This schema is necessary for structured processing in Spark DataFrames.
    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('classcode', StringType(), True),
        StructField('salary_start', DoubleType(), True),
        StructField('salary_end', DoubleType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('education_length', StringType(), True),
        StructField('school_type', StringType(), True),
        StructField('application_location', StringType(), True),
    ])

    # Call to define_udfs() to initialize all custom UDFs for data processing tasks.
    udfs = define_udfs()

    # reading data from input directory   
    # Define streaming DataFrame reading from the text input directory
    job_bulletine_df = (spark.readStream
                    .format('text')
                    .option('wholetext', 'true')
                    .load(text_input_dir)
                )
    #Each step uses Spark DataFrame transformations and custom UDFs to parse, clean, and format the unstructured text data into a structured format,
    # making it more suitable for analysis and reporting.
    job_bulletins_df = job_bulletine_df.withColumn('file_name', regexp_replace(udfs['extract_file_name_udf']('value'),'\r',' ')) 
    job_bulletins_df = job_bulletins_df.withColumn('value', regexp_replace('value', r'\n', ' '))
    job_bulletins_df = job_bulletins_df.withColumn('position', regexp_replace(udfs['extract_position_udf']('value'), r'\r',' '))
    job_bulletins_df = job_bulletins_df.withColumn('salary_start', udfs['extract_salary_udf']('value').getField('salary_start'))
    job_bulletins_df = job_bulletins_df.withColumn('salary_end', udfs['extract_salary_udf']('value').getField('salary_end'))
    job_bulletins_df = job_bulletins_df.withColumn('start_date', udfs['extract_start_date_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('end_date', udfs['extract_end_date_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('classcode', udfs['extract_classcode_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('req', udfs['extract_requirments_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('notes', udfs['extract_notes_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('duties', udfs['extract_duties_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('selection', udfs['extract_selection_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('education_length', udfs['extract_education_length_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('school_type', udfs['extract_school_type_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('experience_length', udfs['extract_experience_length_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('job_type', udfs['extract_job_type_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('application_location', udfs['extract_application_location_udf']('value'))
    
    j_df = job_bulletins_df.select(
    'file_name', 'salary_start', 'salary_end', 'start_date', 'end_date', 
    'req', 'classcode', 'notes', 'duties', 'selection', 'education_length', 
    'school_type', 'experience_length', 'job_type', 'application_location'
    )

    # json_df = json_df.select('file_name','position','salary','start_date','end_date','req','notes','duties','selection','education_length','school_type','experience_length','job_type','application_location')

    # Write the streaming DataFrame to the console for display
    query = (j_df
             .writeStream
             .outputMode("append")  # Use 'append' to show new data as it arrives
             .format("console")       # Output the results to the console
             .option('truncate', False)
             .start()                # Start the streaming query
        )
    
    # Wait for the termination of the stream to keep it active and allow viewing
    query.awaitTermination()
