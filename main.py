from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from config.config import configuration

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
   
    # Define the schema for the data that will be processed. The schema defines the structure of the data.
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
