from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import udf, regexp_replace
from config.config import configuration
from udf_utils import *

# Function to define all user-defined functions (UDFs) for data processing.
def define_udfs():
    return {
        'extract_file_name_udf': udf(extract_file_name, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_salary_udf': udf(extract_salary, StructType([
            StructField('salary_start', DoubleType(), True),
            StructField('salary_end', DoubleType(), True),
        ])),
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

    spark = (
        SparkSession.builder
            .appName('DataStreamIQ')
            .config('spark.jars.packages',
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk:1.11.469')
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY'))
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .getOrCreate()
    )

    # Input directories for various types of data
    json_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_json'
    text_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_text'
    pdf_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_pdf'
    img_input_dir = 'file:///Users/sneha_rangole/Desktop/GitCode/AWS_Big_Data_Project/input/input_img'
    # Define schema for incoming data
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

    # Define UDFs
    udfs = define_udfs()

    # Read JSON data with multiline option
    json_df = spark.readStream \
        .option("multiline", "true") \
        .json(json_input_dir, schema=data_schema)

    # Read text data and process it with UDFs
    job_bulletine_df = (spark.readStream
                        .format('text')
                        .option('wholetext', 'true')
                        .load(text_input_dir)
                    )

    # Transform `job_bulletine_df` to have the same columns as `json_df`
    job_bulletins_df = job_bulletine_df.withColumn('file_name', regexp_replace(udfs['extract_file_name_udf']('value'),r'\r',' '))
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
    job_bulletins_df = job_bulletins_df.withColumn('experience_length', udfs['extract_experience_length_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('job_type', udfs['extract_job_type_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('education_length', udfs['extract_education_length_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('school_type', udfs['extract_school_type_udf']('value'))
    job_bulletins_df = job_bulletins_df.withColumn('application_location', udfs['extract_application_location_udf']('value'))

    # Now select the same columns as `json_df`
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
    # Now you can union the DataFrames
    union_dataframe = job_bulletins_df.union(json_df)

    # Write the resulting DataFrame to the console
    query = (union_dataframe
             .writeStream
             .outputMode("append")
             .format("console")
             .option('truncate', False)
             .start()
        )
    
    # Wait for termination
    query.awaitTermination()
