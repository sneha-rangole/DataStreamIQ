# Importing the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession
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

    # You can now use the `spark` object to interact with Spark and perform operations, 
    # such as reading data from or writing data to S3, creating DataFrames, etc.
