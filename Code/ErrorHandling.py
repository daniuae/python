# Import SparkSession to create a Spark application
from pyspark.sql import SparkSession
# Import specific exceptions for detailed error handling with Spark DataFrames
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
# Import the system module to allow for exiting with error codes
import sys


def create_spark_session():
    """
    Creates and configures a SparkSession with custom settings for error resilience.
    """
    try:
        # Initialize SparkSession with explicit task failure and blacklisting configs
        spark = SparkSession.builder \
            .appName("PySpark Error Handling Demo") \
            .config("spark.task.maxFailures", "3") \
            .config("spark.blacklist.enabled", "true") \
            .getOrCreate()
        return spark  # Return the Spark session on success
    except Exception as e:
        # Catch any issue during SparkSession creation, print error, and exit
        print(f"Failed to create Spark session: {e}")
        sys.exit(1)


def read_csv_handle_exceptions(spark, file_path):
    """
    Reads a CSV file with robust error handling for missing files and stopped Spark context.
    """
    try:
        # Try reading the specified CSV file using Spark DataFrame API
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        return df
    except Py4JJavaError as e:
        # Handle case where SparkSession has been stopped unexpectedly
        if "Cannot call methods on a stopped SparkContext" in str(e):
            raise Exception("Spark session has been stopped. Please restart it.") from None
        else:
            # Raise other Py4JJavaError unmodified to be handled at a higher level
            raise
    except AnalysisException as e:
        # Handle missing file or directory by checking error message prefix
        if str(e).startswith("'Path does not exist:"):
            raise FileNotFoundError(f"File path does not exist: {file_path}") from None
        else:
            raise


def safe_distinct_count(df, column_name):
    """
    Returns the count of distinct values in a column if it exists, else logs a warning and returns zero.
    """
    if column_name in df.columns:
        # If column is present, compute count of unique values
        return df.select(column_name).distinct().count()
    else:
        # Log warning if column does not exist and return 0
        print(f"Warning: Column '{column_name}' not found. Returning count=0.")
        return 0


def run_sql_query(spark, query):
    """
    Executes a SQL query with PySpark, handling exceptions for non-existent tables and invalid queries.
    """
    try:
        # Try running the SQL query
        return spark.sql(query)
    except PySparkException as e:
        # Print descriptive SQL error and return None if the query fails
        print(f"SQL error: {e.desc}")
        return None


def main():
    # Create a Spark session using robust method
    spark = create_spark_session()

    # Realistic file path on HDFS or distributed storage (replace with actual valid path in your environment)
    file_path = "hdfs:///user/data/customers.csv"

    try:
        # Attempt to read CSV file with error handling for missing file/context
        df = read_csv_handle_exceptions(spark, file_path)
    except FileNotFoundError as fnf_err:
        # Print error and exit if file not found
        print(f"File error: {fnf_err}")
        spark.stop()
        sys.exit(2)
    except Exception as err:
        # Catch-all for any other error, print and exit
        print(f"Error reading CSV: {err}")
        spark.stop()
        sys.exit(3)

    # Print DataFrame schema to validate structure
    df.printSchema()
    # Show first 5 records for a quick data preview
    df.show(5)

    # Check and count unique customer IDs, or warn if column missing
    unique_customer_count = safe_distinct_count(df, "customer_id")
    print(f"Unique customers count: {unique_customer_count}")

    # Set up checkpointing directory for fault tolerance (required for some operations)
    spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")
    # Materialize checkpoint and show record count as a confirmation
    df_checkpointed = df.checkpoint(eager=True)
    print(f"Checkpointed DataFrame count: {df_checkpointed.count()}")

    # Register DataFrame as a temporary SQL view for querying
    df.createOrReplaceTempView("customers")
    query = "SELECT * FROM customers WHERE age > 30"
    result = run_sql_query(spark, query)
    # Show results if the query succeeded
    if result:
        result.show(5)

    # Simulate and handle a SQL error with a broken query (non-existent table)
    bad_query = "SELECT * FROM nonexistent_table"
    _ = run_sql_query(spark, bad_query)  # Will print SQL error and return None

    # Gracefully stop the Spark session to free resources
    spark.stop()


if __name__ == "__main__":
    # Entry point for script when run as standalone Python program
    main()
