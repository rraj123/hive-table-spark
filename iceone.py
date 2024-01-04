from pyspark.sql import SparkSession
from pyic iceberg.catalog import HiveCatalog
from pyic iceberg.table import Table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyic iceberg.data import SparkDataFrame

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IcebergTableDataInsertion") \
        .enableHiveSupport() \
        .getOrCreate()

    # Use a Hive catalog for Iceberg
    catalog = HiveCatalog(spark)

    # Define the schema for the "employee" table
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True)
    ])

    # Create the Iceberg table using the Hive catalog
    table = Table.create(catalog, "default.employee", schema)

    # Create a DataFrame with two records
    data = [(1, "John", 30, "HR"), (2, "Alice", 25, "Engineering")]
    df = spark.createDataFrame(data, schema=schema)

    # Append the DataFrame to the Iceberg table
    table.new_append().append_df(SparkDataFrame(df)).commit()

    # Close the SparkSession
    spark.stop()