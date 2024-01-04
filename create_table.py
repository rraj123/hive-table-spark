from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB3")
    spark.catalog.setCurrentDatabase("AIRLINE_DB3")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")
    logger.info(spark.catalog.listTables("AIRLINE_DB3"))

    # Spark SQL to select data from the table
    selectedData = spark.sql("SELECT * FROM flight_data_tbl")
    selectedData.show()
    #
    # spark2 = SparkSession \
    #     .builder \
    #     .master("local[3]") \
    #     .appName("SparkSQLTableDemo2") \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    #
    # # Spark SQL to select data from the table
    # selectedData = spark2.sql("SELECT * FROM flight_data_tbl")
    # selectedData.show()