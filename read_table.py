from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":

    spark2 = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo2") \
        .enableHiveSupport() \
        .getOrCreate()

    spark2.catalog.setCurrentDatabase("AIRLINE_DB2")

    # Spark SQL to select data from the table
    selectedData = spark2.sql("SELECT * FROM flight_data_tbl")
    selectedData.show()

