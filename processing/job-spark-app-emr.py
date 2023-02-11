from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# setup da aplicação Spark
spark = SparkSession \
    .builder \
    .appName("job-1-spark") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# definindo o método de logging da aplicação. use INFO somente em DEV [INFO, ERROR]
spark.sparkContext.setLogLevel("ERROR")

