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

# cria a função para ler dados em .csv
def read_csv(bucket, path):
    # lendo os dados do Data Lake
    df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .csv(f"{bucket}/{path}")

    # imprime os dados lidos da camada bronze
    print("\nImprime os dados lidos da bronze:")
    print(df.show(5))

    # imprime o schema do dataframe
    print("\nImprime o schema do dataframe lido da bronze:")
    print(df.printSchema())

    return df

# cria a função para ler dados em delta
def read_delta(bucket, path):
    df = spark.read.format("delta") \
        .load(f"{bucket}/{path}")
    
    return df