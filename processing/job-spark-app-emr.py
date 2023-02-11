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

# cria a função para escrever os dados processados em delta na camada silver
def write_silver(bucket, path, dataframe, col_partition, data_format, mode):
    print("\nEscrevendo os dados lidos da bronze para delta na silver zone...")
    try:
        dataframe.write.format(data_format) \
            .partitionBy(col_partition) \
            .mode(mode) \
            .save(f"{bucket}/{path}")
        print(f"Dados escritos na silver com sucesso!")
        return 0
    except Exception as err:
        print(f"Falha para escrever os dados na silver: {err}")
        return 1

# cria a função para escrever os dados convertidos em delta da camada silver na camada gold
def write_gold(bucket, path, dataframe, data_format, mode):
    print ("\nEscrevendo os dados na gold zone...")
    try:
        dataframe.write.format(data_format)\
                .mode(mode)\
                .save(f"{bucket}/{path}")
        print (f"Dados escritos na gold com sucesso!")
        return 0
    except Exception as err:
        print (f"Falha para escrever dados na gold: {err}")
        return 1

# função para criar analíticos a partir dos dados da silver zone e persistir na camada gold
def analytics_table(bucket, dataframe, table_name):
    # cria uma view para trabalhar com sql
    dataframe.createOrReplaceTempView(table_name)
    # processa os dados conforme regra de negócio
    df_query1 = dataframe.groupBy("name") \
                .agg(sum("circulating_supply").alias("circulating_supply")) \
                .sort(desc("circulating_supply")) \
                .limit(10)
    df_query2 = dataframe.select(col("name"), col("symbol"), col("price")) \
                .sort(desc("price")) \
                .limit(10)
    # imprime o resultado do dataframe criado
    print ("\n Top 10 Cryptomoedas com maior fornecimento de circulação  no mercado\n")
    print (df_query1.show())
    print ("\n Top 10 Cryptomoedas com preços mais altos de 2022\n")
    print (df_query2.show())
    # escreve na camada gold os analíticos criados
    write_gold(f"{bucket}", "coins_circulating_supply", df_query1, "delta", "overwrite")
    write_gold(f"{bucket}", "top10_prices_2022", df_query1, "delta", "overwrite")
    

# Ler dados da bronze
df = read_csv("s3://bronze-stack-bootcampde", "public/tb_coins/")

# Cria uma coluna de ano para particionar os dados
df = df.withColumn("year", year(df.date_added))

# Processa os dados e escreve na camada silver
write_silver("s3://silver-stack-bootcampde", "tb_coins", df, "year", "delta", "overwrite")

# Ler os dados da silver e escreve na camada gold
df = read_delta("s3://silver-stack-bootcampde", "tb_coins")

analytics_table("s3://gold-stack-bootcampde", df, "tb_coins")

# Finaliza a aplicação
spark.stop()



