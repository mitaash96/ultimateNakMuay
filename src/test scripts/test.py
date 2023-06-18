from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pyspark as ps

conf = ps.SparkConf()

spark = SparkSession.builder.master("local[*]").getOrCreate()

data = [
    (2023, "marketA", 1, 100),
    (2022, "marketA", 1, 10),
    (2021, "marketA", 1, 1),
    ]

df = spark.createDataFrame(data, ["year", "market", "month", "size"])

df.show()