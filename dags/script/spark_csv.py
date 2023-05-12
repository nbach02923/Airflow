import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from airflow.models import Variable
spark = (
    SparkSession.builder.master("local")
    .config(
        "spark.jars", "/opt/spark/assembly/target/scala-2.12/jars/postgresql-42.2.5.jar"
    )
    .getOrCreate()
)
filename_csv = "bat_dong_san_" + datetime.today().strftime("%d_%m_%Y") + ".csv"
schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("date", StringType(), True),
        StructField("price", LongType(), True),
        StructField("address", StringType(), True),
        StructField("owner", StringType(), True),
    ]
)
folder = "/opt/airflow/dags/data/real_estate"
files = [
    os.path.join(folder, file) for file in os.listdir(folder) if file.endswith(".csv")
]
processed_file = Variable.get("Scanned", default_var=[])
for file in files:
    if file not in processed_file:
        df = (
            spark.read.option("header", "true")
            .option("multiLine", "true")
            .schema(schema)
            .csv(file)
        )
        df = df.dropDuplicates(["id"])
        df.write.mode("ignore").format("jdbc").option(
            "url", "jdbc:postgresql://postgres:5432/postgres"
        ).option("driver", "org.postgresql.Driver").option(
            "dbtable", "real_estate"
        ).option(
            "user", "airflow"
        ).option(
            "password", "airflow"
        ).save()
        processed_file.append(file)
        Variable.set("Scanned", processed_file)
