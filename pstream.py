from pyspark.sql.types import *
import pyspark.sql.functions as F

connectionString = "Endpoint=sb://............"

ehConf = {}
ehConf[
    "eventhubs.connectionString"
] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"

json_schema = StructType(
    [
        StructField("idDrink", StringType(), True),
        StructField("strDrink", StringType(), True),
        StructField("dateModified", StringType(), True),
    ]
)
df = spark.readStream.format("eventhubs").options(**ehConf).load()

json_df = df.withColumn("body", F.from_json(df.body.cast("string"), json_schema))

df3 = json_df.select(
    F.col("body.idDrink"), F.col("body.strDrink"), F.col("body.dateModified")
)

# display(df3)
df3.writeStream.format("json").outputMode("append").option(
    "checkpointLocation", "/mnt/..../..../checkpointapievents"
).start("/mnt/..../...")
