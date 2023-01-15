from pyspark.sql.types import *
from pyspark.sql.functions import *

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "<application-id>",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(
        scope="<scope>", key="<service-credential-key>"
    ),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token",
}

dbutils.fs.mount(
    source="abfss://<container>@<datalake>.dfs.core.windows.net/",
    mount_point="/mnt/test",
    extra_configs=configs,
)

json_schema = StructType(
    [
        StructField("idDrink", StringType(), True),
        StructField("strDrink", StringType(), True),
        StructField("dateModified", StringType(), True),
    ]
)
df = spark.read.format("avro").load("/mnt/......../.../0/2023/**/**/**/**/*.avro")

# body_df = df.withColumn("Body", df.Body.cast("string")).select("Body")

json_df = df.withColumn("Body", from_json(df.Body.cast("string"), json_schema))

df3 = json_df.select(
    col("Body.idDrink"), col("Body.strDrink"), col("Body.dateModified")
)

display(df3)
