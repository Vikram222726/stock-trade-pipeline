import os
from pyspark.sql import SparkSession


class KafkaToBronzePipeline():
    def __init__(self, spark):
        self.base_data_dir = "/FileStore/stocks_data"
        self.BOOTSTRAP_SERVER = os.getenv("KAFKA_HOST_URL")
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = os.getenv("KAFKA_API_KEY")
        self.CLUSTER_API_SECRET = os.getenv("KAFKA_API_SECRET")
        self.spark = spark

    def getSchema(self):
        return "Key string, Value string"

    def ingestFromKafka(self, startingTime=1):
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config",
                        f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
                .option("subscribe", "invoices")
                .option("maxOffsetsPerTrigger", 10)
                .option("startingTimestamp", startingTime)
                .load()
                )

    def getStockMessages(self, kafka_df):
        from pyspark.sql.functions import cast, from_json
        return (kafka_df.select(kafka_df.key.cast("string").alias("key"),
                                from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"),
                                "topic", "timestamp")
                )

    def process(self, startingTime=1):
        print(f"Starting Bronze Stream...", end='')
        rawDF = self.ingestFromKafka(startingTime)
        stockDF = self.getStockMessages(rawDF)
        stockExecQuery = (stockDF.writeStream
                  .queryName("stock-bronze-ingestion")
                  .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/stock_bz")
                  .outputMode("append")
                  .toTable("stock_bz")
                  )
        print("Done")
        return stockExecQuery
