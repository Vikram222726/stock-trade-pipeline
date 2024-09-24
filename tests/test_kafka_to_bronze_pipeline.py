from pyspark.sql import SparkSession
from stock_stream_pipeline.kafka_to_bronze_pipeline import KafkaToBronzePipeline


class kafkaToBronzeTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/stocks_data"
        self.spark = SparkSession.builder.appName("Kafka to Bronze Pipeline").master("local[*]").config("spark.executor.memory", "2g").getOrCreate()

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        self.spark.sql("drop table if exists stock_bz")
        dbutils.fs.rm("/user/hive/warehouse/stock_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/stock_bz", True)
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = self.spark.sql("select count(*) from stock_bz").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")

    def runTests(self):
        self.cleanTests()
        bzStream = KafkaToBronzePipeline(spark=self.spark)

        print("Testing Scenario - Start from beginneing on a new checkpoint...")
        bzQuery = bzStream.process()
        self.waitForMicroBatch()
        bzQuery.stop()
        self.assertResult(20)
        print("Validation passed.\n")

        print("Testing Scenarion - Restart from where it stopped on the same checkpoint...")
        bzQuery = bzStream.process()
        self.waitForMicroBatch()
        bzQuery.stop()
        self.assertResult(20)
        print("Validation passed.\n")


if __name__ == "__main__":
    ts = kafkaToBronzeTestSuite()
    ts.runTests()
