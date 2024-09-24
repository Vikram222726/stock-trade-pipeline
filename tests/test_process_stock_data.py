from pyspark.sql import SparkSession
from stock_stream_pipeline.process_stock_data import StockTradeProcessing


class TradeSummaryTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/stocks_data"
        self.spark = SparkSession.builder.appName("Process Stock Trading Pipeline").master("local[*]").config("spark.executor.memory", "2g").getOrCreate()

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists stock_bz")
        spark.sql("drop table if exists trade_summary")
        dbutils.fs.rm("/user/hive/warehouse/stock_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/trade_summary", True)
        self.spark.sql(f"CREATE TABLE stock_bz(key STRING, value STRING)")

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/trade_summary", True)
        print("Done")

    def assertTradeSummary(self, start, end, expected_buy, expected_sell):
        print(f"\tStarting Trade Summary validation...", end='')
        result = (self.spark.sql(f"""select TotalBuy, TotalSell from trade_summary 
                                    where date_format(start, 'yyyy-MM-dd HH:mm:ss') = '{start}' 
                                    and date_format(end, 'yyyy-MM-dd HH:mm:ss')='{end}'""")
                            .collect()
                )
        actual_buy = result[0][0]
        actual_sell = result[0][1]
        assert expected_buy == actual_buy, f"Test failed! actual buy is {actual_buy}"
        assert expected_sell == actual_sell, f"Test failed! actual sell is {actual_sell}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")

    def runTests(self):
        self.cleanTests()

        stream = StockTradeProcessing()
        sQuery = stream.process()

        print("\nTesting first two events...")
        self.spark.sql("""INSERT INTO stock_bz VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:05:00", "Type": "BUY", "Amount": 500, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:12:00", "Type": "BUY", "Amount": 300, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()
        self.assertTradeSummary('2019-02-05 10:00:00', '2019-02-05 10:15:00', 800, 0)

        print("\nTesting third and fourth events...")
        self.spark.sql("""INSERT INTO stock_bz VALUES
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:20:00", "Type": "BUY", "Amount": 600, "BrokerCode": "ABX"}'),
                  ('2019-02-05', '{"CreatedTime": "2019-02-05 10:40:00", "Type": "BUY", "Amount": 900, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()
        self.assertTradeSummary('2019-02-05 10:15:00', '2019-02-05 10:30:00', 600, 0)
        self.assertTradeSummary('2019-02-05 10:30:00', '2019-02-05 10:45:00', 900, 0)

        print("\nTesting late event...")
        self.spark.sql("""INSERT INTO stock_bz VALUES
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:48:00", "Type": "SELL", "Amount": 500, "BrokerCode": "ABX"}'),
                    ('2019-02-05', '{"CreatedTime": "2019-02-05 10:25:00", "Type": "SELL", "Amount": 400, "BrokerCode": "ABX"}')
            """)
        self.waitForMicroBatch()
        self.assertTradeSummary('2019-02-05 10:45:00', '2019-02-05 11:00:00', 0, 500)
        self.assertTradeSummary('2019-02-05 10:15:00', '2019-02-05 10:30:00', 600, 400)

        print("Validation passed.\n")

        sQuery.stop()


if __name__ == "__main__":
    ts = TradeSummaryTestSuite()
    ts.runTests()
