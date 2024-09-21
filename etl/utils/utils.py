from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession


@dataclass
class DefaultUtils:
    spark = SparkSession.builder.getOrCreate()

    def read_raw(self, path: str) -> DataFrame:
        df = self.spark.read.json(path)
        return df

    def write_csv(self, df: DataFrame, path: str) -> None:
        df.write.csv(path, sep=";", header=True, mode="overwrite")
