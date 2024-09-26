from dataclasses import dataclass

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


@dataclass
class DefaultUtils:
    conf = SparkConf()
    conf.setAll(
        [
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0"),
            (
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider",
            ),
        ]
    )
    spark = SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()

    def read_json(self, path: str) -> DataFrame:
        df = self.spark.read.json(f"s3a://{path}")
        return df

    def read_parquet(self, path: str) -> DataFrame:
        df = self.spark.read.parquet(f"s3a://{path}")
        return df

    def write_csv(self, df: DataFrame, path: str) -> None:
        df.write.csv(path, sep=";", header=True, mode="overwrite")

    def write_parquet(self, df: DataFrame, path: str) -> None:
        df.write.parquet(f"s3a://{path}", mode="overwrite")
