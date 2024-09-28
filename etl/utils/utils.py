from dataclasses import dataclass
from itertools import chain
from typing import Dict

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


@dataclass
class DefaultUtils:
    conf = SparkConf()
    conf.setAll(
        [
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"),
            (
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
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

    def fix_date(self, df: DataFrame, col_name: str) -> DataFrame:
        MONTHS: Dict[str, int] = {
            "Jan": "01",
            "Feb": "02",
            "Mar": "03",
            "Apr": "04",
            "May": "05",
            "Jun": "06",
            "Jul": "07",
            "Aug": "08",
            "Sep": "09",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12",
        }

        mapping_expr = F.create_map([F.lit(x) for x in chain(*MONTHS.items())])

        df_date = (
            df.withColumn(f"aux_{col_name}", F.split(col_name, " "))
            .withColumn(
                col_name,
                F.to_date(
                    F.concat(
                        F.col(f"aux_{col_name}")[2],
                        mapping_expr[F.col(f"aux_{col_name}")[0]],
                        F.lpad(
                            F.regexp_replace(F.col(f"aux_{col_name}")[1], ",", ""),
                            2,
                            "0",
                        ),
                    ),
                    "yyyyMMdd",
                ),
            )
            .drop(f"aux_{col_name}")
        )

        return df_date
