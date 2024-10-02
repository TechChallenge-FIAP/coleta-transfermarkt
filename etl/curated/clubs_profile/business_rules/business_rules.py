from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("club_id"),
            F.col("name").alias("club_name"),
            F.col("image").alias("url_image"),
            F.col("league.countryId").alias("country_id"),
            F.col("league.countryName").alias("country_name"),
            F.col("league.tier").alias("club_tier"),
        )
