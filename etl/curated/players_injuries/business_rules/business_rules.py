from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("injuries"))

        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("player_id"),
            F.col("ROWS.days").alias("days"),
            F.col("ROWS.gamesMissed").alias("gamesmissed"),
            F.col("ROWS.gamesMissedClubs").alias("gamesmissedclubs"),
            F.col("ROWS.injury").alias("injury"),
            F.col("ROWS.injuryFrom").alias("injury_from"),
            F.col("ROWS.season").alias("season"),
            F.col("ROWS.until").alias("until"),
        )
