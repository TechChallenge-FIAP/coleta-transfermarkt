from dataclasses import dataclass

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
    def explode_out_rows(self, df: DataFrame) -> DataFrame:
        df_exploded = df.withColumn("ROWS", F.explode_outer("stats"))

        return df_exploded

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("player_id"),
            F.col("ROWS.appearances").cast(T.LongType()).alias("appearances"),
            F.col("ROWS.assists").cast(T.LongType()).alias("assists"),
            F.col("ROWS.clubID").alias("club_id"),
            F.col("ROWS.competitionID").alias("competition_id"),
            F.col("ROWS.competitionName").alias("competition_name"),
            F.col("ROWS.goals").cast(T.LongType()).alias("goals"),
            F.regexp_replace("ROWS.minutesPlayed", "[^0-9]", "")
            .cast(T.LongType())
            .alias("minutesplayed"),
            F.col("ROWS.seasonID").alias("season_id"),
            F.col("ROWS.yellowCards").cast(T.LongType()).alias("yellowcards"),
        )
