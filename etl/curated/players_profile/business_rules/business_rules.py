from dataclasses import dataclass
from itertools import chain
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from etl.utils.utils import DefaultUtils


@dataclass
class BusinessRules(DefaultUtils):
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

    def final_schema(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("id").alias("player_id"),
            F.col("url").alias("url_source"),
            F.col("name").alias("player_name"),
            F.col("description").alias("player_resume"),
            F.col("fullName").alias("player_fullname"),
            F.col("dateOfBirth").alias("player_birthday"),
            F.col("placeOfBirth.city").alias("player_city_from"),
            F.col("placeOfBirth.country").alias("player_country_from"),
            F.col("age"),
            F.col("height"),
            F.col("citizenship"),
            F.col("isRetired").alias("is_retired"),
            F.col("position.main").alias("main_position"),
            F.col("position.other").alias("second_position"),
            F.col("foot"),
            F.col("shirtNumber").alias("shirt_number"),
            F.col("club.id").alias("actual_club_id"),
            F.col("club.name").alias("actual_club_name"),
            F.col("club.joined").alias("actual_club_joined"),
            F.col("club.contractExpires").alias("actual_club_contract_expires"),
            F.col("club.contractOption").alias("actual_club_contract_option"),
            F.col("marketValue").alias("market_value"),
            F.col("agent.name").alias("agent_name"),
            F.col("outfitter"),
            F.col("socialMedia").alias("url_social_media"),
        )
