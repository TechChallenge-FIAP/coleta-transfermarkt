from dataclasses import dataclass

import pyspark.sql.functions as F

from etl.models.run_model.utils.utils import Utils


@dataclass
class RunModel(Utils):
    def run(self):

        df_variables = self.read_parquet(
            "tech-challenge-3-models/feature-store/variables/"
        )

        df_variables = df_variables.filter(F.col("target") == F.lit("0")).drop(
            "target",
            "days_unknown_injury",
            "days_corona_virus",
            "gamesmissed_unknown_injury",
            "gamesmissed_corona_virus",
        )

        df_processing = self.preprocessing(
            df_variables,
            [col for col in df_variables.columns if col != "player_id"],
            "train/pkl/scaler.pkl",
        )

        df_predict = self.predict(df_processing, "train/pkl/random_forest.pkl")

        df_predict.select(
            "player_id", "predict", "predict_prob", "predict_class"
        ).show()
