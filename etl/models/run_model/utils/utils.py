from dataclasses import dataclass
from typing import List

import joblib
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, lit, pandas_udf
from pyspark.sql.types import ArrayType, FloatType

from etl.utils.utils import DefaultUtils


@dataclass
class Utils(DefaultUtils):
    def read_pickle(self, path):
        return joblib.load(path)

    def preprocessing(self, df: DataFrame, columns: List[str], path: str) -> DataFrame:

        scaler = self.read_pickle(path)

        @pandas_udf(ArrayType(FloatType()))
        def udf_preprocessing(samples: pd.Series) -> pd.Series:
            return pd.Series(
                [scaler.transform([sample]).reshape(1, -1)[0] for sample in samples]
            )

        df = df.withColumn("preprocessing_name", lit(f"{scaler.__class__.__name__}"))

        for attribute, value in scaler.__dict__.items():
            df = df.withColumn(f"preprocessing_{attribute}", lit(f"{value}"))

        return df.withColumn("features_preproccessing", array(*columns)).withColumn(
            "features_preproccessing", udf_preprocessing(col("features_preproccessing"))
        )

    def predict(self, df: DataFrame, path: str) -> DataFrame:

        model = self.read_pickle(path)

        @pandas_udf(FloatType())
        def udf_predict(samples: pd.Series) -> pd.Series:
            return pd.Series([model.predict(sample)[0][0] for sample in samples])

        df = df.withColumn("prediction_name", lit(f"{model.__class__.__name__}"))

        for attribute, value in model.__dict__.items():
            df = df.withColumn(f"prediction_{attribute}", lit(value))

        return df.withColumn("predict", udf_predict(col("features_preproccessing")))
