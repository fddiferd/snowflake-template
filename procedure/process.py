from datetime import datetime
from typing import Any, List
import numpy as np
from sklearn.linear_model import LinearRegression
from snowflake.snowpark import Session, DataFrame, Table
from snowflake.snowpark.types import PandasSeriesType, IntegerType, FloatType, StringType
from snowflake.snowpark.functions import col, year, max, lit
import pandas as pd

OUTPUTS: List[Any] = []

def run(session: Session) -> str:
    df: DataFrame = session.table("dbt_production.staging.sq_hawk_dashboards")
    filtered_df: pd.DataFrame = clean_df(df.to_pandas())
    model: LinearRegression = gross_adds_model(filtered_df)

    generate_new_table_with_predicted(filtered_df, model, session)
    register_udf(model, session)

    return str(OUTPUTS)

def clean_df(input_df: pd.DataFrame) -> pd.DataFrame:
   return input_df[['METRIC_TIME__DAY', 'GROSS_ADDS']].dropna(subset=['GROSS_ADDS'])

def gross_adds_model(input_pd: pd.DataFrame) -> LinearRegression:
    # 1. Convert METRIC_TIME__DAY to a numeric value (ordinal) for the model
    x: np.ndarray = (
        pd.to_datetime(input_pd["METRIC_TIME__DAY"])       # Ensure values are datetime
          .apply(lambda d: d.toordinal())                  # Convert to an int (days since 1/1/0001)
          .to_numpy()
          .reshape(-1, 1)
    )
    
    y: np.ndarray = input_pd["GROSS_ADDS"].to_numpy()

    # 2. Train a simple linear regression model
    model: LinearRegression = LinearRegression().fit(x, y)

    # 3. Predict for a specific date (e.g. '2025-11-01')
    predict_day_str = "2025-11-01"
    # Convert the string date to ordinal
    predict_day_ordinal: int = datetime.strptime(predict_day_str, "%Y-%m-%d").toordinal()
    
    pred: np.ndarray = model.predict(np.array([[predict_day_ordinal]]))
    print(f"Prediction for {predict_day_str}: {round(pred[0], 2)}")

    # 4. Store or print the results
    OUTPUTS.append(input_pd.tail())
    OUTPUTS.append(
        f"Prediction for {predict_day_str}: {round(pred[0], 2)}"
    )

    return model

def register_udf(model, session) -> None:

    def predict_gross_adds(date_str: pd.Series) -> pd.Series:
        return date_str.transform(lambda x: model.predict([[datetime.strptime(x, '%Y-%m-%d').toordinal()]])[0].round(2).astype(float))
    session.udf.register(
        predict_gross_adds,
        return_type=PandasSeriesType(FloatType()),
        input_types=[PandasSeriesType(StringType())],
        packages= ["pandas","scikit-learn"],
        is_permanent=True, 
        name="predict_gross_adds", 
        replace=True,
        stage_location="@deploy"
    )
    print('UDF registered')
    OUTPUTS.append('UDF registered')

def generate_new_table_with_predicted(input_df: pd.DataFrame, model: LinearRegression, session: Session) -> None:
    # Create a single date dataframe for 2025-11-01
    predict_date = pd.DataFrame({'METRIC_TIME__DAY': ['2025-11-01']})
    
    # Convert the date to ordinal for prediction
    predict_x: np.ndarray = (
        pd.to_datetime(predict_date["METRIC_TIME__DAY"])
        .apply(lambda d: d.toordinal())
        .to_numpy()
        .reshape(-1, 1)
    )

    # Get prediction
    prediction: np.ndarray = model.predict(predict_x)
    
    # Create output dataframe with date and prediction
    output_df = pd.DataFrame({
        'METRIC_TIME__DAY': predict_date['METRIC_TIME__DAY'],
        'PREDICTED_GROSS_ADDS': prediction.round(2)
    })

    snp_df: DataFrame = session.create_dataframe(output_df)
    snp_df.write.save_as_table("GROSS_ADDS_TEST", mode="overwrite")


if __name__ == "__main__":
    from utils import get_session
    session: Session = get_session.session()
    run(session)
