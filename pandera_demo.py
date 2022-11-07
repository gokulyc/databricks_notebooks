# Databricks notebook source
import pyspark.pandas as ps
import pandas as pd
import pandera as pa

# COMMAND ----------

from pandera.typing.pyspark import DataFrame, Series

# COMMAND ----------

class Schema(pa.SchemaModel):
    state: Series[str]
    city: Series[str]
    price: Series[int] = pa.Field(in_range={"min_value": 5, "max_value": 20})

# COMMAND ----------

# create a pyspark.pandas dataframe that's validated on object initialization
df = DataFrame[Schema](
    {
        'state': ['FL','FL','FL','CA','CA','CA'],
        'city': [
            'Orlando',
            'Miami',
            'Tampa',
            'San Francisco',
            'Los Angeles',
            'San Diego',
        ],
        'price': [8, 12, 10, 16, 20, 18],
    }
)
print(df)

# COMMAND ----------

@pa.check_types
def function(df: DataFrame[Schema]) -> DataFrame[Schema]:
    return df[df["state"] == "CA"]

# COMMAND ----------

print(function(df))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Fague

# COMMAND ----------

import pandas as pd

data = pd.DataFrame(
    {
        'state': ['FL','FL','FL','CA','CA','CA'],
        'city': [
            'Orlando', 'Miami', 'Tampa', 'San Francisco', 'Los Angeles', 'San Diego'
        ],
        'price': [8, 12, 10, 16, 20, 18],
    }
)
print(data)

# COMMAND ----------

from pandera import Column, DataFrameSchema, Check

price_check = DataFrameSchema(
    {"price": Column(int, Check.in_range(min_value=5,max_value=20))}
)

def price_validation(data:pd.DataFrame) -> pd.DataFrame:
    return price_check.validate(data)

# COMMAND ----------

from fugue import transform
from fugue_spark import SparkExecutionEngine

spark_df = transform(data, price_validation, schema="*", engine=SparkExecutionEngine)
spark_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Fague spark ML

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

X = pd.DataFrame({"x_1": [1, 1, 2, 2], "x_2":[1, 2, 2, 3]})
y = np.dot(X, np.array([1, 2])) + 3
reg = LinearRegression().fit(X, y)

# COMMAND ----------

X,y

# COMMAND ----------

def predict(df: pd.DataFrame, model: LinearRegression) -> pd.DataFrame:
    return df.assign(predicted=model.predict(df))

input_df = pd.DataFrame({"x_1": [3, 4, 6, 6], "x_2":[3, 3, 6, 6]})


# COMMAND ----------

# test the function
predict(input_df.copy(), reg)

# COMMAND ----------

from fugue import transform

result = transform(
    input_df,
    predict,
    schema="*,predicted:double",
    params=dict(model=reg),
    engine=spark
)
print(type(result))
result.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Alternative Fague

# COMMAND ----------

input_df, type(input_df)

# COMMAND ----------

from typing import Iterator, Any, Union
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import DataFrame, SparkSession

spark_session = SparkSession.builder.getOrCreate()

def predict_wrapper(dfs: Iterator[pd.DataFrame], model):
    for df in dfs:
        yield predict(df, model)

def run_predict(input_df: Union[DataFrame, pd.DataFrame], model):
    # conversion
    if isinstance(input_df, pd.DataFrame):
        sdf = spark_session.createDataFrame(input_df.copy())
    else:
        sdf = input_df.copy()

    schema = StructType(list(sdf.schema.fields))
    schema.add(StructField("predicted", DoubleType()))
    return sdf.mapInPandas(lambda dfs: predict_wrapper(dfs, model), 
                           schema=schema)

result = run_predict(input_df.copy(), reg)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Sample Problem and transform()
# MAGIC 
# MAGIC https://fugue-tutorials.readthedocs.io/tutorials/beginner/type_flexibility.html  

# COMMAND ----------

import pandas as pd

_area_code_map = {"217": "Champaign, IL", "407": "Orlando, FL", "510": "Fremont, CA"}

data = pd.DataFrame({"phone": ["(217)-123-4567", "(217)-234-5678", "(407)-123-4567", 
                               "(407)-234-5678", "(510)-123-4567"]})

# COMMAND ----------

def map_phone_to_location(df: pd.DataFrame) -> pd.DataFrame:
    df["location"] = df["phone"].str.slice(1,4).map(_area_code_map)
    return df

map_phone_to_location(data.copy())

# COMMAND ----------

#using fague

from fugue import transform

t_df = transform(data.copy(),
          map_phone_to_location,
          schema="*, location:str")

# COMMAND ----------

type(t_df)

# COMMAND ----------

t_df

# COMMAND ----------

t_df = transform(data.copy(),
          map_phone_to_location,
          schema="*, location:str", engine=spark)


# COMMAND ----------

t_df.show()

# COMMAND ----------

from typing import List, Dict, Any, Iterable

def map_phone_to_location2(df: List[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
    for row in df:
        row["location"] = _area_code_map[row["phone"][1:4]]
        yield row

def map_phone_to_location3(df: List[List[Any]]) -> List[List[Any]]:
    for row in df:
        row.append(_area_code_map[row[0][1:4]])
    return df

def map_phone_to_location4(df: List[List[Any]]) -> pd.DataFrame:
    for row in df:
        row.append(_area_code_map[row[0][1:4]])
    df = pd.DataFrame.from_records(df, columns=["phone", "location"])
    return df

# COMMAND ----------

t2_sdf = transform(data.copy(),
          map_phone_to_location2,
          schema="*, location:str", engine=spark)
t2_sdf.show()

# COMMAND ----------

t3_sdf = transform(data.copy(),
          map_phone_to_location3,
          schema="*, location:str", engine=spark)
t3_sdf.show()

# COMMAND ----------

t4_sdf = transform(data.copy(),
          map_phone_to_location4,
          schema="*, location:str", engine=spark)
t4_sdf.show()

# COMMAND ----------


