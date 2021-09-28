# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.10.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from pyspark.ml.feature import SQLTransformer
from pyspark.sql import SparkSession

# %% [markdown]
# Having imported the relevant classes from pyspark, we are now ready to get or create a SparkSession named "SQLTransformerExample" that is available for us to run Spark jobs.

# %%
spark = SparkSession\
        .builder\
        .appName("SQLTransformerExample")\
        .getOrCreate()

# %% [markdown]
# Using the resources in that SparkSession, we create a _Spark_ dataframe that we populate with some numbers. Note that the fields in that dataframe are named `id`, `vi` and `v2`.

# %%
sparkDf = spark.createDataFrame([
        (0, 1.0, 3.0),
        (2, 2.0, 5.0)
    ], ["id", "v1", "v2"])

# %% [markdown]
# We inspect the resulting `sparkDf` dataframe, to make sure it looks like what we want.

# %%
sparkDf.show()

# %% [markdown]
# Now we define a simple Transformation specified in the SQL statement: adding 2 computed columns v3 and v4.

# %%
sqlTrans = SQLTransformer(
        statement="SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

# %% [markdown]
# Now we can apply this SQL transformation to `sparkDf`, which replaces the placeholder `__THIS__` in `sqlTrans` above.

# %%
sparkDf2 = sqlTrans.transform(sparkDf)
sparkDf2.show()

# %% [markdown]
# We can convert the Spark dataframe (which lives in HDFS) to an in-memory pandas dataframe as follows

# %%
pandasDf = sparkDf2.select("*").toPandas()
pandasDf

# %% [markdown]
# Now we can modify that pandas dataframe, using typical pandas operations, adding a new column to the dataframe. We can view the output just to check that the `v5` column has been added.

# %%
import pandas as pd
pandasDf['v5'] = pandasDf['v4'] - pandasDf['v3']
pandasDf

# %% [markdown]
# We can convert pandasDf to a Spark dataframe, writing it to HDFS in the process...

# %%
sparkDf3 = spark.createDataFrame(pandasDf)
sparkDf3.show()

# %% [markdown]
#
# We now stop that SparkContext, releasing its resources back into the pool.

# %%
spark.stop()

# %%
