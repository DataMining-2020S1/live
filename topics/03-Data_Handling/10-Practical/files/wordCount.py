# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# Find out where the pyspark runs
import findspark
findspark.init()

# %%
# Creating Spark Context
from pyspark import SparkContext
sc = SparkContext("local", "first app")

# %%
# Calculating word count
text_file = sc.textFile("data/exampleText.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
output = counts.collect()

# %%
# Print the word counts
for (word, count) in output:
    print("%s: %i" % (word, count))

# %%
# Release the spark context
sc.stop()

# %%
