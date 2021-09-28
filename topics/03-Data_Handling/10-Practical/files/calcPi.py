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

#!/usr/bin/env python
# %%
import findspark
findspark.init()

# %%
import pyspark
import random

# %%
sc = pyspark.SparkContext(appName="Pi")
num_samples = 100000000

# %%
# Use Monte Carlo (sampling) technique to return 1 if a random point in the first quadrant is inside the unit circle.
def inside(p):     
  x, y = random.random(), random.random()
  return x*x + y*y < 1

# %%
# Looping from 0 to num_samples, count the number of times where a random point in the first quadrant is inside the unit circle
count = sc.parallelize(range(0, num_samples)).filter(inside).count()

# %%
# Now Compute the ratio and multiply by 4 to cover all 4 quadrants
pi = 4 * count / num_samples
print(pi)

# %%
sc.stop()

# %%
