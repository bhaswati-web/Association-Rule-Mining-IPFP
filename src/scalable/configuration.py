
# IMPORT LIBRARIES

import time

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    split,
    array_distinct,
    col,
    sum as spark_sum
)

from pyspark.ml.fpm import FPGrowth


# CONFIGURATION

MIN_SUPPORT = 0.007

MIN_CONFIDENCE = 0.5

INPUT_FILE = "datasets/kosarak.txt"

NUM_PARTITIONS = 3
