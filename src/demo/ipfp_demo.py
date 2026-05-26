import time
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    array_distinct,
    col
)
from pyspark.ml.fpm import FPGrowth

# CONFIGURATION

MIN_SUPPORT = 0.25 
MIN_CONFIDENCE = 0.20

INPUT_FILE = "dataset/sample_dataset_1.txt"

NUM_PARTITIONS = 3

# CREATE SPARK SESSION

spark = SparkSession.builder \
    .appName("IPFP Demonstration Algorithm") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

sc = spark.sparkContext

# START TIMER

start_total = time.time()

# PHASE 1:
# LOAD AND PREPROCESS DATASET

print("\n================================================")
print("PHASE 1 : DATA LOADING AND PREPROCESSING")
print("================================================")

data = (
    spark.read.text(INPUT_FILE)
    .withColumn(
        "items",
        array_distinct(
            split(col("value"), " ")
        )
    )
    .select("items")
)

# Remove null transactions
data = data.filter(col("items").isNotNull())

# TOTAL TRANSACTIONS


total_transactions = data.count()

print(f"\nTotal Transactions : {total_transactions}")

# PHASE 2:
# PARTITIONING DATASET

print("PHASE 2 : DATA PARTITIONING")

partitioned_data = data.repartition(NUM_PARTITIONS)

print(f"\nTotal Partitions : {NUM_PARTITIONS}")

# DISPLAY PARTITION SIZES

partition_sizes = (
    partitioned_data.rdd
    .mapPartitions(lambda x: [sum(1 for _ in x)])
    .collect()
)

for i, size in enumerate(partition_sizes):

    print(f"Partition {i+1} : {size} transactions")

# PHASE 3:
# LOCAL FREQUENT PATTERN MINING

print("\n================================================")
print("PHASE 3 : LOCAL FREQUENT PATTERN MINING")
print("================================================")

start_local = time.time()

# ----------------------------------------------------------
# STORE ALL LOCAL PATTERNS
# NO LOCAL PRUNING
# ----------------------------------------------------------

local_pattern_supports = defaultdict(int)

for partition_id in range(NUM_PARTITIONS):

    print(f"\nLOCAL MINING - PARTITION {partition_id + 1}")
    print("--------------------------------------------")

    # ------------------------------------------------------
    # EXTRACT SINGLE PARTITION
    # ------------------------------------------------------

    partition_df = partitioned_data.rdd.mapPartitionsWithIndex(
        lambda idx, itr: itr if idx == partition_id else []
    ).toDF()

    # Skip empty partitions
    if partition_df.count() == 0:

        print("Empty Partition")
        continue

    # LOCAL FP-GROWTH
    # VERY LOW SUPPORT
    # RETAIN ALL LOCAL CANDIDATES

    fp_growth = FPGrowth(
        itemsCol="items",
        minSupport=0.0001,
        minConfidence=MIN_CONFIDENCE
    )

    local_model = fp_growth.fit(partition_df)

    local_patterns = local_model.freqItemsets

    # DISPLAY LOCAL PATTERNS

    sample_local_patterns = (
        local_patterns
        .limit(20)
        .collect()
    )

    for row in sample_local_patterns:

        print(f"{row['items']} -> {row['freq']}")

    # STORE LOCAL SUPPORTS

    for row in local_patterns.toLocalIterator():

        pattern = tuple(sorted(row["items"]))

        support = row["freq"]

        # NO LOCAL PRUNING
        # RETAIN ALL LOCAL CANDIDATES

        local_pattern_supports[pattern] += support

end_local = time.time()

# ==========================================================
# PHASE 4:
# GLOBAL AGGREGATION
# ==========================================================

print("\n================================================")
print("PHASE 4 : GLOBAL AGGREGATION")
print("================================================")

start_global = time.time()

print("\nAggregating local supports globally...")

# ----------------------------------------------------------
# GLOBAL SUPPORT THRESHOLD
# ----------------------------------------------------------

minimum_support_count = (
    MIN_SUPPORT * total_transactions
)

print(f"\nGlobal Minimum Support Count : "
      f"{minimum_support_count}")

# ----------------------------------------------------------
# GLOBAL PRUNING
# APPLIED ONLY AFTER AGGREGATION
# ----------------------------------------------------------

global_frequent_patterns = {
    pattern: support
    for pattern, support in local_pattern_supports.items()
    if support >= minimum_support_count
}

end_global = time.time()

# DISPLAY GLOBAL FREQUENT PATTERNS

print("GLOBAL FREQUENT PATTERNS")

for pattern, support in list(
    global_frequent_patterns.items()
)[:30]:

    print(f"{pattern} -> {support}")

print(f"\nTotal Frequent Patterns : "
      f"{len(global_frequent_patterns)}")

# PHASE 5:
# ASSOCIATION RULE GENERATION

print("PHASE 5 : ASSOCIATION RULE GENERATION")

start_rules = time.time()

association_rules = []

for pattern, support_xy in global_frequent_patterns.items():

    if len(pattern) < 2:
        continue

    for i in range(len(pattern)):

        antecedent = (pattern[i],)

        consequent = tuple(
            item for item in pattern
            if item not in antecedent
        )

        if antecedent in global_frequent_patterns:

            support_x = global_frequent_patterns[
                antecedent
            ]

            confidence = support_xy / support_x

            if confidence >= MIN_CONFIDENCE:

                association_rules.append((
                    antecedent,
                    consequent,
                    support_xy,
                    confidence
                ))

# DISPLAY ASSOCIATION RULES

for rule in association_rules[:30]:

    antecedent, consequent, support, confidence = rule

    print(
        f"{antecedent} => {consequent} | "
        f"Support: {support} | "
        f"Confidence: {confidence:.2f}"
    )

end_rules = time.time()

# FINAL SUMMARY

print("FINAL SUMMARY")

print(f"Total Frequent Patterns : "
      f"{len(global_frequent_patterns)}")

print(f"Total Association Rules : "
      f"{len(association_rules)}")

# EXECUTION TIMES

print("EXECUTION TIMES")

print(
    f"Local Mining Time : "
    f"{end_local - start_local:.2f} sec"
)

print(
    f"Global Aggregation Time : "
    f"{end_global - start_global:.2f} sec"
)

print(
    f"Association Rule Time : "
    f"{end_rules - start_rules:.2f} sec"
)

print(
    f"Total Execution Time : "
    f"{time.time() - start_total:.2f} sec"
)

# STOP SPARK SESSION

spark.stop()
