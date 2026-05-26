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

INPUT_FILE = "kosarak.txt"

NUM_PARTITIONS = 3


# CREATE SPARK SESSION

spark = SparkSession.builder \
    .appName("Scalable IPFP Algorithm") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

try:

    # START TOTAL TIMER

    start_total = time.time()

    # PHASE 1 : LOAD DATASET

    print("\n================================================")
    print("PHASE 1 : DATA LOADING")
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

    total_transactions = data.count()

    print(f"\nTotal Transactions : "
          f"{total_transactions}")

    # PHASE 2 : PARTITIONING

    print("PHASE 2 : DATA PARTITIONING")

    partitioned_data = (
        data.repartition(NUM_PARTITIONS)
        .cache()
    )

    print(f"\nTotal Partitions : "
          f"{NUM_PARTITIONS}")

    partition_sizes = (
        partitioned_data.rdd
        .mapPartitions(
            lambda x: [sum(1 for _ in x)]
        )
        .collect()
    )

    for i, size in enumerate(partition_sizes):

        print(f"Partition {i+1} : "
              f"{size} transactions")

    # PHASE 3 : LOCAL FP-GROWTH

    print("PHASE 3 : LOCAL FP-GROWTH MINING")

    start_local = time.time()

    all_local_patterns = []

    # Reduced local support to minimize
    # aggressive local pruning

    local_support = (
        MIN_SUPPORT / NUM_PARTITIONS
    )

    for partition_id in range(NUM_PARTITIONS):

        print(f"\nProcessing Partition "
              f"{partition_id + 1}")

        partition_df = (
            partitioned_data.rdd
            .mapPartitionsWithIndex(
                lambda idx, itr:
                itr if idx == partition_id else []
            )
            .toDF()
        )

        if partition_df.count() == 0:

            print("Empty Partition")
            continue

        # LOCAL FP-GROWTH

        fp_growth = FPGrowth(
            itemsCol="items",
            minSupport=local_support,
            minConfidence=MIN_CONFIDENCE
        )

        local_model = fp_growth.fit(partition_df)

        local_patterns = (
            local_model.freqItemsets
            .withColumnRenamed(
                "freq",
                "local_support"
            )
        )

        print("\nSample Local Patterns:")

        local_patterns.show(
            10,
            truncate=False
        )

        all_local_patterns.append(
            local_patterns
        )

    end_local = time.time()

    # SAFETY CHECK

    if not all_local_patterns:

        print("\nNo local patterns generated.")
        exit()

    
    # PHASE 4 : GLOBAL AGGREGATION

    print("\n================================================")
    print("PHASE 4 : GLOBAL AGGREGATION")
    print("================================================")

    start_global = time.time()

    # UNION ALL LOCAL PATTERNS

    global_patterns = all_local_patterns[0]

    for pattern_df in all_local_patterns[1:]:

        global_patterns = (
            global_patterns.union(pattern_df)
        )

    # DISTRIBUTED GLOBAL AGGREGATION

    aggregated_patterns = (
        global_patterns
        .groupBy("items")
        .agg(
            spark_sum("local_support")
            .alias("global_support")
        )
    )

    # DELAYED GLOBAL PRUNING

    minimum_support_count = (
        MIN_SUPPORT * total_transactions
    )

    print(f"\nGlobal Minimum Support Count : "
          f"{minimum_support_count}")

    global_frequent_patterns = (
        aggregated_patterns
        .filter(
            col("global_support")
            >= minimum_support_count
        )
        .cache()
    )

    end_global = time.time()

    # DISPLAY GLOBAL FREQUENT PATTERNS

    print("GLOBAL FREQUENT PATTERNS")

    global_frequent_patterns.show(
        20,
        truncate=False
    )

    total_patterns = (
        global_frequent_patterns.count()
    )

    print(f"\nTotal Frequent Patterns : "
          f"{total_patterns}")

    # PHASE 5 : ASSOCIATION RULE GENERATION

    print("PHASE 5 : ASSOCIATION RULE GENERATION")

    start_rules = time.time()

    final_fp_growth = FPGrowth(
        itemsCol="items",
        minSupport=MIN_SUPPORT,
        minConfidence=MIN_CONFIDENCE
    )

    final_model = final_fp_growth.fit(
        partitioned_data
    )

    association_rules = (
        final_model.associationRules
        .cache()
    )

    # DISPLAY ASSOCIATION RULES

    sample_rules = (
        association_rules
        .limit(20)
        .collect()
    )

    print("\nAssociation Rules:\n")

    for row in sample_rules:

        antecedent = tuple(
            row["antecedent"]
        )

        consequent = tuple(
            row["consequent"]
        )

        confidence = row["confidence"]

        support = int(
            row["support"]
            * total_transactions
        )

        print(
            f"{antecedent} => "
            f"{consequent} | "
            f"Support: {support} | "
            f"Confidence: {confidence:.2f}"
        )

    total_rules = (
        association_rules.count()
    )

    print(f"\nTotal Association Rules : "
          f"{total_rules}")

    end_rules = time.time()

    # FINAL SUMMARY

    print("FINAL SUMMARY")

    print(f"Total Frequent Patterns : "
          f"{total_patterns}")

    print(f"Total Association Rules : "
          f"{total_rules}")

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

finally:

    # STOP SPARK SESSION

    spark.stop()
