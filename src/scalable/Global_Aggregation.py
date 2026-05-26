
# GLOBAL AGGREGATION

def global_aggregation(
    all_local_patterns,
    total_transactions
):

    print("PHASE 4 : GLOBAL AGGREGATION")

    start_global = time.time()

    # SAFETY CHECK

    if not all_local_patterns:

        print("\nNo local patterns generated.")
        return None, 0, start_global, start_global

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

    return (
        global_frequent_patterns,
        total_patterns,
        start_global,
        end_global
    )
