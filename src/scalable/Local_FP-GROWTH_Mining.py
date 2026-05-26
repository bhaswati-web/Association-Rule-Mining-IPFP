
# LOCAL FP-GROWTH MINING

def local_fp_growth(partitioned_data):

    print("PHASE 3 : LOCAL FP-GROWTH MINING")

    start_local = time.time()

    all_local_patterns = []

    
    # REDUCED LOCAL SUPPORT
    # MINIMIZES AGGRESSIVE LOCAL PRUNING

    local_support = (
        MIN_SUPPORT / NUM_PARTITIONS
    )

    for partition_id in range(NUM_PARTITIONS):

        print(f"\nProcessing Partition "
              f"{partition_id + 1}")

        # --------------------------------------------------
        # EXTRACT SINGLE PARTITION
        # --------------------------------------------------

        partition_df = (
            partitioned_data.rdd
            .mapPartitionsWithIndex(
                lambda idx, itr:
                itr if idx == partition_id else []
            )
            .toDF()
        )

        # --------------------------------------------------
        # SKIP EMPTY PARTITIONS
        # --------------------------------------------------

        if partition_df.count() == 0:

            print("Empty Partition")
            continue

        # LOCAL FP-GROWTH

        fp_growth = FPGrowth(
            itemsCol="items",
            minSupport=local_support,
            minConfidence=MIN_CONFIDENCE
        )

        local_model = fp_growth.fit(
            partition_df
        )

        local_patterns = (
            local_model.freqItemsets
            .withColumnRenamed(
                "freq",
                "local_support"
            )
        )

        # DISPLAY SAMPLE LOCAL PATTERNS

        print("\nSample Local Patterns:")

        local_patterns.show(
            10,
            truncate=False
        )

        # STORE LOCAL PATTERNS

        all_local_patterns.append(
            local_patterns
        )

    end_local = time.time()

    return (
        all_local_patterns,
        start_local,
        end_local
    )

