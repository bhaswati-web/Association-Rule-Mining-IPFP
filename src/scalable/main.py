# ==========================================================
# MAIN FUNCTION
# ==========================================================

def main():

    # ------------------------------------------------------
    # CREATE SPARK SESSION
    # ------------------------------------------------------

    spark = create_spark_session()

    try:

        # --------------------------------------------------
        # START TOTAL TIMER
        # --------------------------------------------------

        start_total = time.time()

        # --------------------------------------------------
        # LOAD DATASET
        # --------------------------------------------------

        data, total_transactions = (
            load_dataset(spark)
        )

        # --------------------------------------------------
        # PARTITION DATASET
        # --------------------------------------------------

        partitioned_data = (
            partition_dataset(data)
        )

        # --------------------------------------------------
        # LOCAL FP-GROWTH MINING
        # --------------------------------------------------

        (
            all_local_patterns,
            start_local,
            end_local

        ) = local_fp_growth(
            partitioned_data
        )

        # --------------------------------------------------
        # GLOBAL AGGREGATION
        # --------------------------------------------------

        (
            global_frequent_patterns,
            total_patterns,
            start_global,
            end_global

        ) = global_aggregation(
            all_local_patterns,
            total_transactions
        )

        # --------------------------------------------------
        # ASSOCIATION RULE GENERATION
        # --------------------------------------------------

        (
            total_rules,
            start_rules,
            end_rules

        ) = generate_association_rules(
            partitioned_data,
            total_transactions
        )

        # --------------------------------------------------
        # DISPLAY EXECUTION SUMMARY
        # --------------------------------------------------

        display_execution_summary(
            total_patterns,
            total_rules,
            start_local,
            end_local,
            start_global,
            end_global,
            start_rules,
            end_rules,
            start_total
        )

    finally:

        # --------------------------------------------------
        # STOP SPARK SESSION
        # --------------------------------------------------

        spark.stop()


# ==========================================================
# DRIVER
# ==========================================================

if __name__ == "__main__":

    main()

