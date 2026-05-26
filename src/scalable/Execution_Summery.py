# DISPLAY EXECUTION SUMMARY

def display_execution_summary(
    total_patterns,
    total_rules,
    start_local,
    end_local,
    start_global,
    end_global,
    start_rules,
    end_rules,
    start_total
):

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
