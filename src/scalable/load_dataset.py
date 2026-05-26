
def load_dataset(spark):

    print("PHASE 1 : DATA LOADING")

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

    # REMOVE NULL TRANSACTIONS

    data = data.filter(
        col("items").isNotNull()
    )

    # TOTAL TRANSACTIONS

    total_transactions = data.count()

    print(f"\nTotal Transactions : "
          f"{total_transactions}")

    return data, total_transactions
