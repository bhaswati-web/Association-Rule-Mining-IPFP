# PARTITION DATASET

def partition_dataset(data):

    print("PHASE 2 : DATA PARTITIONING")

    # REPARTITION DATASET

    partitioned_data = (
        data.repartition(NUM_PARTITIONS)
        .cache()
    )

    print(f"\nTotal Partitions : "
          f"{NUM_PARTITIONS}")

    # DISPLAY PARTITION SIZES

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

    return partitioned_data
