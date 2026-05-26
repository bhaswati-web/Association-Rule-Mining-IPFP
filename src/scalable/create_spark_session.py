
def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("Scalable IPFP Algorithm")
        .master("local[*]")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    return spark

