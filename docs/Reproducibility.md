# **Reproducibility Guide**

## **Overview**

This document provides reproducibility instructions for the implementation of the proposed Improved Parallel FP-Growth (IPFP) algorithm for distributed association rule mining using Apache Spark.

The repository contains two implementations:

1. `ipfp_demo.py`  
   Used for workflow demonstration and pseudo-code validation using small sample datasets.  
2. `ipfp_scalable.py`  
   Used for scalable distributed experimentation on large transactional datasets such as Kosarak.

---

# **Experimental Environments**

## **Multi-Core Environment**

The local experimental environment used for the demonstration implementation:

* Apple MacBook Air M2  
* 8-core CPU  
* 16 GB RAM  
* macOS  
* Apache Spark 3.3.2  
* Python 3.x  
* Java 11

---

## **Distributed Cloud Environment**

The scalable implementation was evaluated using Google Cloud Dataproc with:

* Apache Spark 3.5  
* Hadoop 3.3  
* Debian 12  
* Google Cloud N2 machine instances  
* Multiple worker nodes  
* Distributed Spark cluster configuration

---

# **Dataset Information**

## **Sample Datasets**

The following datasets are included in the repository for workflow demonstration:

* `Sample_dataset_1.txt`  
* `Sample_dataset_2.txt`

These datasets are used with:

src/ipfp\_demo.py

---

## **Large-Scale Dataset**

The scalable implementation was evaluated using:

* `kosarak.txt`

This dataset is used with:

src/ipfp\_scalable.py

Additional benchmark datasets discussed in the paper can be obtained from their original public sources.

---

# **Dependency Installation**

Install the required dependencies using:

pip install pyspark

---

# **Running the Demonstration Implementation**

Execute:

python src/ipfp\_demo.py

This implementation demonstrates:

* partition-wise local mining  
* global aggregation  
* delayed global pruning  
* association rule generation

---

# **Running the Scalable Implementation**

Execute:

python src/ipfp\_scalable.py

This implementation demonstrates:

* scalable distributed FP-Growth mining  
* distributed global aggregation  
* reduced driver memory overhead  
* scalable association rule generation

---

# **Spark Configuration**

The scalable implementation uses the following Spark configuration:

.config("spark.executor.memory", "4g")  
.config("spark.driver.memory", "4g")

Memory allocation may be increased depending on dataset size and cluster configuration.

---

# **Important Notes**

Large-scale datasets such as Kosarak and Webdocs may require:

* higher Spark memory allocation  
* distributed execution environments  
* optimized Spark cluster configuration

For large-scale experiments, Google Cloud Dataproc is recommended.

---

# **Expected Outputs**

The implementations generate:

* local frequent patterns  
* globally aggregated frequent patterns  
* association rules  
* execution time summaries  
* partition-wise mining information

Sample execution screenshots are provided in:

results/screenshots/

---

# **Reproducibility Objective**

This repository is intended to support reproducible experimentation and validation of the proposed IPFP algorithm for distributed association rule mining using Apache Spark.

