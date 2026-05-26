# Association-Rule-Mining-IPFP
Implementation of the Improved Parallel FP-Growth (IPFP) algorithm for distributed association rule mining using Apache Spark.

# **IPFP: An Improved Parallel FP-Growth Method for Fast Association Rule Mining**

## **Overview**

Traditional FP-Growth-based association rule mining algorithms often face scalability limitations, communication overhead, and incomplete pattern discovery in distributed environments. To address these challenges, this repository provides the implementation of the proposed Improved Parallel FP-Growth (IPFP) algorithm using Apache Spark.

The proposed IPFP workflow eliminates aggressive local pruning, avoids Conditional Pattern Base (CPB) generation during local mining, and performs global aggregation of locally mined frequent patterns for improved scalability and completeness.

The proposed IPFP approach improves distributed frequent pattern mining efficiency by reducing aggressive local pruning and enabling scalable global aggregation of locally mined frequent itemsets using Apache Spark.

The implementation was experimentally evaluated in both:

* Single-machine multi-core environments  
* Distributed cloud-based environments using Google Cloud Dataproc

The repository supports reproducible experimentation for distributed association rule mining using Apache Spark.

---

## **Key Contributions**

* Elimination of aggressive local pruning during partition-wise mining  
* Partition-wise local FP-Growth mining using Apache Spark  
* Global aggregation of locally mined frequent itemsets  
* Delayed global pruning strategy after support recomputation  
* Improved scalability for distributed association rule mining  
* Support for both multi-core and distributed cloud environments

---

## **IPFP Workflow**

The proposed IPFP workflow consists of the following phases:

1. Dataset loading and preprocessing  
2. Partition-wise transaction distribution  
3. Local FP-Growth mining on each partition  
4. Retention of all local candidate patterns without aggressive local pruning  
5. Global aggregation of local frequent patterns  
6. Delayed global pruning using recomputed support thresholds  
7. Association rule generation

---

## **Repository Structure**

IPFP-Association-Rule-Mining/  
│  
├── datasets/  
│   ├── sample\_dataset\_1.txt  
│   ├── sample\_dataset\_2.txt  
│   └── kosarak.txt  
│  
├── src/  
│   ├── demo/  
│   │   └── ipfp\_demo.py  
│   │  
│   └── scalable/  
│       ├── create\_spark\_session.py  
│       ├── load\_dataset.py  
│       ├── partition\_dataset.py  
│       ├── local\_fp\_growth.py  
│       ├── global\_aggregation.py  
│       ├── generate\_association\_rules.py  
│       ├── display\_execution\_summary.py  
│       ├── main.py  
│       └── ipfp\_scalable.py  
│  
├── notebooks/  
│   └── ipfp\_demo.ipynb  
│  
├── results/  
│       ├── sample\_dataset\_1/  
│       │  
│       ├── sample\_dataset\_2/  
│       │  
│       └── kosarak/  
│  
├── docs/  
│   └── reproducibility.md  
│  
├── README.md  
├── requirements.txt  
└── LICENSE

---

## **Implementations**

The repository contains two implementations to separately demonstrate:

1. The conceptual IPFP workflow and pseudo-code alignment  
2. Scalable distributed execution for large benchmark datasets

### **1\. Demonstration Implementation**

Main implementation file: src/ipfp\_demo.py

This implementation is designed for:

* workflow demonstration,  
* pseudo-code alignment,  
* novelty visualization,  
* and reproducibility verification.

The demo implementation uses:

* `Sample_dataset_1.txt`  
* `Sample_dataset_2.txt`

### **Features**

* partition-wise local frequent pattern mining  
* explicit global aggregation  
* delayed global pruning  
* association rule generation  
* pseudo-code aligned workflow implementation

---

### **2\. Scalable Implementation**

Scalable implementation file:

src/ipfp\_scalable.py

This implementation is designed for:

* large-scale distributed execution,  
* scalable frequent pattern mining,  
* and cloud-based experimentation.

The scalable implementation was tested using the: kosarak.txt dataset to demonstrate scalability and distributed execution performance.

### **Features**

* distributed Spark-based aggregation  
* scalable local FP-Growth mining  
* adaptive local support threshold  
* reduced driver memory overhead  
* compatibility with large benchmark datasets  
* Google Dataproc execution support

---

## **Implementation Note**

The proposed IPFP workflow utilizes Apache Spark MLlib FP-Growth as the local mining engine while implementing customized partition-wise local candidate retention, global aggregation, and delayed global pruning strategy proposed in the paper.

---

## **Datasets**

The repository includes:

### **Sample Datasets**

Used for workflow demonstration and reproducibility:

* `Sample_dataset_1.txt`  
* `Sample_dataset_2.txt`

### **Large Benchmark Dataset**

Used for scalable distributed experimentation:

* `kosarak.txt`

Additional benchmark datasets discussed in the paper include:

* T10I4D100K  
* T40I10D100K  
* Accidents  
* Webdocs  
* Chicago Crimes  
* Instacart

Large datasets can be downloaded from their original public sources mentioned in the paper.

---

## **Experimental Environments**

### **Multi-Core Environment**

The local experimental environment used:

* Apple MacBook Air M2  
* 8-core CPU  
* 16 GB RAM  
* macOS  
* Apache Spark 3.3.2  
* Python 3.x  
* Java 11

---

### **Distributed Cloud Environment**

Distributed experiments were conducted using Google Cloud Dataproc with:

* Apache Spark 3.5  
* Hadoop 3.3  
* Debian 12  
* Google Cloud N2 machine instances  
* Multiple worker nodes  
* Distributed Spark cluster configuration

---

## **Requirements**

Install dependencies using:

pip install pyspark

---

## **How to Run**

### **Demonstration Implementation**

python src/ipfp\_demo.py

---

### **Scalable Implementation**

python src/ipfp\_scalable.py

# ---

# **Sample Output**

('11',) \=\> ('6',) | Support: 32456 | Confidence: 0.89  
('737',) \=\> ('11',) | Support: 12673 | Confidence: 0.71  
('2', '11') \=\> ('6',) | Support: 19822 | Confidence: 0.94

---

## **Output**

The implementations generate:

* local frequent patterns  
* globally aggregated frequent patterns  
* association rules  
* execution time summaries  
* partition-wise mining information

---

## **Results**

Sample execution outputs and screenshots are provided in:

results/

The scalable execution results demonstrated in this repository are based on the Kosarak dataset.

---

## **Reproducibility**

Detailed reproducibility instructions, execution environments, and experimental workflow descriptions are provided in:

docs/reproducibility.md

---

## **Important Note**

Large-scale datasets such as Kosarak and Webdocs may require:

* higher Spark memory allocation  
* distributed execution environments  
* optimized cluster configurations

For large-scale experiments, Google Cloud Dataproc is recommended.

---

## **License**

This project is licensed under the BSD 3-Clause License.

