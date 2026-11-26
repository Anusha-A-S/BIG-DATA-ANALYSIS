# BIG-DATA-ANALYSIS
COMPANY: CODTECH IT SOLUTIONS

NAME: ANUSHA ASHOK SANNINGAPPANAVAR

INTERN ID: CITS7383

DOMAIN: DATA ANALYTICS

DURATION: 8 WEEKS

MENTOR: NEELA SANTOSH

DESCRIPTION:
This project demonstrates the use of Apache Spark through its Python API, PySpark, to generate, process and analyse a large synthetic gene-expression dataset. The program creates 1,000,000 gene-sample observations representing 1,000 genes across 1,000 samples. Samples are assigned to control and treatment conditions. Simulated expression values are generated using a gene-wise baseline, a predefined treatment-associated increment for the first 50 genes, and random noise. PySpark DataFrame operations are used to inspect the schema, count records, check missing values, calculate summary statistics, compare mean expression between conditions and rank genes by the treatment-minus-control difference. Three CSV output files are saved. The work illustrates large-scale data-processing concepts in a biology-related example. The dataset is synthetic and the analysis is exploratory; it does not establish biological significance.

AIM:
To demonstrate big data generation, processing and exploratory analysis using PySpark.

OBJECTIVES:
•	Create a reproducible synthetic dataset containing 1,000,000 gene-sample observations.
•	Represent 1,000 genes across 1,000 samples, divided equally into control and treatment groups.
•	Inspect the dataset structure and display sample records.
•	Count records and check key columns for missing values.
•	Calculate observation counts, mean expression and standard deviation for each gene-condition group.
•	Compare treatment and control mean expression values for each gene.
•	Export the generated data and analysis tables as CSV files.

DATASET GENERATION:
• Generation of expression values:
Expression values are constructed from three components. First, a baseline value varies by gene. Second, an increment of 15 is added to the first 50 genes in treatment samples only. Third, random noise is added to introduce variation. Negative values are prevented by taking the greater of zero and the calculated value.

                                         Expression = max(0, Baseline + Treatment Effect + Random Noise)

The random component uses seed 42 to support reproducibility. The first 50 genes have an intentionally introduced treatment-associated signal. This known design allows the comparison step to demonstrate whether the pattern can be recovered.

METHODOLOGY AND CODE EXPLANATION:
1 Importing libraries
from pyspark.sql import SparkSession, functions as F
import os
import csv
SparkSession starts and manages the Spark application. The functions module is imported as F to access column expressions and transformations. os and csv support folder creation and CSV export.

2 Starting the Spark session
spark = (SparkSession.builder\n    .appName("CODTECH_Task1_BigData_BiologyDemo")\n    .master("local[*]")\n    .getOrCreate())
The application is named CODTECH_Task1_BigData_BiologyDemo. local[*] runs Spark on the local computer and allows it to use available logical CPU cores. getOrCreate() obtains a Spark session.

3 Creating the DataFrame
n_genes = 1000
n_samples = 1000
n_records = n_genes * n_samples
df = spark.range(0, n_records)
The multiplication gives 1,000,000 records. spark.range creates the starting DataFrame. withColumn() derives gene_index and sample_index and constructs readable identifiers. when()/otherwise() assigns control or treatment.

4 Creating expression values
baseline = 20.0 + (gene_index % 100) * 0.8
treatment_effect = 15.0 for treatment samples and gene_index < 50; otherwise 0.0
noise = (random value between 0 and 1 - 0.5) * 10
expression = max(0, baseline + treatment_effect + noise)
These lines describe the logic implemented with PySpark column expressions. The final DataFrame retains gene_id, sample_id, condition and expression, with expression rounded to three decimal places.

5 Data inspection and quality checks
df.printSchema()
df.show(5, truncate=False)
print("Total records:", df.count())
printSchema() displays field names and data types. show() displays five records. count() returns the number of rows. A null-check expression sums null indicators for each key column. Zero missing values are expected because the script constructs every field.

6 Grouped summary statistics
summary = (df.groupBy("gene_id", "condition")\n .agg(F.count("*").alias("observations"),\n         F.round(F.avg("expression"), 3).alias("mean_expression"),\n         F.round(F.stddev("expression"), 3).alias("sd_expression")))
groupBy() forms groups for each gene and condition. Count () calculates the number of records, avg() calculates mean expression and stddev() calculates sample standard deviation. The summary is expected to contain 2,000 gene-condition rows: 1,000 genes multiplied by two conditions.

7 Comparing conditions
wide = (summary.groupBy("gene_id")\n  .pivot("condition", ["control", "treatment"])\n    .agg(F.first("mean_expression"))\n    .withColumn("difference_treatment_minus_control",\n        F.round(F.col("treatment") - F.col("control"), 3)))
pivot() places control and treatment means in separate columns. The difference is treatment mean minus control mean. A positive difference indicates a higher simulated mean in treatment; a negative difference indicates a lower mean. The code sorts differences in descending order and displays the 15 largest.

8 Counting records by condition
df.groupBy("condition").count().show()
This aggregation checks the distribution of records. Expected counts are 500,000 control observations and 500,000 treatment observations.

9 Exporting results
The script creates an output folder beside the Python file. A helper function writes a header and iterates over DataFrame rows to save CSV files. This approach is used in this demonstration; Spark's native distributed CSV or Parquet writers are generally more scalable for production workflows.

RESULTS AND OBSERVATIONS:
• Analysis	Expected result	Interpretation
• Total records	-> 1,000,000 -> The DataFrame contains one million observations.
• Control observations -> 500,000	-> 500 control samples × 1,000 genes.
• Treatment observations -> 500,000	-> 500 treatment samples × 1,000 genes.
• Missing values	-> 0 expected -> All final fields are constructed by the script.
• Summary groups -> 2,000	1,000 genes × 2 conditions.
• Gene comparison rows	1,000	One comparison row per gene.

INTERPRETATION:
Because the first 50 genes in treatment samples were assigned an additional value of 15, their treatment means are designed to be higher than their control means on average. The ranking should therefore place many of these genes among the largest positive differences. Random variation can affect the exact order. This is a demonstration of recovering a pattern deliberately embedded in synthetic data, not evidence of a biological treatment effect.

LIMITATIONS:
•	The dataset is synthetic and does not represent actual laboratory measurements.
•	The treatment effect is manually introduced for the first 50 genes; it is not discovered from independent experimental evidence.
•	Spark runs in local mode on one computer; a distributed multi-node cluster was not configured.
•	The analysis ranks mean differences but does not calculate p-values, confidence intervals or multiple-testing-adjusted significance.
•	The generated values should not be interpreted as raw RNA-sequencing counts or validated biological expression measurements.
•	The custom CSV-writing function iterates over rows in Python. Spark's native writers are generally more suitable for larger production-scale exports.

REFERENCES:
1. Apache Software Foundation. (n.d.). PySpark documentation.
2. Apache Software Foundation. (n.d.). Spark SQL, DataFrames and Datasets Guide.
3. Zaharia, M., et al. (2016). Apache Spark: A unified engine for big data processing. Communications of the ACM, 59(11), 56–65.
4. Van den Berge, K., et al. (2019). RNA sequencing data: Hitchhiker's guide to expression analysis. Annual Review of Biomedical Data Science, 2, 139–173.
5. Chowdhury, H. A., et al. (2020). Differential expression analysis of RNA-seq reads: Overview, taxonomy, and tools. IEEE/ACM Transactions on Computational Biology and Bioinformatics, 17(2), 566–586.
6. Python Software Foundation. (n.d.). CSV — CSV File Reading and Writing.
7. Python Software Foundation. (n.d.). OS — Miscellaneous Operating System Interfaces.

