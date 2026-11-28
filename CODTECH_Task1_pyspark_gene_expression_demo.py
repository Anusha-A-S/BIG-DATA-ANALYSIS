"""
CODTECH Internship – Task 1
Big Data Analysis with PySpark using simulated gene-expression records.

This is a reproducible demonstration dataset, NOT real experimental data.
Install: pip install pyspark
Run:     python CODTECH_Task1_pyspark_gene_expression_demo.py
"""

from pyspark.sql import SparkSession, functions as F
import os
import csv

spark = (SparkSession.builder
    .appName("CODTECH_Task1_BigData_BiologyDemo")
    .master("local[*]")  # local parallelism; use a cluster master for a cluster deployment
    .getOrCreate()
)

# Create 1,000,000 synthetic records using Spark's distributed range operation.
# 1,000 genes x 1,000 samples = 1,000,000 gene-sample observations.
n_genes = 1000
n_samples = 1000
n_records = n_genes * n_samples

df = (
    spark.range(0, n_records)
    .withColumn("gene_index", (F.col("id") % n_genes).cast("int"))
    .withColumn("sample_index", F.floor(F.col("id") / n_genes).cast("int"))
    .withColumn("gene_id", F.concat(F.lit("GENE_"), F.lpad(F.col("gene_index").cast("string"), 4, "0")))
    .withColumn("sample_id", F.concat(F.lit("SAMPLE_"), F.lpad(F.col("sample_index").cast("string"), 4, "0")))
    .withColumn(
        "condition",
        F.when((F.col("sample_index") % 2) == 0, F.lit("control"))
         .otherwise(F.lit("treatment"))
    )
    # Baseline expression varies by gene; selected first 50 genes receive
    # an added treatment-associated signal. Random noise makes the demo realistic.
    .withColumn("baseline", F.lit(20.0) + (F.col("gene_index") % 100) * F.lit(0.8))
    .withColumn(
        "treatment_effect",
        F.when(
            (F.col("condition") == "treatment") & (F.col("gene_index") < 50),
            F.lit(15.0)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("noise", (F.rand(seed=42) - F.lit(0.5)) * F.lit(10.0))
    .withColumn(
        "expression",
        F.greatest(F.lit(0.0), F.col("baseline") + F.col("treatment_effect") + F.col("noise"))
    )
    .select("gene_id", "sample_id", "condition", F.round("expression", 3).alias("expression"))
)

print("Schema:")
df.printSchema()
print("First five records:")
df.show(5, truncate=False)

# 1) Basic quality checks
print("Total records:", df.count())
print("Missing values in key fields:")
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in ["gene_id", "sample_id", "condition", "expression"]
]).show()

# 2) Summarise expression by gene and experimental condition
summary = (
    df.groupBy("gene_id", "condition")
      .agg(
          F.count("*").alias("observations"),
          F.round(F.avg("expression"), 3).alias("mean_expression"),
          F.round(F.stddev("expression"), 3).alias("sd_expression")
      )
)

# Pivot control/treatment means to compare conditions gene by gene
wide = (
    summary.groupBy("gene_id")
    .pivot("condition", ["control", "treatment"])
    .agg(F.first("mean_expression"))
    .withColumn("difference_treatment_minus_control",
                F.round(F.col("treatment") - F.col("control"), 3))
)

print("Largest positive treatment-control differences (exploratory ranking only):")
wide.orderBy(F.desc("difference_treatment_minus_control")).show(15, truncate=False)

# 3) Count records by condition
print("Records by condition:")
df.groupBy("condition").count().show()
def save_dataframe_as_csv(dataframe, filepath):
    with open(filepath, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(dataframe.columns)

        for row in dataframe.toLocalIterator():
            writer.writerow(list(row))

# 4) Save reproducible outputs. Parquet is suitable for analytics pipelines.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)
print("Saving results in:", OUTPUT_DIR)

save_dataframe_as_csv(
    df,
    os.path.join(OUTPUT_DIR, "gene_expression_records.csv")
)

save_dataframe_as_csv(
    summary,
    os.path.join(OUTPUT_DIR, "expression_summary.csv")
)

save_dataframe_as_csv(
    wide,
    os.path.join(OUTPUT_DIR, "gene_condition_comparison.csv")
)

print("All analysis results saved successfully!")
print("Project folder:", BASE_DIR)

spark.stop()


