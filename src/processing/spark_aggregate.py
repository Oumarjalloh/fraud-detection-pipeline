"""
Job Spark batch : lit le CSV de transactions, calcule des agrégats
analytiques (par catégorie, pays, heure, marchand) et les écrit en
Parquet (format colonnaire compressé, standard data engineering).

"""

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, hour,
    round as spark_round, desc,
)

# --- Configuration ---
INPUT_CSV  = "data/raw/transactions.csv"
OUTPUT_DIR = Path("data/processed")


def get_spark() -> SparkSession:
    """SparkSession locale, paramétrée pour un volume modéré."""
    return (
        SparkSession.builder
        .appName("FraudDetection-Aggregate")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession) -> DataFrame:
    """Lit le CSV de transactions, schéma inféré (timestamp, decimal...)."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_CSV)
    )


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Taux de fraude et volumes par catégorie de marchand."""
    return (
        df.groupBy("category")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(
                spark_sum(when(col("is_fraud"), col("amount")).otherwise(0)), 2
            ).alias("fraud_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            spark_round(col("fraud_count") * 100.0 / col("total_transactions"), 2),
        )
        .orderBy(desc("fraud_rate_pct"))
    )


def aggregate_by_country(df: DataFrame) -> DataFrame:
    """Détecte les pays 'à risque' (RU, NG, CN, etc.)."""
    return (
        df.groupBy("country")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            spark_round(col("fraud_count") * 100.0 / col("total_transactions"), 2),
        )
        .orderBy(desc("fraud_rate_pct"))
    )


def aggregate_by_hour(df: DataFrame) -> DataFrame:
    """Distribution des fraudes sur 24 heures (les fraudes sont nocturnes)."""
    return (
        df.withColumn("hour_of_day", hour(col("timestamp")))
        .groupBy("hour_of_day")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
        )
        .withColumn(
            "fraud_rate_pct",
            spark_round(col("fraud_count") * 100.0 / col("total_transactions"), 2),
        )
        .orderBy("hour_of_day")
    )


def top_fraud_merchants(df: DataFrame, n: int = 20) -> DataFrame:
    """Top N marchands par nombre de fraudes."""
    return (
        df.filter(col("is_fraud") == True)
        .groupBy("merchant", "category")
        .agg(
            count("*").alias("fraud_count"),
            spark_round(spark_sum("amount"), 2).alias("fraud_amount"),
            spark_round(avg("amount"), 2).alias("avg_fraud_amount"),
        )
        .orderBy(desc("fraud_count"))
        .limit(n)
    )


def write_parquet(df: DataFrame, name: str) -> None:
   
    path = OUTPUT_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    
    # Spark fait le calcul distribué → pandas récupère le résultat → pyarrow écrit
    pandas_df = df.toPandas()
    file_path = path / "data.parquet"
    pandas_df.to_parquet(file_path, engine="pyarrow", index=False)
    
    print(f"   ✅ {name} → {file_path} ({len(pandas_df):,} lignes)")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"📥 Lecture de {INPUT_CSV}...")
    df = read_transactions(spark)

    # Cache : on va passer plusieurs fois sur ce DataFrame
    df.cache()

    total = df.count()
    fraud_count = df.filter(col("is_fraud") == True).count()
    print(f"   📈 {total:,} transactions, "
          f"{fraud_count:,} fraudes ({fraud_count / total * 100:.2f}%)")

    print("\n🔬 Calcul des agrégats...")
    df_by_category   = aggregate_by_category(df)
    df_by_country    = aggregate_by_country(df)
    df_by_hour       = aggregate_by_hour(df)
    df_top_merchants = top_fraud_merchants(df)

    print(f"\n💾 Écriture des Parquets dans {OUTPUT_DIR}/...")
    write_parquet(df_by_category,   "fraud_by_category")
    write_parquet(df_by_country,    "fraud_by_country")
    write_parquet(df_by_hour,       "fraud_by_hour")
    write_parquet(df_top_merchants, "top_fraud_merchants")

    print("\n📊 Aperçu : taux de fraude par catégorie")
    df_by_category.show(truncate=False)

    print("\n📊 Aperçu : taux de fraude par pays")
    df_by_country.show(truncate=False)

    print("\n📊 Aperçu : taux de fraude par heure")
    df_by_hour.show(24, truncate=False)

    print("\n📊 Aperçu : top marchands frauduleux")
    df_top_merchants.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()