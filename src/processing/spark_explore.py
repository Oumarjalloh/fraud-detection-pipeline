"""
Premier contact avec Spark : lecture de la table 'transactions' depuis
PostgreSQL, exploration du DataFrame, comptages et filtrages basiques.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Configuration ---

PG_URL  = "jdbc:postgresql://localhost:5432/fraud_db?channelBinding=disable"
PG_USER = "fraud_user"
PG_PASS = "fraud_password"

JDBC_JAR = "jars/postgresql-42.7.3.jar"


def get_spark() -> SparkSession:
    """Crée une SparkSession locale avec le driver JDBC PostgreSQL chargé."""
    return (
        SparkSession.builder
        .appName("FraudDetection-Explore")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession):
    """Lecture de la table transactions via JDBC (syntaxe .option())."""
    return (
        spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "transactions")
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("📥 Lecture de la table 'transactions' depuis PostgreSQL...")
    df = read_transactions(spark)

    print("\n📐 Schéma du DataFrame :")
    df.printSchema()

    total = df.count()
    print(f"\n📈 Total transactions : {total:,}")

    print("\n🔍 5 premières transactions :")
    df.show(5, truncate=False)

    fraud_count = df.filter(col("is_fraud") == True).count()
    print(f"\n🚨 Transactions frauduleuses : {fraud_count:,} "
          f"({fraud_count / total * 100:.2f}%)")

    print("\n🚨 5 fraudes en exemple :")
    (
        df.filter(col("is_fraud") == True)
          .select("transaction_id", "amount", "merchant", "country")
          .show(5, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()