"""
Charge les Parquets d'agrégats (issus de Spark) dans PostgreSQL comme
data mart analytique. Quatre tables avec un préfixe `mart_`.

Pipeline complet :
    CSV → Spark (agrégations) → Parquet → psycopg2 → PostgreSQL
                                          ^^^^^^^^^^^^^^^^^^^^^^
                                          Cette étape.

Usage :
    python src/processing/load_data_mart.py
"""

from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- Configuration ---
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "fraud_db",
    "user":     "fraud_user",
    "password": "fraud_password",
}
PARQUET_DIR = Path("data/processed")

# --- Schémas des 4 tables data mart ---
SCHEMAS = {
    "mart_fraud_by_category": """
        CREATE TABLE mart_fraud_by_category (
            category            VARCHAR(50)    PRIMARY KEY,
            total_transactions  INTEGER        NOT NULL,
            fraud_count         INTEGER        NOT NULL,
            total_amount        NUMERIC(14, 2),
            fraud_amount        NUMERIC(14, 2),
            avg_amount          NUMERIC(10, 2),
            fraud_rate_pct      NUMERIC(5, 2)
        );
    """,
    "mart_fraud_by_country": """
        CREATE TABLE mart_fraud_by_country (
            country             CHAR(2)        PRIMARY KEY,
            total_transactions  INTEGER        NOT NULL,
            fraud_count         INTEGER        NOT NULL,
            total_amount        NUMERIC(14, 2),
            fraud_rate_pct      NUMERIC(5, 2)
        );
    """,
    "mart_fraud_by_hour": """
        CREATE TABLE mart_fraud_by_hour (
            hour_of_day         INTEGER        PRIMARY KEY
                                              CHECK (hour_of_day BETWEEN 0 AND 23),
            total_transactions  INTEGER        NOT NULL,
            fraud_count         INTEGER        NOT NULL,
            fraud_rate_pct      NUMERIC(5, 2)
        );
    """,
    "mart_top_fraud_merchants": """
        CREATE TABLE mart_top_fraud_merchants (
            merchant            VARCHAR(100),
            category            VARCHAR(50),
            fraud_count         INTEGER        NOT NULL,
            fraud_amount        NUMERIC(14, 2),
            avg_fraud_amount    NUMERIC(10, 2),
            PRIMARY KEY (merchant, category)
        );
    """,
}

# Mapping table data mart → dossier Parquet correspondant
PARQUET_MAPPING = {
    "mart_fraud_by_category":   "fraud_by_category",
    "mart_fraud_by_country":    "fraud_by_country",
    "mart_fraud_by_hour":       "fraud_by_hour",
    "mart_top_fraud_merchants": "top_fraud_merchants",
}


def load_table(conn, table_name: str, df: pd.DataFrame, schema_sql: str) -> None:
    """DROP + CREATE + bulk INSERT (pattern idempotent)."""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(schema_sql)

        # Conversion DataFrame → liste de tuples pour execute_values
        rows = [tuple(row) for row in df.itertuples(index=False)]
        cols = ", ".join(df.columns)

        # Bulk insert performant : un seul INSERT pour toutes les lignes
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({cols}) VALUES %s",
            rows,
        )
    conn.commit()
    print(f"   ✅ {table_name:<30} : {len(rows):>3} lignes")


def main() -> None:
    print(f"🐘 Connexion à PostgreSQL ({PG_CONFIG['host']}:{PG_CONFIG['port']})...")
    with psycopg2.connect(**PG_CONFIG) as conn:

        print("\n📥 Chargement des Parquets dans le data mart...")
        for mart_table, parquet_subdir in PARQUET_MAPPING.items():
            parquet_path = PARQUET_DIR / parquet_subdir / "data.parquet"
            df = pd.read_parquet(parquet_path)
            load_table(conn, mart_table, df, SCHEMAS[mart_table])

        # --- Vérification : compter les lignes de chaque table ---
        print("\n📊 Résumé du data mart :")
        with conn.cursor() as cur:
            for table in PARQUET_MAPPING.keys():
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                print(f"   - {table:<30} {count:>3} lignes")

        # --- Une vraie requête métier pour finir en beauté ---
        print("\n🚨 Top 5 pays à plus haut risque de fraude :")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country, fraud_count, total_transactions, fraud_rate_pct
                FROM mart_fraud_by_country
                ORDER BY fraud_rate_pct DESC, total_amount DESC
                LIMIT 5;
            """)
            print(f"   {'Pays':<6} {'Fraudes':<10} {'Total':<10} {'Taux %':<8}")
            print(f"   {'-'*6} {'-'*10} {'-'*10} {'-'*8}")
            for country, fraud, total, rate in cur.fetchall():
                print(f"   {country:<6} {fraud:<10} {total:<10} {rate:<8}")


if __name__ == "__main__":
    main()