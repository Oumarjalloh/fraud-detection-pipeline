"""
Charge les transactions depuis le CSV vers PostgreSQL.

"""

from pathlib import Path

import psycopg2

# --- Configuration (à externaliser dans un .env en prod) ---
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "fraud_db",
    "user":     "fraud_user",
    "password": "fraud_password",
}

SCHEMA_FILE = Path("sql/schema_postgres.sql")
CSV_FILE    = Path("data/raw/transactions.csv")
TABLE_NAME  = "transactions"


def run_schema(conn) -> None:
    """Exécute le fichier SQL de création du schéma."""
    print(f"📐 Application du schéma depuis {SCHEMA_FILE}...")
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("   ✅ Table et index prêts")


def load_csv(conn) -> None:
    """Vide la table puis charge le CSV avec COPY."""
    print(f"📥 Chargement de {CSV_FILE}...")
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME};")
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER",
                f,
            )
    conn.commit()


def print_summary(conn) -> None:
    """Affiche quelques stats pour valider le chargement."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        total = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE is_fraud = TRUE;")
        frauds = cur.fetchone()[0]

        cur.execute(f"""
            SELECT category, COUNT(*) AS n
            FROM {TABLE_NAME}
            GROUP BY category
            ORDER BY n DESC;
        """)
        by_category = cur.fetchall()

    print(f"\n✅ {total:,} transactions chargées")
    print(f"🚨 {frauds:,} fraudes ({frauds / total * 100:.2f}%)")
    print(f"\n📊 Répartition par catégorie :")
    for category, n in by_category:
        print(f"   - {category:<15} {n:>5,}")


def main() -> None:
    print(f"🐘 Connexion à PostgreSQL ({DB_CONFIG['host']}:{DB_CONFIG['port']})...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        run_schema(conn)
        load_csv(conn)
        print_summary(conn)


if __name__ == "__main__":
    main()