"""
Charge les alertes de fraude dans Cassandra.

"""

from pathlib import Path

import psycopg2
from cassandra.cluster import Cluster

# --- Configuration ---
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "fraud_db",
    "user":     "fraud_user",
    "password": "fraud_password",
}
CASSANDRA_HOSTS    = ["localhost"]
CASSANDRA_KEYSPACE = "fraud_alerts"
SCHEMA_FILE        = Path("sql/schema_cassandra.cql")
DEFAULT_FRAUD_SCORE = 0.95  # Score simulé pour le bootstrap


def apply_schema(session) -> None:
    """Exécute le fichier CQL (statement par statement)."""
    print(f"📐 Application du schéma depuis {SCHEMA_FILE}...")
    cql = SCHEMA_FILE.read_text(encoding="utf-8")
    # Le driver ne supporte pas le multi-statement → on split sur ';'
    statements = [s.strip() for s in cql.split(";") if s.strip()]
    for stmt in statements:
        session.execute(stmt)
    print(f"   ✅ {len(statements)} statements exécutés (keyspace + tables)")


def fetch_frauds_from_postgres() -> list[tuple]:
    """Récupère toutes les transactions frauduleuses depuis PostgreSQL."""
    print("🐘 Récupération des fraudes depuis PostgreSQL...")
    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT transaction_id, timestamp, card_id, amount, currency,
                       merchant, category, country
                FROM transactions
                WHERE is_fraud = TRUE
                ORDER BY timestamp DESC;
            """)
            rows = cur.fetchall()
    print(f"   ✅ {len(rows):,} fraudes récupérées")
    return rows


def load_alerts(session, frauds: list[tuple]) -> None:
    """Insère chaque fraude dans les deux tables d'alertes."""
    print("💾 Chargement dans Cassandra...")

    # Statements préparés : Cassandra parse une fois, exécute N fois → bien plus rapide
    stmt_by_card = session.prepare("""
        INSERT INTO alerts_by_card (
            card_id, alert_timestamp, transaction_id, amount, currency,
            merchant, category, country, fraud_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    stmt_by_day = session.prepare("""
        INSERT INTO alerts_by_day (
            alert_date, alert_timestamp, transaction_id, card_id, amount, currency,
            merchant, category, country, fraud_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for tx_id, ts, card_id, amount, currency, merchant, category, country in frauds:
        session.execute(stmt_by_card, (
            card_id, ts, tx_id, amount, currency, merchant, category, country,
            DEFAULT_FRAUD_SCORE,
        ))
        session.execute(stmt_by_day, (
            ts.date(), ts, tx_id, card_id, amount, currency, merchant, category, country,
            DEFAULT_FRAUD_SCORE,
        ))


def print_summary(session) -> None:
    """Vérifie le chargement avec quelques requêtes."""
    n_card = session.execute("SELECT COUNT(*) FROM alerts_by_card").one().count
    n_day  = session.execute("SELECT COUNT(*) FROM alerts_by_day").one().count
    print(f"\n✅ {n_card:,} alertes dans alerts_by_card")
    print(f"✅ {n_day:,} alertes dans alerts_by_day")


def main() -> None:
    print(f"🔗 Connexion à Cassandra ({CASSANDRA_HOSTS})...")
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    try:
        apply_schema(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)

        frauds = fetch_frauds_from_postgres()
        load_alerts(session, frauds)
        print_summary(session)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()