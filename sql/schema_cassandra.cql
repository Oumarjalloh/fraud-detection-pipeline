-- =====================================================================
-- Schéma Cassandra : keyspace et tables d'alertes de fraude
-- =====================================================================
-- Stratégie de modélisation : "une table par requête métier"
-- (dénormalisation assumée pour optimiser les lectures).
-- =====================================================================

-- Création du keyspace (équivalent d'une "database" en SQL)
CREATE KEYSPACE IF NOT EXISTS fraud_alerts
WITH replication = {
    'class':              'SimpleStrategy',
    'replication_factor': 1
};

-- ---------------------------------------------------------------------
-- Table 1 : alertes par carte
-- Requête cible : SELECT * FROM alerts_by_card WHERE card_id = ?
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_alerts.alerts_by_card (
    card_id          TEXT,
    alert_timestamp  TIMESTAMP,
    transaction_id   TEXT,
    amount           DECIMAL,
    currency         TEXT,
    merchant         TEXT,
    category         TEXT,
    country          TEXT,
    fraud_score      FLOAT,
    PRIMARY KEY ((card_id), alert_timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (alert_timestamp DESC, transaction_id ASC);

-- ---------------------------------------------------------------------
-- Table 2 : alertes par jour
-- Requête cible : SELECT * FROM alerts_by_day WHERE alert_date = ?
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_alerts.alerts_by_day (
    alert_date       DATE,
    alert_timestamp  TIMESTAMP,
    transaction_id   TEXT,
    card_id          TEXT,
    amount           DECIMAL,
    currency         TEXT,
    merchant         TEXT,
    category         TEXT,
    country          TEXT,
    fraud_score      FLOAT,
    PRIMARY KEY ((alert_date), alert_timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (alert_timestamp DESC, transaction_id ASC);