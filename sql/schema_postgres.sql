-- =====================================================================
-- Schéma PostgreSQL : table principale des transactions bancaires
-- =====================================================================
-- Cette table stocke toutes les transactions historiques. Servira de
-- source pour les agrégats analytiques (data mart) et l'entraînement
-- des modèles ML.
-- =====================================================================

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   VARCHAR(20)    PRIMARY KEY,
    timestamp        TIMESTAMP      NOT NULL,
    card_id          VARCHAR(20)    NOT NULL,
    amount           NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency         CHAR(3)        NOT NULL,
    merchant         VARCHAR(100)   NOT NULL,
    category         VARCHAR(50)    NOT NULL,
    country          CHAR(2)        NOT NULL,
    city             VARCHAR(100),
    is_fraud         BOOLEAN        NOT NULL DEFAULT FALSE
);

-- Index pour accélérer les requêtes analytiques fréquentes
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_tx_card_id   ON transactions(card_id);
CREATE INDEX IF NOT EXISTS idx_tx_country   ON transactions(country);
CREATE INDEX IF NOT EXISTS idx_tx_is_fraud  ON transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_tx_category  ON transactions(category);