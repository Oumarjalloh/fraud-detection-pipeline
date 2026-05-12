"""
Génère un fichier CSV de transactions bancaires fictives avec un taux
de fraude réaliste et des patterns détectables.

Usage :
    python src/ingestion/generate_transactions.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

# --- Reproductibilité ---
# Avec une seed fixe, on génère TOUJOURS les mêmes données. Pratique
# pour le debugging et pour qu'un recruteur ait les mêmes résultats que toi.
fake = Faker("fr_FR")
Faker.seed(42)
random.seed(42)

# --- Configuration ---
NUM_TRANSACTIONS = 10_000     # Nombre total de transactions à générer
NUM_CARDS = 500               # Nombre de cartes bancaires différentes
FRAUD_RATE = 0.015            # 1.5% de transactions frauduleuses
OUTPUT_PATH = Path("data/raw/transactions.csv")

# --- Référentiels métier ---
MERCHANTS_BY_CATEGORY = {
    "groceries":     ["Carrefour", "Monoprix", "Lidl", "Auchan", "Franprix"],
    "transport":     ["Uber", "SNCF", "RATP", "Total Energies", "BlaBlaCar"],
    "entertainment": ["Netflix", "Spotify", "Disney+", "Fnac", "Pathé"],
    "shopping":      ["Amazon", "Zara", "Decathlon", "IKEA", "Galeries Lafayette"],
    "restaurant":    ["McDonald's", "Burger King", "Sushi Shop", "Uber Eats", "Deliveroo"],
    "tech":          ["Apple Store", "Darty", "Boulanger", "Microsoft", "Google"],
}

# Plages de montants réalistes (min, max) par catégorie
AMOUNT_RANGES = {
    "groceries":     (10, 200),
    "transport":     (5, 80),
    "entertainment": (5, 60),
    "shopping":      (15, 800),
    "restaurant":    (8, 120),
    "tech":          (20, 2500),
}

# Pays habituels d'un détenteur français (poids déséquilibré : surtout la France)
NORMAL_COUNTRIES = ["FR", "FR", "FR", "FR", "FR", "BE", "ES", "DE", "IT"]

# Pays "à risque" utilisés pour simuler des fraudes
SUSPICIOUS_COUNTRIES = ["RU", "NG", "CN", "BR", "RO"]


def generate_card_ids(num_cards: int) -> list[str]:
    """Crée une liste d'identifiants de cartes uniques, type CARD-000001."""
    return [f"CARD-{i:06d}" for i in range(1, num_cards + 1)]


def generate_transaction(transaction_id: int, card_ids: list[str]) -> dict:
    """
    Génère une transaction. Avec une petite probabilité (FRAUD_RATE),
    elle suit un pattern frauduleux (gros montant + pays étranger + heure tardive).
    """
    is_fraud = random.random() < FRAUD_RATE

    # Tirage de la catégorie et du marchand
    category = random.choice(list(MERCHANTS_BY_CATEGORY.keys()))
    merchant = random.choice(MERCHANTS_BY_CATEGORY[category])

    # Plage de montant normale pour cette catégorie
    amount_min, amount_max = AMOUNT_RANGES[category]

    if is_fraud:
        # Fraude : montant largement au-dessus de la norme, pays à risque, nuit
        amount = round(random.uniform(amount_max * 3, amount_max * 10), 2)
        country = random.choice(SUSPICIOUS_COUNTRIES)
        hour = random.randint(1, 5)
    else:
        amount = round(random.uniform(amount_min, amount_max), 2)
        country = random.choice(NORMAL_COUNTRIES)
        # Heures pondérées : pic en journée, creux la nuit
        hour = random.choices(
            range(24),
            weights=[1, 1, 1, 1, 1, 2, 4, 6, 8, 8, 8, 9,
                     10, 9, 8, 8, 9, 10, 9, 8, 6, 4, 3, 2],
            k=1,
        )[0]

    # Timestamp réparti sur les 30 derniers jours
    days_ago = random.randint(0, 30)
    timestamp = (
        datetime.now()
        - timedelta(days=days_ago)
    ).replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))

    return {
        "transaction_id": f"TXN-{transaction_id:08d}",
        "timestamp": timestamp.isoformat(timespec="seconds"),
        "card_id": random.choice(card_ids),
        "amount": amount,
        "currency": "EUR",
        "merchant": merchant,
        "category": category,
        "country": country,
        "city": fake.city(),
        "is_fraud": is_fraud,
    }


def main() -> None:
    """Point d'entrée : génère le CSV complet."""
    print(f"🏦 Génération de {NUM_TRANSACTIONS:,} transactions...")

    # Crée le dossier data/raw/ s'il n'existe pas
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    card_ids = generate_card_ids(NUM_CARDS)

    fieldnames = [
        "transaction_id", "timestamp", "card_id", "amount", "currency",
        "merchant", "category", "country", "city", "is_fraud",
    ]

    fraud_count = 0

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, NUM_TRANSACTIONS + 1):
            transaction = generate_transaction(i, card_ids)
            if transaction["is_fraud"]:
                fraud_count += 1
            writer.writerow(transaction)

            # Petit indicateur de progression tous les 1000
            if i % 1000 == 0:
                print(f"   {i:,}/{NUM_TRANSACTIONS:,} transactions générées")

    print(f"\n✅ Fichier créé : {OUTPUT_PATH}")
    print(f"📊 {NUM_TRANSACTIONS:,} transactions au total")
    print(f"🚨 {fraud_count:,} fraudes ({fraud_count / NUM_TRANSACTIONS * 100:.2f}%)")


# Point d'entrée standard Python
if __name__ == "__main__":
    main()