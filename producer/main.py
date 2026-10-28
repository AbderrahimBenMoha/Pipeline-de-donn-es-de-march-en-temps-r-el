import json
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
from websocket import WebSocketApp


# Configuration du pipeline
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "market-data"

# Flux public des transactions Bitcoin sur Binance Spot
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/ws/btcusdt@trade"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Producteur qui envoie des messages JSON à Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

message_count = 0


def on_open(ws):
    logging.info("Connexion au flux Binance établie.")


def on_message(ws, raw_message):
    global message_count

    try:
        data = json.loads(raw_message)

        # On ne conserve que les événements de transaction
        if data.get("e") != "trade":
            return

        # Normalisation des données reçues
        event = {
            "source": "binance_spot",
            "symbol": data["s"],
            "trade_id": data["t"],
            "price": data["p"],
            "quantity": data["q"],
            "trade_time": datetime.fromtimestamp(
                data["T"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "event_time": datetime.fromtimestamp(
                data["E"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "buyer_is_market_maker": data["m"],
            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        # Envoi de l'événement vers Kafka
        future = producer.send(
            KAFKA_TOPIC,
            value=event,
        )

        message_count += 1

        # Vérifier l'envoi et afficher le premier événement
        if message_count == 1:
            future.get(timeout=10)
            logging.info(
                "Première transaction envoyée : %s",
                json.dumps(event, ensure_ascii=False),
            )

        # Afficher périodiquement l'avancement
        elif message_count % 20 == 0:
            producer.flush(timeout=10)
            logging.info(
                "%s transactions envoyées vers Kafka.",
                message_count,
            )

    except Exception:
        logging.exception(
            "Erreur lors du traitement d'une transaction."
        )


def on_error(ws, error):
    logging.error("Erreur WebSocket : %s", error)


def on_close(ws, status_code, message):
    logging.info(
        "Connexion fermée : code=%s, message=%s",
        status_code,
        message,
    )


if __name__ == "__main__":
    websocket = WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    try:
        logging.info("Connexion au flux BTC/USDT...")
        websocket.run_forever()

    except KeyboardInterrupt:
        logging.info("Arrêt demandé par l'utilisateur.")

    finally:
        producer.flush()
        producer.close()
        logging.info("Producteur Kafka arrêté.")
