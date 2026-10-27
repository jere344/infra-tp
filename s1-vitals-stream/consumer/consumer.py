import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:8183")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "vitals-persister")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5.0"))
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "55432"))
PG_DB = os.getenv("PG_DB", "healthstream")
PG_USER = os.getenv("PG_USER", "hs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "hs_password")

SQL_INSERT = (
    "INSERT INTO vitals_raw "
    "(kafka_partition, kafka_offset, patient_id, event_time, vital_type, value, unit) "
    "VALUES %s "
    "ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING"
)

_stop = False


def gerer_signal(signum, frame):
    global _stop
    _stop = True


def creer_consommateur():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": "vitals-persister",
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "max.poll.interval.ms": 300000,
    })


def connecter_postgres(tentatives=30, delai_s=2.0):
    for essai in range(1, tentatives + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASSWORD)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as err:
            print(f"postgres indisponible ({essai}/{tentatives}): {err}",
                  file=sys.stderr, flush=True)
            time.sleep(delai_s)
    raise RuntimeError("postgres injoignable")


def ligne_depuis_message(msg):
    releve = json.loads(msg.value())
    event_time = datetime.fromtimestamp(releve["event_time"] / 1000, tz=timezone.utc)
    return (
        msg.partition(),
        msg.offset(),
        str(releve["patient_id"]),
        event_time,
        str(releve["vital_type"]),
        float(releve["value"]),
        str(releve["unit"]),
    )


def inserer_lot(conn, lignes):
    with conn.cursor() as cur:
        execute_values(cur, SQL_INSERT, lignes, page_size=len(lignes))
        n = cur.rowcount
    conn.commit()
    return n


def main():
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    consommateur = creer_consommateur()
    consommateur.subscribe([TOPIC])
    conn = connecter_postgres()

    lus = inseres = ignores = rejets = 0
    lot = []
    dernier_envoi = time.time()
    debut = time.monotonic()

    while not _stop:
        msg = consommateur.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"erreur kafka: {msg.error()}", file=sys.stderr, flush=True)
            else:
                lus += 1
                try:
                    lot.append(ligne_depuis_message(msg))
                except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                    rejets += 1

        lot_pret = lot and (len(lot) >= BATCH_SIZE
                            or time.time() - dernier_envoi >= BATCH_TIMEOUT_S)
        if lot_pret:
            try:
                n = inserer_lot(conn, lot)                # 1. la base d'abord
                consommateur.commit(asynchronous=False)   # 2. l'acquittement ENSUITE
            except psycopg2.OperationalError:
                conn = connecter_postgres()   # lot CONSERVÉ, offsets NON acquittés
                continue
            inseres += n
            ignores += len(lot) - n
            print(f"LOT taille={len(lot)} inseres={n} lus={lus}", flush=True)
            lot, dernier_envoi = [], time.time()

    if lot:
        try:
            n = inserer_lot(conn, lot)
            consommateur.commit(asynchronous=False)
            inseres += n
            ignores += len(lot) - n
        except psycopg2.OperationalError:
            print("lot residuel non ecrit (postgres indisponible)", file=sys.stderr,
                  flush=True)

    duree = time.monotonic() - debut
    print(f"Arret. lus={lus} inseres={inseres} ignores_doublons={ignores} "
          f"rejets={rejets} debit={lus / duree:.1f} msg/s", flush=True)
    consommateur.close()
    conn.close()


if __name__ == "__main__":
    main()
