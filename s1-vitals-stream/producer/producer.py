import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:8183")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1"))

PLAGES = {
    "HR": (55, 115, "bpm"),
    "SPO2": (90, 100, "%"),
    "ABP_SYS": (95, 145, "mmHg"),
}

_stop = False
_envoyes = 0
_echecs = 0


def gerer_signal(signum, frame):
    global _stop
    _stop = True


def accuse_reception(err, msg):
    global _envoyes, _echecs
    if err is not None:
        _echecs += 1
        print(f"echec livraison {msg.key()}: {err}", file=sys.stderr)
    else:
        _envoyes += 1


def creer_producteur():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulator-s1",
        "acks": "all",
        "linger.ms": 50,
        "retries": 5,
        "enable.idempotence": True,
        "partitioner": "murmur2",
    })


def generer_releve(patient_id):
    vital_type = random.choice(list(PLAGES))
    bas, haut, unite = PLAGES[vital_type]
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": round(random.uniform(bas, haut), 1),
        "unit": unite,
    }


def main():
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    producteur = creer_producteur()
    debut = time.monotonic()

    while not _stop:
        for patient_id in patients:
            releve = generer_releve(patient_id)
            producteur.produce(
                topic=TOPIC,
                key=patient_id.encode("utf-8"),
                value=json.dumps(releve).encode("utf-8"),
                on_delivery=accuse_reception,
            )
        producteur.poll(0)
        time.sleep(INTERVAL_S)

    restants = producteur.flush(10)
    duree = time.monotonic() - debut
    debit = _envoyes / duree if duree else 0
    print(
        f"bilan: envoyes={_envoyes} non_livres={restants} echecs={_echecs} "
        f"duree={duree:.1f}s debit={debit:.1f} msg/s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
