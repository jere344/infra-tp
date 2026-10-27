import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:8183")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() == "true"
INSTANCE = os.getenv("INSTANCE", "c1")
STRATEGIE = os.getenv("ASSIGNMENT_STRATEGY", "cooperative-sticky")

_stop = False
lus = 0
par_partition = {}


def gerer_signal(signum, frame):
    global _stop
    _stop = True


def sur_attribution(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] ATTRIBUTION t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def sur_retrait(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] RETRAIT t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def creer_consommateur():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"lecteur-{INSTANCE}",
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": AUTO_COMMIT,
        "partition.assignment.strategy": STRATEGIE,
    })


def main():
    global lus
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    consommateur = creer_consommateur()
    # subscribe() n'attribue rien : l'attribution arrive au premier poll().
    consommateur.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)

    while not _stop:
        msg = consommateur.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[{INSTANCE}] erreur: {msg.error()}", file=sys.stderr, flush=True)
            continue

        releve = json.loads(msg.value())
        lus += 1
        par_partition[msg.partition()] = par_partition.get(msg.partition(), 0) + 1

        if lus % 50 == 0:
            print(f"[{INSTANCE}] lus={lus} p={msg.partition()} off={msg.offset()} "
                  f"{releve['patient_id']} {releve['vital_type']}={releve['value']}",
                  flush=True)

        if not AUTO_COMMIT and lus % 100 == 0:
            consommateur.commit(asynchronous=False)

    print(f"[{INSTANCE}] Arret. lus={lus} repartition={dict(sorted(par_partition.items()))}",
          flush=True)
    consommateur.close()


if __name__ == "__main__":
    main()
