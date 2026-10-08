# MESURES — Séance 1 : Conteneurs et flux (Docker, Compose et premier producteur Kafka)

**Emy Guerin** — Master 2 · YNOV Campus Montpellier · 2026-2027

## Vérification des prérequis

| Outil          | Version relevée |
| -------------- | --------------- |
| Docker Engine  | `29.8.1`        |
| Docker Compose | `2.29.2`        |
| Python         | `3.12.3`        |

## Partie 1 — Docker : image, conteneur, couches

### Question 1.1

> Il ne voit pas le fichier ("fichier absent") car les deux conteneur on chacun leurs couche de fichiers qui par défaut ne partagent pas

### Mesure 1

| Grandeur                                          | Valeur relevée                                           |
| ------------------------------------------------- | -------------------------------------------------------- |
| Taille de `python:3.12-slim`                      | `119mb`                                                  |
| Taille de votre image finale                      | `130mb`                                                  |
| Nombre de couches non vides dans `docker history` | `4`                                                      |
| Couche la plus volumineuse (instruction + taille) | `RUN /bin/sh -c pip install --no-cache-dir -r…   10.7MB` |

### Mesure 2

| Scénario de rebuild                | Durée / étapes `CACHED` |
| ---------------------------------- | ----------------------- |
| Aucune modification                | `0.194s / 5 CACHED`     |
| Modification de `producer.py`      | `0.454s / 3 CACHED`     |
| Modification de `requirements.txt` | `4.192s / 1 CACHED`     |

### Question 1.2

> Si j'avais ecrite COPY . ./ avant le RUN pip install, le scénario modification de producer.py changerait. Il invaliderait le cache pour pip install et le build serait similaire au sénario modification de `requirements.txt`.

## Partie 2 — Docker Compose : du `docker run` au service déclaré

### Décisions

| Décision                         | Valeur retenue                                                    |
| -------------------------------- | ----------------------------------------------------------------- |
| Nom du volume nommé des journaux | `vitals-logs`                                                     |
| Nom du réseau applicatif         | `vitals-net` (il servira à tous les services jusqu'à la séance 6) |
| Valeur de `VITALS_TOPIC`         | `mimic-vitals` (voir le tableau 4.1)                              |

Décisions du `healthcheck` du producteur :

| Décision       | Valeur retenue | Justification                                                            |
| -------------- | -------------- | ------------------------------------------------------------------------ |
| `interval`     | `30s`          | le test est trivial (`sys.exit(0)`), inutile de le relancer plus souvent |
| `retries`      | `3`            | 1 minute 30 suffit pour retry dans le cas où on rebuild le docker        |
| `timeout`      | `5s`           | imposé par l'énoncé                                                      |
| `start_period` | `10s`          | imposé par l'énoncé                                                      |

Points à vérifier et à noter :

```
76d491107ca3   s1-vitals-stream_vitals-net   bridge    local
docker volume ls | grep vitals
> docker volume inspect s1-vitals-stream_vitals-logs --format '{{.Mountpoint}}'
/var/lib/docker/volumes/s1-vitals-stream_vitals-logs/_data
```

### Question 2.1

> - on laisse docker gérer vitals-logs pour ne pas avoir à gérer la structure du répertoire et des potentiels conflicts.
> - On peut réinstaller le docker-compose ailleurs sans avoir à porter le volume pour conserver les logs.

### Résolution par nom de service (2.3)

```
docker run --rm --network s1-vitals-stream_vitals-net busybox nslookup producer
> Server:         127.0.0.11
Address:        127.0.0.11:53

Non-authoritative answer:
Name:   producer
Address: 172.19.0.3
```
> `producer` doit passer par le réseau de docker-compose pour accéder aux autres containers il doit donc utiliser `kafka:9092` le nom de service.

### Mesure 3

Notez : délai observé avant `healthy` = `11` s.

> note : le processus s'arrète immédiatement puisque producer.py ne fait encore rien (print)
> Le service devient donc healthy puis unhealthy immédiatement avec l'éxécution.

## Partie 3 — Kafka en local, en mode KRaft

### Décisions

| Décision                                   | Valeur retenue      | Justification                        |
| ------------------------------------------ | ------------------- | ------------------------------------ |
| Port hôte publié en face de `29092`        | `8183`              | aléatoire. 9092/29092 déjà pris      |
| Valeur de `KAFKA_PROCESS_ROLES`            | `broker,controller` | = KRaft                              |
| Facteur de réplication des topics internes | `1`                 | "un seul broker"                     |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE`          | `false`             | = faute de frappe sur un nom se voie |
| Nom du volume nommé des journaux Kafka     | `kafka-logs`        | aléatoire                            |

Extrait sur le point délicat — listeners et listeners annoncés :

```
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:8183
```


### Mesure 4

Temps écoulé entre `up -d kafka` et le passage à `healthy` : `11` s.

### 3.3 Créer le topic et inspecter ses partitions

```
TOPIC=mimic-vitals
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
--create --topic "$TOPIC" --partitions 1 --replication-factor 2
```
> Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered.

Commande finale retenue (réplication ramenée à 1, un seul broker) :

```
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
--create --topic mimic-vitals --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic mimic-vitals
> Topic: mimic-vitals	TopicId: ...	PartitionCount: 1	ReplicationFactor: 1	...
```

| Décision               | Valeur retenue | Justification                        |
| ---------------------- | -------------- | ------------------------------------ |
| Nombre de partitions   | `1`            | "un seul broker"                     |
| Facteur de réplication | `1`            | plus de 1 est impossible (voir Q3.1) |


### Question 3.1

> Il n'y a qu'un seul broker paramétré dans docker-ocmpose, on ne peut donc pas répliquer dans un autre broker.

### Mesure 5

| Clé    | Partition(s) et offsets observés |
| ------ | -------------------------------- |
| `P001` | `0 et 0, 2, 4`                   |
| `P002` | `0 et 1`                         |
| `P003` | `0 et 3`                         |

Bornes de partition :

> Note : la commande ci-dessus ne fonctionnais pas (could not find kafka.tools.GetOffsetShell), selon l'IA elle est dépréciée. L'équivalent moderne :
> docker compose exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic mimic-vitals --time -1
> mimic-vitals:0:5

### Question 3.2

> Les trois messages sont dans la partition 0. Les offsets se suivent : 0, 2 et 4. On peut en déduire que l'ordre des messages est garanti à l'intérieur d'une partition, l'offeset correspond à l'ordre d'insertion.

## Partie 4 — Producteur Python de signes vitaux

### Décisions de configuration

| Paramètre            | Valeur retenue         | Justification                                                        |
| -------------------- | ---------------------- | -------------------------------------------------------------------- |
| `bootstrap.servers`  | depuis l'environnement | même code l'hôte et conteneur                                        |
| `client.id`          | `vitals-simulator-s1`  |                                                                      |
| `acks`               | `all`                  |                                                                      |
| `linger.ms`          | `50`                   | regroupe les messages en lots sur 50 ms                              |
| `retries`            | `5`                    |                                                                      |
| `enable.idempotence` | `True`                 |                                                                      |
| `partitioner`        | `murmur2`              | compatible avec le client Java de la séance 2 (voir note ci-dessous) |
| `NB_PATIENTS`        | `12`                   | un lit par patient : l'ICU compte 12 lits                            |
| `INTERVAL_S`         | `1`                    | un moniteur ICU échantillonne environ 1 Hz                           |

Sur le `partitioner` :

> mumur2 est la version par défaut en Java alors que python utilise consistent_random. Pour garder les deux compatible il faut utiliser murmur2 en python.


### 4.4 Exécuter depuis l'hôte

```
KAFKA_BOOTSTRAP=localhost:8183 VITALS_TOPIC=mimic-vitals python producer/producer.py
```

```
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic mimic-vitals --from-beginning --property print.key=true \
  --property print.partition=true --max-messages 10 --timeout-ms 10000
```

```
Partition:0     P001    {"vital_type":"HR","value":88}
Partition:0     P002    {"vital_type":"HR","value":102}
Partition:0     P001    {"vital_type":"SPO2","value":96}
Partition:0     P003    {"vital_type":"HR","value":71}
Partition:0     P001    {"vital_type":"ABP_SYS","value":118}
Partition:0     P001    {"patient_id": "P001", "event_time": 1791456405511, "vital_type": "SPO2", "value": 95.2, "unit": "%"}
Partition:0     P002    {"patient_id": "P002", "event_time": 1791456405512, "vital_type": "ABP_SYS", "value": 142.5, "unit": "mmHg"}
Partition:0     P003    {"patient_id": "P003", "event_time": 1791456405512, "vital_type": "HR", "value": 100.0, "unit": "bpm"}
Partition:0     P004    {"patient_id": "P004", "event_time": 1791456405512, "vital_type": "SPO2", "value": 96.1, "unit": "%"}
Partition:0     P005    {"patient_id": "P005", "event_time": 1791456405512, "vital_type": "SPO2", "value": 91.0, "unit": "%"}
```

### 4.5 Dépendance conditionnelle — expérience

Forme courte (`depends_on: [kafka]`) :

```
vitals-producer  | %3|1791462027.564|FAIL|vitals-simulator-s1#producer-1| [thrd:kafka:9092/bootstrap]: kafka:9092/bootstrap: Connect to ipv4#172.19.0.2:9092 failed: Connection refused (after 0ms in state CONNECT)
vitals-producer  | %3|1791462028.063|FAIL|vitals-simulator-s1#producer-1| [thrd:kafka:9092/bootstrap]: kafka:9092/bootstrap: Connect to ipv4#172.19.0.2:9092 failed: Connection refused (after 0ms in state CONNECT, 1 identical error(s) suppressed)
vitals-producer  | %4|1791462033.580|GETPID|vitals-simulator-s1#producer-1| [thrd:main]: Failed to acquire idempotence PID from broker kafka:9092/1: Broker: Coordinator load in progress: retrying
```

Avec `condition: service_healthy` :

```
vitals-producer  | %4|1791462085.399|GETPID|vitals-simulator-s1#producer-1| [thrd:main]: Failed to acquire idempotence PID from broker kafka:9092/1: Broker: Coordinator load in progress: retrying
```

### Mesure 6

| Forme de `depends_on`        | Le producteur affiche-t-il des erreurs de connexion au démarrage ? | Nombre de lignes d'erreur avant stabilisation |
| ---------------------------- | ------------------------------------------------------------------ | --------------------------------------------- |
| `depends_on: [kafka]`        | `oui`                                                              | `1`                                           |
| `condition: service_healthy` | `no`                                                               | `0    `                                       |

## Partie 5 — Vérification et mesure

### Mesure 7 — Débit mesuré

```
docker compose exec kafka kafka-get-offsets --bootstrap-server localhost:9092 \
  --topic mimic-vitals --time -1
sleep 60
docker compose exec kafka kafka-get-offsets --bootstrap-server localhost:9092 \
  --topic mimic-vitals --time -1
```

> mimic-vitals:0:4313
> mimic-vitals:0:5057

| Grandeur                                             | Valeur       |
| ---------------------------------------------------- | ------------ |
| Somme des offsets de fin avant (t0)                  | `4313`       |
| Somme des offsets de fin après 60 s (t1)             | `5057`       |
| Débit calculé `(t1 - t0) / 60`                       | `12.4` msg/s |
| Débit annoncé par le producteur à l'arrêt            | `12.0` msg/s |
| Valeur de `INTERVAL_S` et de `NB_PATIENTS` utilisées | `1 et 12`    |

### Question 5.1

> débit calculé : 12
> débit mesuré : 12.4
> écart : (12.4 - 12) / 12 * 100 = 3.33 %

Mesure de 60 secondes avec `INTERVAL_S=0.1` :

> mimic-vitals:0:0
> mimic-vitals:0:7272
> taux : 7272 / 60 = 121.2 msg/s
>
> théorique : 1 / 0.1 * 12 = 120 msg/s
> écart : (121.2 - 120) / 120 * 100 = 1.0 %

| Grandeur                           | Valeur        |
| ---------------------------------- | ------------- |
| Débit mesuré avec `INTERVAL_S=0.1` | `121.2` msg/s |
| Écart avec la valeur théorique     | `1.00%`       |

### Mesure 8 — Coupure du broker pendant la production

```
kafka:9092/1: Disconnected (after 65144ms in state UP)
kafka:9092/1: Failed to resolve 'kafka:9092': Temporary failure in name resolution (after 1ms in state CONNECT)
kafka:9092/1: Failed to resolve 'kafka:9092': Temporary failure in name resolution (after 2ms in state CONNECT, 1 identical error(s) suppressed)
```

| Observation                                                         | Valeur relevée                                         |
| ------------------------------------------------------------------- | ------------------------------------------------------ |
| Le processus producteur s'arrête-t-il ?                             | `non`                                                  |
| Message d'erreur exact affiché (première ligne)                     | kafka:9092/1: Disconnected (after 65144ms in state UP) |
| Délai entre `stop` et la première erreur                            | `presque 0` s                                          |
| Après `start`, la production reprend-elle sans relancer le script ? | `oui`                                                  |
| Des messages produits pendant la coupure ont-ils été perdus ?       | `non`                                                  |

### Question 5.2

> retries : 5, acks : all, enable.idempotence : True.
> Le producer remarque que la conection TCP tombe et stocke les messages dans un buffer. À la reconnection il renvoi les messages en attentes.
> Ici avec ack le producer sait si la donnée a été reçu, avec retries il réessaie d'envoyer les messages en erreur, et avec enable.idempotence il garantie l'ordre d'insertion et empèche les doublons.
> Ce qui limite la durée pendant laquelle il peut tenir sans broker est le paramètre message.timeout.ms

## Nettoyage

### Question 6.1

> le -v supprime les volumes gérés par docker, ici kafka-logs. Au prochain up le volume sera recréé et le broker recommencera avec des données vides.
> Sans le -v le volume aurait été conservé, et le broker aurait repris les données précédentes.