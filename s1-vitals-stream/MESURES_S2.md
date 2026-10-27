# Mesures — séance 2

## Vérification des prérequis

| Contrôle                                                                                   | Valeur relevée                                                                 |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| Sortie de `docker compose config --quiet`                                                  | `compose OK` (aucune erreur)                                                   |
| Le volume `kafka-data` existe-t-il encore (`docker volume ls`) ?                           | `Non` (aucun volume `kafka-*` ni `vitals-*` dans la liste ; le mien s'appelle `kafka-logs`) |
| Version de PostgreSQL déjà installée nativement sur le poste (`psql --version`)            | `psql (PostgreSQL) 16.15`                                                      |

## Partie 1 — Reprise de l'artefact S1 et passage à plusieurs partitions

### Mesure 1

| Grandeur                                               | Valeur relevée |
| ------------------------------------------------------ | -------------- |
| Nombre de partitions de `mimic-vitals` avant modification | `3`         |
| Somme des offsets de fin (`$KO --topic "$TOPIC" --time -1`) | `0` (topic recréé, vide : `0:0`, `1:0`, `2:0`) |

### Mesure 2

| Partition | Patients observés                          | Nombre de patients |
| --------- | ------------------------------------------ | ------------------ |
| 0         | `P003, P005, P008, P009, P010, P012`       | `6`                |
| 1         | `P002, P006, P007, P011`                   | `4`                |
| 2         | `P001, P004`                               | `2`                |

### Décision 1.3

`NB_PARTITIONS=6`

### Mesure 3

| Partition | Patients observés (messages récents) | Nombre de patients |
| --------- | ------------------------------------ | ------------------ |
| 0         | `P005, P010, P012`                   | `3`                |
| 1         | `P007`                               | `1`                |
| 2         | (aucun)                              | `0`                |
| 3         | `P003, P008, P009`                   | `3`                |
| 4         | `P002, P006, P011`                   | `3`                |
| 5         | `P001, P004`                         | `2`                |

### Question 1.1

> P005, P007, P010 et P012 sont restés dans les mêmes partitions. La formule redistribue les patients selon le reste du hash modulo le nombre de partitions. Ainsi, si le nombre de partitions augmente, le patient ne bouge pas si le reste est le même. Par exemple 14 % 3 = 2 et 14 % 6 = 2 : avec un hash de 14, il n'y aurait aucun changement.

### Question 1.2

> Non, la répartition n'est pas uniforme : la partition 2 ne reçoit aucun patient. Le hash envoie chaque clé vers une partition au hasard (mais de façon fixe), il peut donc y avoir un déséquilibre par hasard. Avec plus de clés, le hasard égaliserait les écarts (l'écart type diminue proportionnellement), mais 12 clés sont trop peu. En partie 3, le consommateur qui aura la partition 2 sera presque inactif, et ceux qui auront des partitions à 3 patients seront plus chargés.

### Question 1.3

> La garantie d'ordre des messages d'une même clé est cassée, car Kafka ne déplace pas les messages existants : les anciens messages de `P001` restent dans l'ancienne partition et les nouveaux vont dans une autre. Cela dure de la modification jusqu'à ce que les anciens messages disparaissent (rétention). Pour conserver l'ordre, il faudrait créer un nouveau topic et y recopier les messages existants.

## Partie 2 — Premier consommateur : offsets et acquittement

### Mesure 4

| Groupe          | `auto.offset.reset` | Messages lus au 1ᵉʳ lancement (20 s) | Messages lus au 2ᵉ lancement (20 s) |
| --------------- | ------------------- | ------------------------------------ | ----------------------------------- |
| `explo-earliest` | `earliest`         | `7033`                               | `252`                               |
| `explo-latest`  | `latest`            | `241`                                | `252`                               |

### Mesure 5

Relevé du `--describe` sur `explo-earliest`, juste après un arrêt propre (partition 0) :

| Colonne           | Valeur relevée (partition 0) |
| ----------------- | ---------------------------- |
| `CURRENT-OFFSET`  | `1866`                       |
| `LOG-END-OFFSET`  | `1872`                       |
| `LAG`             | `6`                          |
| `CONSUMER-ID` affiché (ou `-`) | `-`             |

### Question 2.1

> Le deuxième lancement de `explo-earliest` n'a pas relu les messages du premier grâce au marque-page (offset) commité par le premier lancement au moment de son arrêt propre. `earliest` ne correspond qu'au comportement quand il n'y a pas de marque-page : le premier lancement en a créé un à son arrêt, donc `earliest` n'a pas été réutilisé au deuxième lancement.

### Question 2.2

> Première façon : supprimer le marque-page du groupe existant ou le déplacer à 0, puis relancer le consommateur depuis le début. Deuxième façon : créer un groupe neuf (`test-v2`) avec `auto.offset.reset=earliest`, qui part du début de l'historique. En production je choisirais le groupe neuf, car cela ajoute simplement un nouveau traitement sans modifier les existants.

### Mesure 6

| Groupe          | Mode                  | Messages lus avant le kill (dernier `lus=` affiché) | `CURRENT-OFFSET` total après le kill | Messages relus au redémarrage |
| --------------- | --------------------- | --------------------------------------------------- | ------------------------------------ | ----------------------------- |
| `explo-auto`    | automatique           | `28050`                                             | `28020`                              | `30` (calculé : 28050 − 28020, à ±50 près) |
| `explo-manuel`  | manuel (tous les 100) | `28050`                                             | `28000`                              | `50` (calculé : 28050 − 28000, à ±50 près) |

*Les groupes n'ont pas été relancés après le kill : les messages relus sont déduits de l'écart entre les messages lus et l'offset commité.*

### Question 2.3

> En mode automatique, on risque de perdre un message si l'offset est commité dans Kafka alors que le message n'a pas réellement été inséré en base de données. Il vaut donc mieux utiliser le mode manuel, qui permet de noter ce qui a réellement été traité. Le risque est alors d'avoir à traiter la même donnée deux fois en cas d'erreur entre l'insertion et le commit.

## Partie 3 — Consumer groups et rééquilibrage

### Mesure 7

| Situation        | Partitions de `cA` | Partitions de `cB` | Partitions de `cC` |
| ---------------- | ------------------ | ------------------ | ------------------ |
| `cA` seul        | `0, 1, 2, 3, 4, 5` | —                  | —                  |
| `cA` + `cB`      | `0, 1, 2`          | `3, 4, 5`          | —                  |
| `cA` + `cB` + `cC` | `0, 1`           | `2, 3`             | `4, 5`             |

### Mesure 8

| Instance | Messages lus pendant les 60 s à trois (différence des `lus=`) | Patients couverts (déduits de la mesure 3) |
| -------- | ------------------------------------------------------------- | ------------------------------------------ |
| `cA`     | `265` (505 − 240)                                             | `4` (P005, P010, P012, P007)               |
| `cB`     | `204` (324 − 120)                                             | `3` (P003, P008, P009)                     |
| `cC`     | `372` (372 − 0)                                               | `5` (P002, P006, P011, P001, P004)         |

### Question 3.1

> Non : le nombre de messages lus par instance dépend du nombre de messages reçus par instance, et la répartition des patients étant aléatoire, il est normal d'avoir un écart selon l'instance. Dans la mesure 3, les partitions 0 et 1 contiennent 4 patients, les partitions 2 et 3 en contiennent 3, et les partitions 4 et 5 en contiennent 5, ce qui correspond à l'ordre des messages lus par `cA`, `cB` et `cC`.

### Mesure 9

| Grandeur                                    | Valeur relevée                 |
| ------------------------------------------- | ------------------------------ |
| Nombre de partitions du topic               | `6`                            |
| Nombre d'instances lancées                  | `7`                            |
| Nombre d'instances affichant `partitions=[aucune]` | `1` (`cG`)              |
| `lus=` de ces instances après 30 s          | `0`                            |

### Question 3.2

> La borne de parallélisme est 6 pour nous, puisque nous avons 6 partitions. Il faudrait augmenter le nombre de partitions pour augmenter le parallélisme. Cette modification aurait pour effet de réorganiser les patients dans les partitions et de perdre la continuité des données.

### Mesure 10

| Scénario                              | Délai avant réattribution à `cA` | Valeur de `session.timeout.ms` par défaut du client |
| ------------------------------------- | -------------------------------- | --------------------------------------------------- |
| A — `Ctrl-C` (arrêt propre, `close()` appelé) | `3` s (13:51:47 → 13:51:50) | 45 000 ms                                           |
| B — `kill -9` (arrêt brutal)          | `44` s (13:52:15 → 13:52:59)     | idem                                                |

*Arrêt envoyé par `SIGTERM` (équivalent du `Ctrl-C` : le script appelle `close()`), et `SIGSTOP` suivi de `SIGKILL` pour l'arrêt brutal. Précision de mesure : ±1 s.*

### Question 3.3

> Cet écart s'explique par le mécanisme de heartbeat de Kafka. Kafka ne sait qu'un consommateur est mort pour de bon qu'après un certain délai paramétré : c'est instantané dans le cas d'un arrêt propre, car le processus indique directement « je pars ». Sans cela, Kafka doit le deviner : le consommateur envoie des heartbeats, et Kafka le déclare mort quand il n'en reçoit plus pendant `session.timeout.ms`. On ne devrait pas mettre ce timeout à 1 seconde pour éviter de déclarer mort un service qui est momentanément ralenti. Le risque est d'avoir une entrée demandant un traitement particulièrement long : s'il est plus long que le timeout, Kafka déclarera le processus mort alors qu'il tourne toujours, le forçant à abandonner son traitement, qui sera donné au prochain consommateur et qui aura nécessairement le même problème.

### Mesure 11

| Stratégie            | Lignes `RETRAIT` affichées par `cA` lors du départ de `cB` | Partitions listées dans ce `RETRAIT` |
| -------------------- | ---------------------------------------------------------- | ------------------------------------ |
| `range`              | `1`                                                        | `0, 1, 2` (toutes celles de `cA`)    |
| `cooperative-sticky` | `0`                                                        | — (`cA` reçoit seulement `2, 1, 0` en `ATTRIBUTION`, ses partitions `3, 4, 5` ne sont jamais retirées) |

### Question 3.4

> Avec la stratégie `range` (eager), le consommateur est dérangé dans son travail : il doit rendre le traitement de ses partitions temporairement, le temps que Kafka les lui réattribue, ce qui crée une période de pause. Avec la stratégie `cooperative-sticky`, le consommateur n'est pas dérangé : il garde ses partitions et se voit simplement attribuer les partitions supplémentaires pour ses prochains traitements.

**Stratégie retenue dans le code de rendu : `cooperative-sticky`** (valeur par défaut de `ASSIGNMENT_STRATEGY` dans `consumer/lecteur.py`).

## Partie 4 — Persistance PostgreSQL

### Question 4.1

> La clé `(kafka_partition, kafka_offset)` identifie de façon unique un message dans Kafka : si le même message est rejoué après un plantage, il a les mêmes coordonnées, donc la base refuse le doublon. Avec `(patient_id, event_time, vital_type)`, deux relevés distincts du même patient, du même type et dans la même milliseconde auraient la même clé, et le second serait vu comme un doublon alors qu'en réalité il est différent. Il y aurait une perte de donnée.

### Mesure 12

| Grandeur                                                       | Valeur relevée |
| -------------------------------------------------------------- | -------------- |
| Délai entre `up -d postgres` et `healthy`                      | `5` s          |
| Nombre de tables listées par `\dt`                             | `1` (`vitals_raw`) |
| Nom de la contrainte de clé primaire affichée par `\d vitals_raw` | `pk_vitals_raw` |

### Décisions 4.3

| Décision                              | Valeur retenue                                                   |
| ------------------------------------- | ---------------------------------------------------------------- |
| Clause de conflit de l'insertion      | `ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING`             |
| Mode d'acquittement                   | `enable.auto.commit = False` (commit manuel après l'insertion)   |

### Mesure 13

| Grandeur                                          | Valeur relevée |
| ------------------------------------------------- | -------------- |
| `lus=` affiché par le consommateur à l'arrêt      | `65449`        |
| `inseres=` affiché à l'arrêt                      | `65449`        |
| `lignes_totales` de `v_vitals_controle`           | `65449`        |
| `messages_distincts` de `v_vitals_controle`       | `65449`        |
| `patients_distincts`                              | `12`           |

### Question 4.2

> `lignes_totales` et `messages_distincts` sont égaux. S'ils ne l'étaient pas, j'aurais conclu que la clé primaire n'était pas respectée ou correctement implémentée.

### Mesure 14

| Grandeur                                                              | Valeur relevée |
| --------------------------------------------------------------------- | -------------- |
| Nombre de services affichés par `docker compose ps`                   | `4` (`kafka`, `postgres`, `producer`, `consumer`) |
| Services en état `healthy`                                            | `3` (`kafka`, `postgres`, `producer` ; `consumer` n'a pas de healthcheck, il est `Up`) |
| Taille moyenne des lots dans les logs du consommateur (`LOT taille=`) | `68` (11 lots en 60 s) |
| Croissance de `lignes_totales` sur 60 s                               | `688` lignes   |

### Question 4.3

> En 5 secondes à 12 messages par seconde il y a 5 × 12 = 60 messages générés. Cela correspond à peu près aux 68 messages mesurés. La contrainte de temps des 5 secondes se déclenche donc avant celle des 200 messages dans ma configuration.

## Partie 5 — Résilience et mesure

### Mesure 15

| Instant   | Somme des `LAG` sur toutes les partitions |
| --------- | ----------------------------------------- |
| t0        | `23`                                      |
| t0 + 30 s | `0`                                       |
| t0 + 60 s | `47`                                      |

### Question 5.1

> Le lag mesuré est stable entre 0 et 47 sans croître. On s'attend à un lag entre 0 et 60 messages (timeout × débit, 5 s × 12 msg/s).

### Mesure 16

Réglages utilisés : `INTERVAL_S=0.005`, `BATCH_SIZE=1`, consommateur bridé à 5 % d'un cœur.

| Instant    | Somme des `LAG` | Écart avec le relevé précédent |
| ---------- | --------------- | ------------------------------ |
| t0         | `13082`         | —                              |
| t0 + 30 s  | `80027`         | `+66945`                       |
| t0 + 60 s  | `146256`        | `+66229`                       |
| t0 + 120 s | `275593`        | `+129337`                      |

### Mesure 17

| Grandeur                                                           | Valeur relevée |
| ------------------------------------------------------------------ | -------------- |
| Somme des `LAG` juste avant le `--scale`                           | `278312`       |
| Somme des `LAG` 60 s après                                         | `414346`       |
| Nombre de membres du groupe affichés par `--describe`              | `2`            |
| Le lag décroît-il, se stabilise-t-il ou continue-t-il de croître ? | `continue de croître` (476739 à +90 s) |

### Question 5.2

> Il faudrait examiner le débit du producteur, le temps de traitement par message, puis les ressources du consommateur. Ajouter des consommateurs accélère ces deux derniers points, mais on ne peut rien faire par rapport au débit de données entrant, qui dépend du producteur (sauf bien sûr optimiser le producteur lui-même pour réduire la charge reçue par Kafka).

### Mesure 18

| Observation                                                   | Valeur relevée |
| ------------------------------------------------------------- | -------------- |
| Le conteneur consommateur s'arrête-t-il ou redémarre-t-il ?   | `Il redémarre` : après 30 tentatives de connexion (~60 s) il lève `RuntimeError: postgres injoignable` et s'arrête, puis `restart: unless-stopped` le relance (`RestartCount=1`) |
| Première ligne d'erreur affichée (début de la ligne)          | `postgres indisponible (1/30): could not translate host name "postgres" to address…` |
| Somme des `LAG` après 90 s de coupure                         | `1127`         |
| `lignes_totales` avant la coupure                             | `635267`       |
| `lignes_totales` 60 s après le redémarrage de PostgreSQL      | `637297`       |
| Somme des `LAG` 60 s après le redémarrage                     | `59`           |
| Le consommateur a-t-il rattrapé son retard sans intervention ? | `Oui`         |

### Question 5.3

> Les offsets n'ont pas avancé pendant la coupure, le lag a donc augmenté (le producteur produit de nouvelles données mais aucun consommateur ne les a gérées). C'est parce que `inserer_lot()` est exécuté avant `commit()` : si l'insertion échoue, le commit n'est pas exécuté et le curseur n'avance pas. Dans l'autre sens, le curseur aurait avancé mais les messages n'auraient pas été écrits en base, et ils auraient été perdus.

### Mesure 19

| Grandeur                                                           | Valeur relevée |
| ------------------------------------------------------------------ | -------------- |
| `lignes_totales` avant le `SIGKILL`                                | `639865`       |
| `inseres=` de l'instance relancée après 60 s (ligne `Arret.`)      | `756`          |
| `ignores_doublons=` de l'instance relancée (ligne `Arret.`)        | `0`            |
| Nombre de lignes renvoyées par la requête de doublons              | `0`            |
| Partitions où `lignes` ≠ `etendue` (donc avec un trou)             | `aucune`       |

### Question 5.4

> `ignores_doublons` est nul : le `SIGKILL` est arrivé entre deux lots, donc aucun message n'a été relu. S'il avait été non nul, il y aurait eu des doublons entre le lot inséré en base et ceux pas encore commités, et au redémarrage la clé primaire les aurait refusés (`DO NOTHING`). Cela prouve que le mécanisme d'idempotence des données fonctionne.

### Question 5.5

> Ma chaîne garantit *at-least-once* : un message peut être livré plusieurs fois après un crash mais n'est jamais perdu. Dans la table, l'état final est *exactly-once*, car la clé primaire absorbe les doublons.

## Nettoyage

### Question 6.1

> Les offsets disparaissent avec le volume Kafka et les données avec le volume PostgreSQL. Au prochain `up`, le topic et les offsets n'existeront plus. Avec `AUTO_OFFSET_RESET=earliest`, le consommateur repartirait du début de ce qui existe (si le producteur continue à générer des données, sinon rien ne se passe).

