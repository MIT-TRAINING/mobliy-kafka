# Lab 02 — Topic Administration & CLI Produce/Consume

| | |
| --- | --- |
| **Level** | Beginner → Intermediate |
| **Duration** | ~75 minutes |
| **Guide sections** | §7 CLI overview · §8 Topic management · §9 Producing and consuming · §10 End-to-end walkthrough · §11 Troubleshooting |
| **You will need** | The 3-node cluster from Lab 01 running (`docker compose ps` shows 3 × healthy), two terminals |

## Learning objectives

By the end of this lab you will be able to:

1. **Create** topics with an explicit partition count, replication factor and
   configs, and predict the errors an admin most often hits.
2. **List** and **describe** topics, and read leader / replica / ISR placement
   and the three health filters.
3. **Alter** topics safely: add partitions, change configs with
   `kafka-configs.sh`, and know what cannot be changed.
4. **Produce** and **consume** from the CLI with keys, headers, config files,
   exact partitions and offsets, and consumer groups.
5. Explain what happens to leaders, the ISR and writes when a broker goes down,
   and how `acks` and `min.insync.replicas` decide whether a write is accepted.
6. **Delete** topics and know why deletion needs care.

> **Kafka 4.x CLI flags.** The console tools now use
> `--command-property` / `--command-config` for client settings and
> `--reader-property` / `--formatter-property` for input/output formatting. The
> older `--producer-property`, `--consumer-property`, `--producer.config` and
> `--property` still work in 4.3 but print a deprecation warning. Older
> tutorials and parts of the guide use the old names.

Unless a block says otherwise, run everything **inside kafka-1**:

```bash
# (host) - from labs/module-02
docker compose ps                  # 3 x (healthy)
docker exec -it kafka-1 bash
```

---

## Part 1 — Create topics (15 min)

### 1.1 A production-style topic

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create \
  --topic cdr.voice \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000
```

```
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic cdr.voice.
```

The warning appears for every name containing `.` or `_`. You will see why in 1.3.

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.voice
```

```
Topic: cdr.voice  TopicId: krUx4y6zR3-3xN2gVD8abg  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
    Topic: cdr.voice  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  Elr:   LastKnownElr:
    Topic: cdr.voice  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2  Elr:   LastKnownElr:
    Topic: cdr.voice  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
    Topic: cdr.voice  Partition: 3  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
    Topic: cdr.voice  Partition: 4  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  Elr:   LastKnownElr:
    Topic: cdr.voice  Partition: 5  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2  Elr:   LastKnownElr:
```

| Column | Meaning |
| ------ | ------- |
| `Leader` | The broker serving reads and writes for this partition right now |
| `Replicas` | Every broker holding a copy. The **first** one is the *preferred* leader |
| `Isr` | In-sync replicas: copies that are fully caught up |
| `Elr` / `LastKnownElr` | *Eligible leader replicas* (new in Kafka 4.x): replicas that left the ISR but are still safe to elect. Empty while healthy |

Count leaders per broker:

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.voice | grep -o "Leader: [0-9]" | sort | uniq -c
```

```
      2 Leader: 1
      2 Leader: 2
      2 Leader: 3
```

The controller spreads leadership evenly, so every broker shares the work.
`min.insync.replicas=2` appears even though you did not set it. It is the
cluster default from the Compose file (`KAFKA_MIN_INSYNC_REPLICAS`).

### 1.2 Let the cluster defaults decide

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic sms.events
kafka-topics.sh --bootstrap-server $BS --describe --topic sms.events
```

```
Topic: sms.events  ...  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2
```

With no flags, the broker defaults `num.partitions=3` and
`default.replication.factor=3` apply. In production, always pass both
explicitly so the result does not depend on how a cluster happens to be
configured.

### 1.3 Four mistakes worth making once

```bash
# (container)
# a) More replicas than brokers
kafka-topics.sh --bootstrap-server $BS --create --topic cdr.data --partitions 3 --replication-factor 4

# b) The topic already exists  (then the safe, repeatable way)
kafka-topics.sh --bootstrap-server $BS --create --topic cdr.voice --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --create --topic cdr.voice --partitions 6 --replication-factor 3 --if-not-exists; echo "exit code $?"

# c) A name that differs only by '.' vs '_'
kafka-topics.sh --bootstrap-server $BS --create --topic sms_events
```

```
a) Error while executing topic command : Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be reached because only 3 broker(s) are registered or some brokers have all their log directories cordoned.
b) Error while executing topic command : Topic 'cdr.voice' already exists.
   exit code 0
c) Error while executing topic command : Topic 'sms_events' collides with existing topic: sms.events
```

Kafka metric names replace `.` with `_`, so `sms.events` and `sms_events`
would report under the same metric name. Kafka refuses the second one. Pick one
naming style per cluster.

```bash
# (container)
# d) Produce to a topic that does not exist
echo "x:y" | kafka-console-producer.sh --bootstrap-server $BS --topic cdr.typo \
  --command-property max.block.ms=5000
```

```
WARN [Producer clientId=console-producer] The metadata response from the cluster reported a recoverable issue with correlation id 2 : {cdr.typo=UNKNOWN_TOPIC_OR_PARTITION}
...
ERROR Error when sending message to topic cdr.typo with key: null, value: 3 bytes with error:
org.apache.kafka.common.errors.TimeoutException: Topic cdr.typo not present in metadata after 5000 ms.
```

This cluster has `auto.create.topics.enable=false` (set in the Compose file).
Without it, the typo would have silently created a new topic, `cdr.typo`, with
default settings. Most production clusters disable auto-creation for exactly
this reason.

### 1.4 Place replicas by hand (stretch)

You can choose the brokers for each partition yourself. The format is one
`leader:follower...` list per partition:

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic mms.events --replica-assignment 1:2,2:3,3:1
kafka-topics.sh --bootstrap-server $BS --describe --topic mms.events
kafka-topics.sh --bootstrap-server $BS --delete --topic mms.events
```

```
Topic: mms.events  ...  PartitionCount: 3  ReplicationFactor: 2  ...
    Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
    Partition: 1  Leader: 2  Replicas: 2,3  Isr: 2,3
    Partition: 2  Leader: 3  Replicas: 3,1  Isr: 3,1
```

Manual placement is rarely used when creating topics. The same idea powers
`kafka-reassign-partitions.sh` for moving data between brokers (Module 5).

---

## Part 2 — List, describe and health checks (10 min)

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --list
kafka-topics.sh --bootstrap-server $BS --list --exclude-internal
kafka-topics.sh --bootstrap-server $BS --describe --topic "cdr.*"         # --topic accepts a regex
kafka-topics.sh --bootstrap-server $BS --describe --topics-with-overrides  # topic line only, no partitions
```

`__consumer_offsets` appears in the first list only after a consumer group has
committed offsets (Part 4). `--exclude-internal` hides it.

The three **health filters** are the first thing to run when you are paged
about a Kafka problem (guide §8.3):

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions
```

On a healthy cluster all three print **nothing**. Nothing printed is the good
result. You will make them print something in Part 5.

| Filter | Lists partitions where | Risk |
| ------ | ---------------------- | ---- |
| `--under-replicated-partitions` | ISR is smaller than the replica list | Less redundancy: one more failure could lose data |
| `--under-min-isr-partitions` | ISR is smaller than `min.insync.replicas` | Producers using `acks=all` are **rejected** |
| `--unavailable-partitions` | No leader at all | Nobody can read or write |

---

## Part 3 — Alter topics (10 min)

### 3.1 Partitions: up only

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --alter --topic sms.events --partitions 6
kafka-topics.sh --bootstrap-server $BS --describe --topic sms.events | head -1

kafka-topics.sh --bootstrap-server $BS --alter --topic sms.events --partitions 4
```

```
Topic: sms.events  ...  PartitionCount: 6  ...

Error while executing topic command : The topic sms.events currently has 6 partition(s); 4 would not be an increase.
```

Adding partitions changes `hash(key) % partitions`, so **new** records for a
key may go to a different partition than the old ones. Per-key ordering
breaks across that boundary. Removing partitions would lose data, so Kafka does
not allow it. Plan partition counts up front.

### 3.2 Configs: use `kafka-configs.sh`

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --alter --topic sms.events --config retention.ms=86400000
```

```
Option combination "[[bootstrap-server], [config]]" can't be used with option "[alter]" (To alter topic configurations, the kafka-configs tool can be used.)
```

In Kafka 4.x, topic configs are changed **only** with `kafka-configs.sh`. (The
guide's §8.4 example uses the old way.)

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name sms.events \
  --add-config retention.ms=86400000,max.message.bytes=2097152

kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name sms.events
```

```
Completed updating config for topic sms.events.
Dynamic configs for topic sms.events are:
  max.message.bytes=2097152 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:max.message.bytes=2097152, DEFAULT_CONFIG:message.max.bytes=1048588}
  retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=86400000}
```

The change is live immediately. No restart. `--topic NAME` is a shortcut for
`--entity-type topics --entity-name NAME`. See where **every** value comes from:

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --describe --topic sms.events --all \
  | grep -E "^  (retention.ms|cleanup.policy|min.insync.replicas|segment.bytes)="
```

```
  cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}
  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2, STATIC_BROKER_CONFIG:min.insync.replicas=2, DEFAULT_CONFIG:min.insync.replicas=1}
  retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=86400000}
  segment.bytes=1073741824 sensitive=false synonyms={DEFAULT_CONFIG:log.segment.bytes=1073741824}
```

`synonyms` lists every level that sets a value, highest priority first:
**topic** → cluster-wide dynamic default → broker's `server.properties` →
Kafka built-in default. Module 3 works with these levels in depth.

Remove an override (the topic falls back to the next level), and try an
invalid value:

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --alter --topic sms.events --delete-config max.message.bytes
kafka-configs.sh --bootstrap-server $BS --describe --topic sms.events
kafka-configs.sh --bootstrap-server $BS --alter --topic sms.events --add-config retention.ms=forever
```

```
Dynamic configs for topic sms.events are:
  retention.ms=86400000 ...

... InvalidConfigurationException: Invalid value forever for configuration retention.ms: Not a number of type LONG
```

| Change | Tool | Possible? |
| ------ | ---- | --------- |
| Add partitions | `kafka-topics.sh --alter --partitions` | Yes, up only |
| Remove partitions | — | No |
| Topic configs | `kafka-configs.sh --alter --add-config / --delete-config` | Yes, live |
| Replication factor | `kafka-reassign-partitions.sh` | Yes, but it is a data move (Module 5) |
| Rename a topic | — | No: create a new one and copy the data |

---

## Part 4 — Produce and consume from the CLI (20 min)

### 4.1 Keyed messages from a file

Scripted input is repeatable, which matters in real operations work:

```bash
# (container)
cat > /tmp/calls.txt <<EOF
966500000001:call-start
966500000002:call-start
966500000003:call-start
966500000001:call-end
966500000004:call-start
966500000002:call-end
966500000003:call-end
966500000004:call-end
EOF

kafka-console-producer.sh --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=all < /tmp/calls.txt

kafka-get-offsets.sh --bootstrap-server $BS --topic cdr.voice
```

```
cdr.voice:0:0
cdr.voice:1:2
cdr.voice:2:4
cdr.voice:3:2
cdr.voice:4:0
cdr.voice:5:0
```

`kafka-get-offsets.sh` prints `topic:partition:next-offset`, which is the
quickest way to see where data landed without consuming it.

```bash
# (container)
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice --from-beginning \
  --formatter-property print.partition=true --formatter-property print.offset=true \
  --formatter-property print.key=true --timeout-ms 5000
```

```
Partition:2  Offset:0  966500000001  call-start
Partition:2  Offset:1  966500000002  call-start
Partition:2  Offset:2  966500000001  call-end
Partition:2  Offset:3  966500000002  call-end
Partition:1  Offset:0  966500000004  call-start
Partition:1  Offset:1  966500000004  call-end
Partition:3  Offset:0  966500000003  call-start
Partition:3  Offset:1  966500000003  call-end
Processed a total of 8 messages
```

(A `TimeoutException` after the last line is just `--timeout-ms` ending the run.)

- Each subscriber's `call-start` comes before its `call-end`: ordering per
  **key** holds, because a key always maps to the same partition.
- Output is grouped by partition, not in the order you sent it. There is **no**
  ordering across partitions.
- Six partitions, but only three used: four keys are too few to spread evenly.

### 4.2 Headers and a producer config file

Headers carry metadata next to the value (source system, trace ID, etc.):

```bash
# (container)
echo "switch:msc01,region:central|966500000005|call-start" | kafka-console-producer.sh \
  --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.headers=true --reader-property parse.key=true \
  --reader-property key.separator="|" --reader-property headers.delimiter="|"
```

Real producer settings belong in a file, as they would in an application:

```bash
# (container)
cat > /tmp/producer.properties <<EOF
acks=all
enable.idempotence=true
compression.type=lz4
linger.ms=20
client.id=cdr-loader
EOF

printf "966500000006:call-start\n966500000006:call-end\n" | kafka-console-producer.sh \
  --bootstrap-server $BS --topic cdr.voice --command-config /tmp/producer.properties \
  --reader-property parse.key=true --reader-property key.separator=:

kafka-get-offsets.sh --bootstrap-server $BS --topic cdr.voice
```

```
cdr.voice:0:0
cdr.voice:1:2
cdr.voice:2:4
cdr.voice:3:2
cdr.voice:4:1
cdr.voice:5:2
```

`--bootstrap-server` is still required, even with a config file.

### 4.3 Read exactly what you want

```bash
# (container)
# One partition, from the start, with headers and timestamps
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice \
  --partition 4 --offset earliest --max-messages 1 \
  --formatter-property print.key=true --formatter-property print.headers=true \
  --formatter-property print.timestamp=true

# Two records starting at a given offset
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice \
  --partition 2 --offset 2 --max-messages 2 \
  --formatter-property print.offset=true --formatter-property print.key=true
```

```
CreateTime:1790652613997  switch:msc01,region:central  966500000005  call-start
Processed a total of 1 messages

Offset:2  966500000001  call-end
Offset:3  966500000002  call-end
Processed a total of 2 messages
```

`--partition` with `--offset` is how you inspect one suspicious record when
investigating an incident. It does not join a consumer group, so it changes
nothing for real consumers.

### 4.4 Consumer groups and lag

Read everything as group `billing`, then check its position:

```bash
# (container)
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice \
  --group billing --from-beginning --timeout-ms 5000

kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
```

```
Consumer group 'billing' has no active members.

GROUP    TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
billing  cdr.voice  0          0               0               0    -            -     -
billing  cdr.voice  1          2               2               0    -            -     -
billing  cdr.voice  2          4               4               0    -            -     -
billing  cdr.voice  3          2               2               0    -            -     -
billing  cdr.voice  4          1               1               0    -            -     -
billing  cdr.voice  5          2               2               0    -            -     -
```

Produce three more records **while nobody is consuming**, then check again:

```bash
# (container)
printf "966500000001:call-start\n966500000003:call-start\n966500000005:call-start\n" | \
  kafka-console-producer.sh --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.key=true --reader-property key.separator=:

kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
```

```
GROUP    TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
billing  cdr.voice  0          0               0               0
billing  cdr.voice  1          2               2               0
billing  cdr.voice  2          4               5               1
billing  cdr.voice  3          2               3               1
billing  cdr.voice  4          1               2               1
billing  cdr.voice  5          2               2               0
```

`LAG = LOG-END-OFFSET − CURRENT-OFFSET`: three records are waiting. Run the
consumer again. It still has `--from-beginning`, but it reads **only the three
new records**, because the group already has committed offsets:

```bash
# (container)
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice \
  --group billing --from-beginning --timeout-ms 5000 --formatter-property print.key=true
```

```
966500000001  call-start
966500000003  call-start
966500000005  call-start
Processed a total of 3 messages
```

Look at the groups, and at the internal topic that stores their offsets:

```bash
# (container)
kafka-consumer-groups.sh --bootstrap-server $BS --list
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing --state
kafka-topics.sh --bootstrap-server $BS --describe --topic __consumer_offsets | head -1
```

```
console-consumer-71503
billing

GROUP    COORDINATOR (ID)     ASSIGNMENT-STRATEGY  STATE  #MEMBERS
billing  kafka-3:29092  (3)   -                    Empty  0

Topic: __consumer_offsets  ...  PartitionCount: 50  ReplicationFactor: 3  Configs: compression.type=producer,min.insync.replicas=2,cleanup.policy=compact,segment.bytes=104857600
```

- `console-consumer-NNNNN` is a throwaway group created by a consumer started
  without `--group`.
- Each group has a **coordinator** broker, here kafka-3, which stores the
  group's commits in `__consumer_offsets`. With 3 brokers that topic is RF 3,
  so a broker failure does not lose committed offsets. In Module 1 it was RF 1.

---

## Part 5 — A broker goes down (15 min)

Open a **second terminal** on the host for the `docker compose` commands and
keep the kafka-1 shell in the first.

### 5.1 Stop a broker and read the damage report

```bash
# (host) - terminal 2
docker compose stop kafka-3
```

```bash
# (container) - terminal 1
kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.voice 2>/dev/null
```

```
Topic: cdr.voice  ...  Configs: min.insync.replicas=2,retention.ms=604800000
    Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,1
    Partition: 1  Leader: 1  Replicas: 3,1,2  Isr: 1,2
    Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2
    Partition: 3  Leader: 1  Replicas: 1,2,3  Isr: 1,2
    Partition: 4  Leader: 2  Replicas: 2,3,1  Isr: 2,1
    Partition: 5  Leader: 1  Replicas: 3,1,2  Isr: 1,2
```

(`2>/dev/null` hides the `Couldn't resolve server kafka-3:29092` warning: a
stopped container's DNS name disappears.)

- Partitions 1 and 5 were **led by broker 3**. They now have a new leader,
  elected from the ISR within seconds.
- Broker 3 is gone from every ISR, but it is still listed in `Replicas`: the
  cluster expects it back.

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions 2>/dev/null | wc -l
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions 2>/dev/null
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions 2>/dev/null
```

```
62
```

62 under-replicated partitions (6 of `cdr.voice`, 50 of `__consumer_offsets`,
6 of `sms.events`). None are under min ISR and none are unavailable: ISR 2 still
meets `min.insync.replicas=2`.

### 5.2 Writes keep working with `acks=all`

```bash
# (container)
echo "966500000003:during-outage" | kafka-console-producer.sh --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=all 2>/dev/null
kafka-get-offsets.sh --bootstrap-server $BS --topic-partitions cdr.voice:3 2>/dev/null
```

```
cdr.voice:3:4
```

Accepted: with `acks=all` the leader waits for **every replica in the ISR**
(now 2), and 2 ≥ `min.insync.replicas`. RF 3 + min ISR 2 is the standard
production pair because it survives one broker failure with **no** impact on
producers.

### 5.3 Make the rule stricter than the cluster can meet

Pretend the business demands three confirmed copies of every CDR:

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --alter --topic cdr.voice --add-config min.insync.replicas=3 2>/dev/null
kafka-topics.sh --bootstrap-server $BS --describe --under-min-isr-partitions 2>/dev/null
```

All six `cdr.voice` partitions are now listed. Try an `acks=all` write:

```bash
# (container)
echo "966500000003:strict-write" | kafka-console-producer.sh --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=all --command-property delivery.timeout.ms=10000 \
  --command-property request.timeout.ms=5000 2>&1 | grep -v -i resolve
```

```
WARN [Producer clientId=console-producer] Got error produce response with correlation id 6 on topic-partition cdr.voice-3, retrying (2 attempts left). Error: NOT_ENOUGH_REPLICAS
...
ERROR Error when sending message to topic cdr.voice with key: 12 bytes, value: 12 bytes with error:
org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required.
```

Kafka **refuses** the write rather than accept it with weaker durability than
you configured. The producer retried, then gave up. A real application must
handle this error.

Now write the same record with `acks=1` (the leader alone confirms):

```bash
# (container)
echo "966500000003:leader-only-write" | kafka-console-producer.sh --bootstrap-server $BS --topic cdr.voice \
  --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=1 2>/dev/null

kafka-get-offsets.sh --bootstrap-server $BS --topic-partitions cdr.voice:3 2>/dev/null
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice --partition 3 --offset earliest \
  --formatter-property print.offset=true --timeout-ms 4000 2>/dev/null
```

```
cdr.voice:3:4

Offset:0  call-start
Offset:1  call-end
Offset:2  call-start
Offset:3  during-outage
```

The producer reported **no error**, yet the record is **not visible**: the
offset is still 4 and the consumer stops at offset 3. Is it lost? Look at the
leader's segment file on disk:

```bash
# (container) - kafka-1 is the leader of partition 3 in this example
kafka-dump-log.sh --files /var/lib/kafka/data/cdr.voice-3/00000000000000000000.log --print-data-log \
  | grep -o "offset: [0-9]* .*payload: .*" | sed "s/CreateTime.*payload/payload/"
```

```
offset: 0 payload: call-start
offset: 1 payload: call-end
offset: 2 payload: call-start
offset: 3 payload: during-outage
offset: 4 payload: leader-only-write
```

It **is** in the log. Consumers only see records below the **high watermark**,
and in Kafka 4.x the high watermark does not advance while the ISR is smaller
than `min.insync.replicas`. The record is written but not yet committed.

### 5.4 Bring the broker back

```bash
# (host) - terminal 2
docker compose start kafka-3
```

Wait about 10 seconds, then:

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.voice
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice --partition 3 --offset earliest \
  --formatter-property print.offset=true --timeout-ms 4000
```

```
    Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 1,2,3
    Partition: 1  Leader: 1  Replicas: 3,1,2  Isr: 1,2,3
    ...
    Partition: 5  Leader: 1  Replicas: 3,1,2  Isr: 1,2,3

Offset:0  call-start
...
Offset:3  during-outage
Offset:4  leader-only-write
```

Broker 3 copied what it missed and rejoined every ISR. ISR 3 now meets
`min.insync.replicas=3`, the high watermark moved, and `leader-only-write`
became visible.

Put the topic back to the cluster default:

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --alter --topic cdr.voice --delete-config min.insync.replicas
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions    # nothing
```

### 5.5 Rebalance leadership

Partitions 1 and 5 are healthy but still led by broker 1, not their
**preferred** leader, broker 3 (the first in `Replicas`). Broker 1 is doing more
work than broker 3.

```bash
# (container)
kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions
kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.voice | grep -o "Leader: [0-9]" | sort | uniq -c
```

```
Successfully completed leader election (PREFERRED) for partitions sms.events-1, __consumer_offsets-48, ..., cdr.voice-1, ..., cdr.voice-5, ...
      2 Leader: 1
      2 Leader: 2
      2 Leader: 3
```

Brokers also do this by themselves (`auto.leader.rebalance.enable=true`,
checked every `leader.imbalance.check.interval.seconds=300`), so after five
minutes you would have seen the same result without the command.

---

## Part 6 — Delete topics (5 min)

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --delete --topic sms.events
kafka-topics.sh --bootstrap-server $BS --list
ls /var/lib/kafka/data | grep sms
```

```
__consumer_offsets
cdr.voice

sms.events-0.d145cbc612654d51b22e71039ea36a49-delete
sms.events-1.c3ab8a8970b64159809049c1524887d7-delete
...
```

The topic disappears from metadata at once. On disk, its directories are first
renamed `...-delete` and removed about a minute later (`file.delete.delay.ms`).
Run the `ls` again after a minute: nothing is left.

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --delete --topic sms.events               # error: does not exist
kafka-topics.sh --bootstrap-server $BS --delete --topic sms.events --if-exists   # silent, exit 0
```

> ⚠️ **`--delete --topic` accepts a regular expression.** `--topic "cdr.*"`
> deletes **every** topic that matches, without asking and without a dry-run
> option, and there is no undo. On shared clusters, always pass the full, exact
> name, and run `--list` with the same pattern first to see what would match.

---

## Checkpoint questions

<details>
<summary>1. A colleague wants to change <code>retention.ms</code> on a topic with <code>kafka-topics.sh --alter --config</code>. What happens on Kafka 4.x, and what should they run?</summary>

The command is rejected: topic configs are no longer altered through
`kafka-topics.sh`. Use
`kafka-configs.sh --bootstrap-server ... --alter --topic NAME --add-config retention.ms=...`.
The change is applied live, with no restart.
</details>

<details>
<summary>2. Why can you increase a topic's partition count but never decrease it? What is the side effect of increasing it?</summary>

Removing a partition would delete its data and invalidate committed offsets.
Increasing is allowed, but `hash(key) % partitions` changes, so new records for
an existing key may land in a different partition than its older records.
Per-key ordering is broken across the change, and consumers that assume "one key
lives in one partition" may process out of order.
</details>

<details>
<summary>3. With RF 3 and <code>min.insync.replicas=2</code>, how many brokers can fail before an <code>acks=all</code> producer is rejected? And an <code>acks=1</code> producer?</summary>

`acks=all` keeps working with **one** broker down (ISR 2 ≥ 2). With two down,
the ISR is 1 and writes fail with `NOT_ENOUGH_REPLICAS`. An `acks=1` producer
gets a success as long as the partition has a leader. But as you saw in Part
5.3, on Kafka 4.x those records are not visible to consumers until the ISR is
back to `min.insync.replicas`. If the leader's disk is lost first, they are
gone.
</details>

<details>
<summary>4. After a broker restart, all partitions show a full ISR but <code>--describe</code> shows that broker leads no partitions. Is anything wrong?</summary>

No data problem: the broker is a healthy follower. Leadership has not moved back
to the **preferred** replicas yet. The brokers rebalance automatically within
`leader.imbalance.check.interval.seconds` (5 minutes by default), or you run
`kafka-leader-election.sh --election-type preferred`.
</details>

<details>
<summary>5. <code>--from-beginning</code> was set, yet the second run of the <code>billing</code> consumer printed only 3 records. Why?</summary>

`--from-beginning` only applies when the group has **no committed offset**
(it sets `auto.offset.reset=earliest`). `billing` had committed offsets from the
first run, so it resumed from them. To re-read, reset the group's offsets (it
must have no active members) or use a new group name.
</details>

<details>
<summary>6. <code>--under-replicated-partitions</code> lists 62 partitions, <code>--under-min-isr-partitions</code> lists none. Page the on-call engineer at 3 a.m.?</summary>

It is urgent, but not an outage. All writes and reads work, but redundancy is
reduced: one more broker failure would push partitions under min ISR and
reject `acks=all` producers. Find out why the broker is down and bring it back
promptly. If `--under-min-isr-partitions` or `--unavailable-partitions` print
anything, clients are already affected, and that is the page.
</details>

---

## Clean up

Keep the cluster (and `cdr.voice`) for Lab 03.

```bash
# (container)
exit
```

**Next:** [Lab 03 — Kafka administration from Spring Boot](lab-03-spring-boot-admin-client.md)
