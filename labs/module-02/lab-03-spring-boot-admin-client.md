# Lab 03 — Kafka Administration from Spring Boot

| | |
| --- | --- |
| **Level** | Intermediate |
| **Duration** | ~60 minutes |
| **Guide sections** | §7 CLI overview · §8 Topic management · §9 Producing and consuming · §11 Troubleshooting · §12 Bridging (Module 4 preview) |
| **You will need** | The 3-node cluster from Lab 01/02 running, Java 17+ JDK, Maven 3.9+, port 8080 free, three terminals, `curl` (and `jq` for readable JSON, optional) |

## Why this lab

Every `kafka-*.sh` tool you used in Labs 01–02 is a small Java program that
calls the Kafka **Admin API** or the **producer/consumer** clients. Applications
use exactly the same APIs. In this lab a Spring Boot application:

- **declares topics as code**, and creates or corrects them at startup,
- exposes the Admin API as REST endpoints (describe cluster, create /
  describe / alter / delete topics, consumer lag), which you compare with the CLI,
- produces with `KafkaTemplate` and consumes with `@KafkaListener`, in the same
  consumer group model as the console tools.

The app connects **from your laptop**, through the `EXTERNAL` listeners
(`localhost:9092,9094,9096`). This is the path real applications take, not the
internal `kafka-N:29092` one the CLI used inside the containers.

## Learning objectives

By the end of this lab you will be able to:

1. Map each CLI admin command to its Java **Admin API** call.
2. Manage topics **declaratively** with Spring's `NewTopic` beans, and explain
   what `KafkaAdmin` will and will not change on an existing topic.
3. Produce and consume from Java, and prove that CLI and Java clients share the
   same topics, key partitioning, consumer groups and committed offsets.
4. Read consumer lag both from Java and from `kafka-consumer-groups.sh`.
5. Explain how the application behaves when the cluster is unreachable at
   startup, and when a broker fails while it is running.

---

## Part 1 — Tour the project (10 min)

The project is in [`spring-kafka-admin/`](spring-kafka-admin/): Spring Boot 4.1,
Spring for Apache Kafka 4.1, Kafka clients 4.2, and Java 17 or newer.

| File | What it does | CLI equivalent |
| ---- | ------------ | -------------- |
| [`application.yml`](spring-kafka-admin/src/main/resources/application.yml) | Bootstrap servers, producer (`acks=all`, idempotence) and consumer (`group-id: cdr-billing`) settings | `--bootstrap-server`, `--command-config` files |
| [`KafkaTopicsConfig.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/KafkaTopicsConfig.java) | Two `NewTopic` beans: `cdr.data` (6 partitions, RF 3, min ISR 2, 7-day retention) and `subscriber.profile` (compacted), plus an `Admin` client bean | `kafka-topics.sh --create ... --config ...` |
| [`ClusterAdminService.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/ClusterAdminService.java) | One method per Admin API call. Each method's comment names the CLI command it replaces | `kafka-topics.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh`, `kafka-metadata-quorum.sh` |
| [`AdminController.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/AdminController.java) | REST endpoints under `/api` for the service | — |
| [`ApiErrorHandler.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/ApiErrorHandler.java) | Returns Kafka errors as JSON, with the same message the CLI prints | `Error while executing topic command : ...` |
| [`CdrController.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/CdrController.java) | `KafkaTemplate` producer endpoints, and stop/start of the listener | `kafka-console-producer.sh` |
| [`CdrListener.java`](spring-kafka-admin/src/main/java/com/training/kafka/admin/CdrListener.java) | `@KafkaListener` on `cdr.data`, group `cdr-billing`, 3 consumer threads | `kafka-console-consumer.sh --group cdr-billing` |

Open `ClusterAdminService.java` and read `describeTopic`. It calls
`admin.describeTopics(...)`, then `admin.describeConfigs(...)`, and keeps only
configs whose source is not `DEFAULT_CONFIG`. That is what
`kafka-topics.sh --describe` plus `kafka-configs.sh --describe` do together.

| REST endpoint | Admin API call |
| ------------- | -------------- |
| `GET /api/cluster` | `describeCluster()`, `describeMetadataQuorum()` |
| `GET /api/topics[?internal=true]` | `listTopics()` |
| `GET /api/topics/{name}` | `describeTopics()`, `describeConfigs()` |
| `POST /api/topics` | `createTopics()` |
| `PATCH /api/topics/{name}/partitions?count=N` | `createPartitions()` |
| `DELETE /api/topics/{name}` | `deleteTopics()` |
| `GET /api/groups/{groupId}` | `describeConsumerGroups()`, `listConsumerGroupOffsets()`, `listOffsets()` |

---

## Part 2 — Build and start the application (5 min)

**Terminal 1** (host), from `labs/module-02`:

```bash
# (host)
docker compose ps                     # 3 x (healthy)
cd spring-kafka-admin
java -version                         # 17 or newer
mvn -q package
java -jar target/spring-kafka-admin-1.0.0.jar
```

The first build downloads dependencies and can take a few minutes. Leave the
application running in this terminal: its log **is** the consumer output.

```
... Started KafkaAdminLabApplication in 1.695 seconds (process running for 2.078)
... [r-billing-0-C-1] o.s.k.l.KafkaMessageListenerContainer : cdr-billing: partitions assigned: [cdr.data-0, cdr.data-1]
... [r-billing-1-C-1] o.s.k.l.KafkaMessageListenerContainer : cdr-billing: partitions assigned: [cdr.data-2, cdr.data-3]
... [r-billing-2-C-1] o.s.k.l.KafkaMessageListenerContainer : cdr-billing: partitions assigned: [cdr.data-4, cdr.data-5]
```

- Before the web server started, `KafkaAdmin` **created** `cdr.data` and
  `subscriber.profile` from the `NewTopic` beans.
- `concurrency = "3"` started three consumers in one group, and the 6
  partitions were split 2 per consumer.

**Terminal 2** (host): check with the CLI that the topics exist exactly as
declared:

```bash
# (host) - from labs/module-02
docker exec kafka-1 bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.data | head -1'
docker exec kafka-1 bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --topic subscriber.profile | head -1'
```

```
Topic: cdr.data  ...  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
Topic: subscriber.profile  ...  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,cleanup.policy=compact
```

> **Windows PowerShell:** use `curl.exe` instead of `curl` in the rest of the
> lab, or run the commands in Git Bash / WSL. The course VMs are Linux.

---

## Part 3 — The Admin API next to the CLI (15 min)

Use **terminal 2**. Remove `| jq ...` from any command if you do not have `jq`.

### 3.1 The cluster

```bash
# (host)
curl -s localhost:8080/api/cluster | jq .
```

```json
{
  "clusterId": "Je89w39_Q46DWmFYXCGkGw",
  "activeControllerId": 2,
  "controllerVoters": [1, 2, 3],
  "brokers": [
    { "id": 1, "host": "localhost", "port": 9092, "fenced": false },
    { "id": 2, "host": "localhost", "port": 9094, "fenced": false },
    { "id": 3, "host": "localhost", "port": 9096, "fenced": false }
  ]
}
```

Compare with `kafka-metadata-quorum.sh ... describe --status` from Lab 01.
The broker **hosts** are `localhost:9092/9094/9096` here, but `kafka-N:29092`
from the CLI inside a container. Each client sees the addresses advertised on
the listener it connected through.

### 3.2 Topics

```bash
# (host)
curl -s localhost:8080/api/topics | jq -c .
curl -s "localhost:8080/api/topics?internal=true" | jq -c .
curl -s localhost:8080/api/topics/cdr.data | jq -c '.partitions[], .configs'
```

```
["cdr.data","cdr.voice","subscriber.profile"]
["__consumer_offsets","cdr.data","cdr.voice","subscriber.profile"]
{"partition":0,"leader":1,"replicas":[1,2,3],"isr":[1,2,3]}
{"partition":1,"leader":2,"replicas":[2,3,1],"isr":[2,3,1]}
...
{"min.insync.replicas":"2","retention.ms":"604800000"}
```

Same data as `kafka-topics.sh --describe`: leader, replicas and ISR per
partition, plus the non-default configs.

### 3.3 Create, grow and delete a topic

```bash
# (host)
curl -s -H "Content-Type: application/json" \
  -d '{"name":"cdr.fraud","partitions":3,"replicationFactor":3,"configs":{"retention.ms":"86400000"}}' \
  localhost:8080/api/topics | jq -c '{name, partitions: (.partitions|length), configs}'

curl -s -X PATCH "localhost:8080/api/topics/cdr.fraud/partitions?count=6" | jq '.partitions | length'
```

```
{"name":"cdr.fraud","partitions":3,"configs":{"min.insync.replicas":"2","retention.ms":"86400000"}}
6
```

Check with the CLI, then delete it through the API:

```bash
# (host)
docker exec kafka-1 bash -c 'kafka-topics.sh --bootstrap-server $BS --describe --topic cdr.fraud | head -1'
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE localhost:8080/api/topics/cdr.fraud
docker exec kafka-1 bash -c 'kafka-topics.sh --bootstrap-server $BS --list'
```

```
Topic: cdr.fraud  ...  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=86400000
204
__consumer_offsets
cdr.data
cdr.voice
subscriber.profile
```

### 3.4 The same errors, from Java

Repeat the mistakes from Lab 02 through the API:

```bash
# (host)
H="Content-Type: application/json"
curl -s -H "$H" -d '{"name":"cdr.fraud","partitions":3,"replicationFactor":4}' localhost:8080/api/topics | jq -c '{status,title,detail}'
curl -s -H "$H" -d '{"name":"cdr.data","partitions":6,"replicationFactor":3}' localhost:8080/api/topics | jq -c '{status,title,detail}'
curl -s -H "$H" -d '{"name":"cdr_data"}' localhost:8080/api/topics | jq -c '{status,title,detail}'
curl -s -H "$H" -d '{"name":"cdr.bad","configs":{"retention.ms":"abc"}}' localhost:8080/api/topics | jq -c '{status,title,detail}'
curl -s -X PATCH "localhost:8080/api/topics/cdr.data/partitions?count=3" | jq -c '{status,title,detail}'
curl -s localhost:8080/api/topics/no.such.topic | jq -c '{status,title,detail}'
```

```
{"status":400,"title":"InvalidReplicationFactorException","detail":"Unable to replicate the partition 4 time(s): The target replication factor of 4 cannot be reached because only 3 broker(s) are registered or some brokers have all their log directories cordoned."}
{"status":409,"title":"TopicExistsException","detail":"Topic 'cdr.data' already exists."}
{"status":400,"title":"InvalidTopicException","detail":"Topic 'cdr_data' collides with existing topic: cdr.data"}
{"status":400,"title":"InvalidConfigurationException","detail":"Invalid value abc for configuration retention.ms: Not a number of type LONG"}
{"status":400,"title":"InvalidPartitionsException","detail":"The topic cdr.data currently has 6 partition(s); 3 would not be an increase."}
{"status":404,"title":"UnknownTopicOrPartitionException","detail":"This server does not host this topic-partition."}
```

The messages are word for word what the CLI printed, because they come from
the **broker**. The CLI and the application are only messengers. When a
developer reports an error from their app, you can reproduce it with the CLI,
and the other way round.

---

## Part 4 — Produce and consume across Java and the CLI (15 min)

### 4.1 Java produces, both consume

```bash
# (host) - terminal 2
curl -s -H "Content-Type: application/json" -d '{"msisdn":"966500000001","event":"call-start"}' localhost:8080/api/cdr | jq -c .
curl -s -H "Content-Type: application/json" -d '{"msisdn":"966500000001","event":"call-end"}' localhost:8080/api/cdr | jq -c .
```

```
{"topic":"cdr.data","partition":2,"offset":0,"key":"966500000001","value":"call-start"}
{"topic":"cdr.data","partition":2,"offset":1,"key":"966500000001","value":"call-end"}
```

**Terminal 1** (the app log) shows the listener receiving them:

```
... [r-billing-1-C-1] com.training.kafka.admin.CdrListener : partition=2 offset=0 key=966500000001 value=call-start
... [r-billing-1-C-1] com.training.kafka.admin.CdrListener : partition=2 offset=1 key=966500000001 value=call-end
```

Key `966500000001` went to **partition 2**, the same partition it went to in
`cdr.voice` in Lab 02. Both topics have 6 partitions, and the Java producer and
the console producer use the same hash (murmur2) of the key.

The CLI, in a **different** group, reads the same records independently:

```bash
# (host) - terminal 2
docker exec kafka-1 bash -c 'kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.data --from-beginning \
  --formatter-property print.partition=true --formatter-property print.offset=true \
  --formatter-property print.key=true --timeout-ms 4000 2>/dev/null'
```

```
Partition:2  Offset:0  966500000001  call-start
Partition:2  Offset:1  966500000001  call-end
```

### 4.2 The CLI produces, Java consumes

```bash
# (host) - terminal 2
docker exec kafka-1 bash -c 'echo 966500000002:call-start-from-cli | kafka-console-producer.sh \
  --bootstrap-server $BS --topic cdr.data --reader-property parse.key=true --reader-property key.separator=:'
```

Terminal 1:

```
... CdrListener : partition=2 offset=2 key=966500000002 value=call-start-from-cli
```

Kafka does not care which client wrote a record. There are only bytes, a key
and a partition.

### 4.3 Load, lag and the consumer group

Send 100 records spread over 10 subscribers:

```bash
# (host) - terminal 2
curl -s -X POST "localhost:8080/api/cdr/generate?count=100" | jq -c .
```

```
{"0":10,"1":10,"2":20,"3":10,"4":20,"5":30}
```

That is the number of records per partition. Ten keys over six partitions is not
even: partition 5 got three keys' worth. Terminal 1 scrolls as the three
listener threads process them.

Now **stop** the listener (its three consumers leave the group), and send
another 100:

```bash
# (host) - terminal 2
curl -s -X POST localhost:8080/api/listener/stop | jq -c .
curl -s -X POST "localhost:8080/api/cdr/generate?count=100" | jq -c .
curl -s localhost:8080/api/groups/cdr-billing | jq -c '{groupId,state,members,totalLag}'
docker exec kafka-1 bash -c 'kafka-consumer-groups.sh --bootstrap-server $BS --describe --group cdr-billing'
```

```
{"running":false,"listenerId":"cdr-billing"}
{"0":10,"1":10,"2":20,"3":10,"4":20,"5":30}
{"groupId":"cdr-billing","state":"Empty","members":0,"totalLag":100}

Consumer group 'cdr-billing' has no active members.

GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
cdr-billing  cdr.data  0          10              20              10   -            -     -
cdr-billing  cdr.data  1          10              20              10   -            -     -
cdr-billing  cdr.data  2          23              43              20   -            -     -
cdr-billing  cdr.data  3          10              20              10   -            -     -
cdr-billing  cdr.data  4          20              40              20   -            -     -
cdr-billing  cdr.data  5          30              60              30   -            -     -
```

The Java view (`totalLag: 100`) and the CLI view agree. They read the same
committed offsets from `__consumer_offsets`. Open `groupLag()` in
`ClusterAdminService.java` to see how lag is computed: committed offsets
(`listConsumerGroupOffsets`) against log-end offsets (`listOffsets` with
`OffsetSpec.latest()`).

Start the listener again and watch the lag drain:

```bash
# (host) - terminal 2
curl -s -X POST localhost:8080/api/listener/start | jq -c .
sleep 5
curl -s localhost:8080/api/groups/cdr-billing | jq -c '{groupId,state,members,totalLag}'
docker exec kafka-1 bash -c 'kafka-consumer-groups.sh --bootstrap-server $BS --describe --group cdr-billing --members'
```

```
{"groupId":"cdr-billing","state":"Stable","members":3,"totalLag":0}

GROUP        CONSUMER-ID                                    HOST           CLIENT-ID              #PARTITIONS
cdr-billing  consumer-cdr-billing-4-774b35a8-...            /192.168.65.1  consumer-cdr-billing-4 2
cdr-billing  consumer-cdr-billing-5-442abb4d-...            /192.168.65.1  consumer-cdr-billing-5 2
cdr-billing  consumer-cdr-billing-6-7947b87a-...            /192.168.65.1  consumer-cdr-billing-6 2
```

The CLI sees the application's three consumers, each with 2 partitions. The
`HOST` is your laptop as seen from inside Docker.

---

## Part 5 — Topics as code: what `KafkaAdmin` changes (10 min)

`KafkaAdmin` runs once, at application startup. Stop the application in
**terminal 1** (`Ctrl+C`) before each step.

### 5.1 Someone changes a config by hand

```bash
# (host) - terminal 2
docker exec kafka-1 bash -c 'kafka-configs.sh --bootstrap-server $BS --alter --topic cdr.data --add-config retention.ms=3600000'
docker exec kafka-1 bash -c 'kafka-configs.sh --bootstrap-server $BS --describe --topic cdr.data' | grep retention
```

```
  retention.ms=3600000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=3600000}
```

Start the application (terminal 1: `java -jar target/spring-kafka-admin-1.0.0.jar`),
then look again:

```bash
# (host) - terminal 2
docker exec kafka-1 bash -c 'kafka-configs.sh --bootstrap-server $BS --describe --topic cdr.data' | grep retention
```

```
  retention.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=604800000}
```

The hand-made change was **reverted** to the declared 7 days, because
`application.yml` sets `spring.kafka.admin.modify-topic-configs: true`. Powerful
and dangerous: the code, not the admin, owns this topic's configuration. Agree
on who owns each topic before turning this on in production.

### 5.2 The code asks for more partitions

Stop the app, then start it with 8 partitions for `cdr.data`:

```bash
# (host) - terminal 1
java -jar target/spring-kafka-admin-1.0.0.jar --lab.topics.cdr-partitions=8
```

```
... o.s.kafka.core.KafkaAdmin : Topic 'cdr.data' exists but has a different partition count: 6 not 8, increasing if the broker supports it
... cdr-billing: partitions assigned: [cdr.data-0, cdr.data-1, cdr.data-2]
... cdr-billing: partitions assigned: [cdr.data-3, cdr.data-4, cdr.data-5]
... cdr-billing: partitions assigned: [cdr.data-6, cdr.data-7]
```

8 partitions over 3 consumers: 3 + 3 + 2. If you restart without the option
(6 declared, 8 actual), `KafkaAdmin` does **not** remove partitions, for the
same reason the CLI cannot. It only logs:

```
... o.s.kafka.core.KafkaAdmin : Topic 'cdr.data' exists but has a different partition count: 8 not 6
```

| Declared in `NewTopic` vs. actual topic | What `KafkaAdmin` does at startup |
| --------------------------------------- | --------------------------------- |
| Topic missing | Creates it |
| More partitions declared | Adds partitions |
| Fewer partitions declared | Nothing (logs it) |
| Different topic config | Changes it, only with `modify-topic-configs: true` |
| Different replication factor | Nothing: that is a reassignment (Module 5) |
| Topic exists but has no bean | Nothing. It never deletes topics |

### 5.3 Fail fast

What if the declared topic cannot be created? Stop the app and start it asking
for RF 4 on a new topic:

```bash
# (host) - terminal 1
java -jar target/spring-kafka-admin-1.0.0.jar --lab.topics.cdr=cdr.rf4 --lab.topics.cdr-replicas=4
```

```
java.lang.IllegalStateException: Could not configure topics
Caused by: org.springframework.kafka.KafkaException: Failed to create topics
Caused by: org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 4 time(s): ...
```

The application **refuses to start**: `spring.kafka.admin.fail-fast: true`.
Without it, the app would start and fail later on the first send. Failing at
deploy time is easier to diagnose. The same thing happens if no broker is
reachable at all: after about 45 seconds the app stops with
`Caused by: java.util.concurrent.TimeoutException`.

Start the application normally again for Part 6:

```bash
# (host) - terminal 1
java -jar target/spring-kafka-admin-1.0.0.jar
```

---

## Part 6 — A broker fails under a running application (5 min)

```bash
# (host) - terminal 2, from labs/module-02
docker compose stop kafka-2

for i in 1 2 3; do
  curl -s -H "Content-Type: application/json" \
    -d "{\"msisdn\":\"96650000000$i\",\"event\":\"during-outage\"}" localhost:8080/api/cdr | jq -c .
done
curl -s localhost:8080/api/cluster | jq -c '.activeControllerId, .brokers[]'
curl -s localhost:8080/api/topics/cdr.data | jq -c '.partitions[] | {partition, leader, isr}'
```

```
{"topic":"cdr.data","partition":0,"offset":20,"key":"966500000001","value":"during-outage"}
{"topic":"cdr.data","partition":0,"offset":21,"key":"966500000002","value":"during-outage"}
{"topic":"cdr.data","partition":5,"offset":60,"key":"966500000003","value":"during-outage"}
3
{"id":1,"host":"localhost","port":9092,"fenced":false}
{"id":2,"host":"localhost","port":9094,"fenced":true}
{"id":3,"host":"localhost","port":9096,"fenced":false}
{"partition":0,"leader":3,"isr":[3,1]}
{"partition":1,"leader":3,"isr":[3,1]}
{"partition":2,"leader":1,"isr":[1,3]}
...
{"partition":7,"leader":3,"isr":[3,1]}
```

Two things to notice straight away:

- `966500000001` now lands in **partition 0**, not partition 2 as in Part 4.
  `cdr.data` has 8 partitions since Part 5.2, so `hash(key) % 8` gives a
  different answer than `hash(key) % 6`. This is the ordering risk of adding
  partitions (Lab 02 Part 3.1), shown live.
- If broker 2 still shows `"fenced": false`, run the `/api/cluster` command
  again a few seconds later. The controller fences a broker once its shutdown
  completes.

(Which node is the active controller depends on your cluster. If it was 2, a new
one took over.)

- `acks=all` writes kept succeeding: every partition still has ISR 2, which
  meets `min.insync.replicas=2`.
- Broker 2 is still **registered** but **fenced**: the controller has stopped
  sending it work. `DescribeClusterOptions().includeFencedBrokers(true)` in
  `describeCluster()` is what makes it visible.
- The app log (terminal 1) shows `WARN ... Connection to node -2
  (localhost/127.0.0.1:9094) could not be established` (negative IDs are
  entries in the bootstrap list; -2 is the second one). The clients noticed
  and moved on to the new leaders. **No code** handled the failover. The Kafka
  client library did it using metadata.

Bring it back:

```bash
# (host) - terminal 2
docker compose start kafka-2
sleep 10
curl -s localhost:8080/api/cluster | jq -c '.brokers[] | {id, fenced}'
```

All three `"fenced": false` again.

---

## Checkpoint questions

<details>
<summary>1. A developer says "my Spring app gets <code>InvalidReplicationFactorException</code> but the CLI works fine". What do you check first?</summary>

That both point at the **same cluster**. The error comes from the broker, not
from the client, so the same request fails the same way from any client. Most
likely the app's `bootstrap-servers` points at a smaller (for example 1-broker
dev) cluster, or the app asks for a higher RF than the CLI command did.
</details>

<details>
<summary>2. You increased <code>retention.ms</code> on an application's topic with <code>kafka-configs.sh</code>. The next day it is back to the old value. What happened?</summary>

The application declares that topic as a `NewTopic` bean with
`modify-topic-configs` enabled. At its next restart (a redeploy), `KafkaAdmin`
reset the config to the declared value. Change the value in the application's
code/configuration, or agree that admins own that topic and disable config
modification in the app.
</details>

<details>
<summary>3. Why does <code>KafkaAdmin</code> add partitions but never remove them, or change the replication factor?</summary>

Removing partitions would lose data and is not supported by Kafka at all.
Changing the replication factor means **moving data** between brokers (a
partition reassignment). That is a long-running operational task that needs
throttling and monitoring (Module 5), not something to trigger silently at
application startup.
</details>

<details>
<summary>4. Lag for <code>cdr-billing</code> reported only one partition, even though records were waiting in all six. How can that happen?</summary>

Lag is computed only for partitions where the group has a **committed offset**.
If a consumer group has never committed for a partition (it never received data
there, or it is a brand-new group), `kafka-consumer-groups.sh` and the Admin API
show no row for it, so those waiting records are not counted as lag. That is a
real blind spot for monitoring new consumer groups. Compare the group's
partitions with the topic's partitions, not just the lag total.
</details>

<details>
<summary>5. The application lists all three brokers in <code>bootstrap-servers</code>. Is that required? What happens if it lists only <code>localhost:9092</code> and kafka-1 is down when the app starts?</summary>

Not required: one reachable broker is enough to fetch metadata for the whole
cluster. With only `localhost:9092` listed and kafka-1 down at startup, there is
nothing to bootstrap from, so the app fails to start (fail-fast). Once running,
the list no longer matters: clients use the metadata. Listing several brokers
protects the **startup** path.
</details>

<details>
<summary>6. Inside Docker the CLI used <code>kafka-2:29092</code>, the app on your laptop used <code>localhost:9094</code>, for the same broker. Why?</summary>

Each broker has two client listeners with different **advertised** addresses:
`PLAINTEXT` (reachable inside the Docker network by container name) and
`EXTERNAL` (reachable from the host through the published port). A client
receives the addresses of the listener it bootstrapped through, so each side
gets addresses it can actually reach (Lab 01 Part 6.1).
</details>

---

## Clean up

```bash
# (host) - terminal 1: stop the application with Ctrl+C

# (host) - from labs/module-02: stop the cluster and delete all data
docker compose down -v
```

You now have the same toolset for administering Kafka three ways: the CLI, the
Admin API, and declarative topics in code. Module 3 goes deeper into the
configuration you set here: segments, retention, cleanup policies and
`min.insync.replicas`, on the shared AWS cluster. Module 4 builds full Java
producer and consumer applications.
