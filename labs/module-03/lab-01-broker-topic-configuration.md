# Lab 01 — Broker & Topic Configuration on the Shared Cluster

| | |
| --- | --- |
| **Level** | Beginner |
| **Duration** | ~55 minutes |
| **Guide sections** | §2 Broker configuration parameters · §4 Topic-level configuration overrides · §5 Retention and cleanup policies · §6 min.insync.replicas and acknowledgments · §7.1 Measure first · §8.1 Hands-on Part A |
| **You will need** | Your lab VM with `~/kafka/apache.properties`, your prefix `lNN`, one terminal |

## Learning objectives

By the end of this lab you will be able to:

1. Connect to the **shared Apache Kafka cluster** with your prepared config
   file, and work safely inside your own `lNN.*` prefix.
2. Create a topic whose **storage and durability contract is designed, not
   defaulted**: retention, segment size, `min.insync.replicas`, cleanup policy.
3. Read **every effective config value and its source** from
   `kafka-configs.sh --describe`, and explain the precedence ladder (topic →
   dynamic default → static → built-in).
4. Change `retention.ms` **live** — no restart, no topic downtime — and remove
   an override so the value falls back to the broker default.
5. Produce with `acks=all` and consume as a group through the same config file,
   and measure the per-partition disk footprint with `kafka-log-dirs.sh`.
6. Recognise the **multi-tenant guardrails** of the shared cluster and why they
   exist.

This lab runs entirely **against the shared cluster**. If you did the
T-3 connectivity check from `infra/LAB-SETUP.md`, you have already used these
commands once. The examples below use `l07` as the prefix — replace it with
your own everywhere.

---

## Part 1 — Meet the shared cluster (8 min)

### 1.1 Your coordinates

Every learner authenticates with SASL/SCRAM through a prepared config file.
Define the three variables every command in this lab uses:

```bash
# (VM)
APACHE=apache-kafka.lab.internal:9092
CFG=~/kafka/apache.properties
ME=lNN                          # your learner prefix: l01 … l18
```

If you open a new terminal later, re-export them. Check connectivity the same
way the course's pre-flight check does:

```bash
# (VM)
kafka-topics.sh --bootstrap-server $APACHE --command-config $CFG --list
```

**Expected:** nothing. The cluster is running, you are authenticated, and you
have no topics yet. What you will never see here: other learners' topics. The
cluster ACLs give each user access only to their own `lNN.*` topics, so the
list is filtered to what you are allowed to describe.

> **Administrator rule:** `--command-config $CFG` goes on every **admin** tool
> (`kafka-topics.sh`, `kafka-configs.sh`, `kafka-log-dirs.sh`,
> `kafka-consumer-groups.sh`) and `--command-config $CFG` on the console
> clients too. Forgetting it does not produce a helpful error message — the
> tool just cannot authenticate.

### 1.2 Where your topic defaults come from

Every value you do not set on a topic comes from the **broker** layer (guide
§2.1). Read what the operator set cluster-wide:

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --entity-type brokers --entity-default
```

**Expected** — the shape is always the same; the values are whatever your
cluster's operator configured:

```
Default configs for brokers in the cluster are:
  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2}
```

This is the **cluster-wide dynamic default** layer. Your topics inherit these
values unless you override them — which is exactly what Part 2 does.

> **Note on what you cannot do here:** learners have read-only `Describe` on
> the cluster, so you can read broker configs but not alter them. Part 6
> demonstrates the guardrail deliberately.

---

## Part 2 — Create a topic with a designed contract (12 min)

Storage and durability settings should be decided per topic class **before**
the first record arrives, not left to cluster defaults (guide §4.1, §4.4).
Create a CDR topic the way a telecom admin would:

```bash
# (VM)
kafka-topics.sh --bootstrap-server $APACHE --command-config $CFG --create \
  --topic $ME.cdr.voice \
  --partitions 6 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config segment.bytes=268435456 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete
```

**Expected:**

```
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic l07.cdr.voice.
```

| Setting | Value | What it means |
| ------- | ----- | ------------- |
| `partitions 6` | 6 | Parallelism for producers/consumers (Module 2 §8.2) |
| `replication-factor 3` | 3 | Every record stored on 3 brokers |
| `retention.ms` | 7 days | Events kept one week (guide §5.2) |
| `segment.bytes` | 256 MiB | Smaller than the 1 GiB default: finer-grained deletion for a high-volume topic (guide §7.4) |
| `min.insync.replicas` | 2 | `acks=all` writes need at least 2 in-sync copies (guide §6.3) |
| `cleanup.policy` | delete | Events expire by time, not by key (guide §5.6) |

See the contract in the topic's summary line:

```bash
# (VM)
kafka-topics.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --topic $ME.cdr.voice
```

**Expected** (the leader/replica spread varies from run to run; the summary
line is what matters):

```
Topic: l07.cdr.voice	TopicId: vz2d1u0TSUWDpuVQVg9JzA	PartitionCount: 6	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,segment.bytes=268435456,retention.ms=604800000
	Topic: l07.cdr.voice	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 
	Topic: l07.cdr.voice	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: l07.cdr.voice	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: l07.cdr.voice	Partition: 3	Leader: 1	Replicas: 1,3,2	Isr: 1,3,2	Elr: 	LastKnownElr: 
	Topic: l07.cdr.voice	Partition: 4	Leader: 3	Replicas: 3,2,1	Isr: 3,2,1	Elr: 	LastKnownElr: 
	Topic: l07.cdr.voice	Partition: 5	Leader: 2	Replicas: 2,1,3	Isr: 2,1,3	Elr: 	LastKnownElr: 
```

Every partition has 3 replicas across 3 different brokers and a full ISR. Note
the `Elr` / `LastKnownElr` columns (Kafka 4.x, guide §6.2) — empty while the
cluster is healthy.

### 2.1 A comparison topic left on defaults

```bash
# (VM)
kafka-topics.sh --bootstrap-server $APACHE --command-config $CFG --create \
  --topic $ME.metrics.raw --partitions 3 --replication-factor 3

kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --entity-type topics --entity-name $ME.metrics.raw
```

**Expected:**

```
Dynamic configs for topic l07.metrics.raw are:
```

…and nothing else. `metrics.raw` has **no topic-level configs at all**: every
behaviour comes from the broker defaults you read in Part 1.2. That is fine
for throwaway telemetry and unacceptable for billing records — the two topics
side by side are the whole lesson of this module.

---

## Part 3 — Read every value and its source (10 min)

`kafka-configs.sh --describe` prints the effective value of each property plus
its **synonyms**: the full list of layers that set it, highest precedence
first (guide §2.2, §4.2). This is the paper trail you follow when asking "why
is this value like that?".

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --entity-type topics --entity-name $ME.cdr.voice
```

**Expected** (the default layers listed may differ slightly on your cluster,
because they reflect what the operator configured):

```
Dynamic configs for topic l07.cdr.voice are:
  cleanup.policy=delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=delete, DEFAULT_CONFIG:log.cleanup.policy=delete}
  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=2, DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2, STATIC_BROKER_CONFIG:min.insync.replicas=2, DEFAULT_CONFIG:min.insync.replicas=1}
  retention.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=604800000}
  segment.bytes=268435456 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:segment.bytes=268435456, DEFAULT_CONFIG:log.segment.bytes=1073741824}
```

| Synonym label | Meaning | Precedence |
| ------------- | ------- | ---------- |
| `DYNAMIC_TOPIC_CONFIG` | You set it on this topic | Highest — wins |
| `DYNAMIC_DEFAULT_BROKER_CONFIG` | Cluster-wide dynamic default (Part 1.2) | Next |
| `STATIC_BROKER_CONFIG` | From the broker's `server.properties` | Then |
| `DEFAULT_CONFIG` | Kafka's built-in default | Lowest |

Two observations worth writing down:

- `min.insync.replicas` has **four** synonyms: you set it on the topic, the
  operator also set it cluster-wide and statically, and Kafka's built-in
  default is `1`. Your value wins for this topic.
- `retention.ms` has only **one** synonym: no layer below you sets it, so the
  moment you remove the override (Part 4), the topic falls all the way to the
  built-in 7 days.

> **Common trap:** topic-level properties never carry the `log.` prefix. At
> topic level it is `retention.ms`, `segment.bytes`, `cleanup.policy`;
> `log.retention.ms` only exists at broker level. Passing the broker name to a
> topic command fails with `InvalidConfigException`.

---

## Part 4 — Change retention live, then remove the override (8 min)

Storage configs are **dynamic** (guide §2.2): changing them takes effect
immediately, on every broker, with no restart and no consumer impact.

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG --alter \
  --entity-type topics --entity-name $ME.cdr.voice \
  --add-config retention.ms=86400000
```

**Expected:**

```
Completed updating config for topic l07.cdr.voice.
```

Confirm the new value and its source:

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --entity-type topics --entity-name $ME.cdr.voice
```

**Expected:**

```
Dynamic configs for topic l07.cdr.voice are:
  ...
  retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=86400000}
  ...
```

The change propagated while the topic stayed fully usable. In KRaft mode this
is a single event in the metadata log, not a restart (guide §4.3):

```mermaid
sequenceDiagram
    participant Admin as kafka-configs.sh
    participant Ctrl as Active controller
    participant Meta as __cluster_metadata
    participant B as Brokers (all 4)
    Admin->>Ctrl: IncrementalAlterConfigs(cdr.voice, retention.ms=86400000)
    Ctrl->>Meta: CONFIG_RECORD
    Ctrl-->>Admin: success
    B->>Meta: fetch
    Note over B: caches updated; next produce/fetch uses the new value
```

Now remove the override and watch the value snap back to the next layer down:

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG --alter \
  --entity-type topics --entity-name $ME.cdr.voice \
  --delete-config retention.ms

kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --entity-type topics --entity-name $ME.cdr.voice
```

**Expected:** `Completed updating config for topic l07.cdr.voice.` and then a
config list with **no `retention.ms` line at all**. The override was the only
thing removed; the effective value is now whatever the broker layer provides
(7 days by built-in default). `--delete-config` never deletes data — it
deletes your *instruction* about the data.

---

## Part 5 — Prove the durable contract end to end (12 min)

Produce three keyed CDRs through your config file with an explicit `acks=all`,
so the contract you are demonstrating is visible in the command (guide §6.3):

```bash
# (VM)
kafka-console-producer.sh --bootstrap-server $APACHE --command-config $CFG \
  --topic $ME.cdr.voice --command-property acks=all \
  --reader-property parse.key=true --reader-property key.separator=:
# 966500000001:call-start
# 966500000002:call-start
# 966500000001:call-end
```

(Type the three lines, then `Ctrl+D` to end input. The producer prints nothing
on success — silence is the confirmation.)

Consume as a consumer group — the same way a billing application would:

```bash
# (VM)
kafka-console-consumer.sh --bootstrap-server $APACHE --command-config $CFG \
  --topic $ME.cdr.voice --group $ME.billing --from-beginning --timeout-ms 5000 \
  --formatter-property print.partition=true --formatter-property print.offset=true
```

**Expected** (your partition numbers may differ):

```
Partition:2	Offset:0	call-start
Partition:2	Offset:1	call-start
Partition:2	Offset:2	call-end
Processed a total of 3 messages
```

Same key (`966500000001`), same partition — per-key ordering holds. The group
committed its position, so a second run with the same `--group` would read
nothing new:

```bash
# (VM)
kafka-consumer-groups.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --group $ME.billing
```

**Expected:**

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
l07.billing     l07.cdr.voice   0          0               0               0               -               -               -
l07.billing     l07.cdr.voice   1          0               0               0               -               -               -
l07.billing     l07.cdr.voice   2          3               3               0               -               -               -
l07.billing     l07.cdr.voice   3          0               0               0               -               -               -
l07.billing     l07.cdr.voice   4          0               0               0               -               -               -
l07.billing     l07.cdr.voice   5          0               0               0               -               -               -
```

Finally, measure the disk footprint per partition — the first of the three
commands every storage incident starts with (guide §7.1):

```bash
# (VM)
kafka-log-dirs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --topic-list $ME.cdr.voice
```

**Expected:** one JSON document (per broker, per log directory, per partition)
with `"size"` in bytes per replica. Because the topic is RF 3, you will see
each of the 6 partitions three times — once per broker that holds a copy. On
your VM you can pipe it through `jq` (guide §7.1 shows the full treatment):

```bash
# (VM)
kafka-log-dirs.sh --bootstrap-server $APACHE --command-config $CFG \
  --describe --topic-list $ME.cdr.voice 2>/dev/null \
  | grep -o '^{.*' | jq -r '.brokers[] | "broker \(.broker)  \(.logDirs[0].partitions | map("\(.partition)=\(.size)B") | join("  "))"'
```

| Observation | Concept | Guide |
| ----------- | ------- | ----- |
| The create-time configs appear in the topic's `Configs:` line | Topic overrides recorded per topic | §4.2 |
| `min.insync.replicas` lists four synonym layers | Config precedence ladder | §2.1 |
| `--delete-config` removes only the override | Overrides are the only thing removed | §4.2 |
| Altering retention caused no restart, no downtime | KRaft dynamic config propagation | §4.3 |
| `acks=all` write succeeded with ISR 3 ≥ 2 | The durability contract | §6.2 |
| The same partition listed on several brokers | Replication: one copy per broker | Module 1 §6 |

---

## Part 6 — The guardrails you work under (5 min)

You share this cluster with 17 other learners. Try to change a **cluster-wide**
broker setting, as a curious admin might:

```bash
# (VM)
kafka-configs.sh --bootstrap-server $APACHE --command-config $CFG --alter \
  --entity-type brokers --entity-default \
  --add-config log.retention.ms=259200000
```

**Expected:** the command is **rejected** with a
`ClusterAuthorizationException` (`Cluster authorization failed`). No data was
changed anywhere — the broker refused to accept the request.

This is the shared cluster's design, not a malfunction:

| Guardrail | What it prevents | Where it comes from |
| --------- | ---------------- | ------------------- |
| `AlterConfigs` denied on the cluster | One learner changing retention for **everyone's** topics | Cluster ACLs (`infra/LAB-SETUP.md` §4) |
| Topics and groups restricted to `lNN.*` | Cross-tenant reads and writes | Prefix ACLs |
| `auto.create.topics.enable=false` | Typo'd topic names silently appearing | Broker config |
| Per-user quotas | One runaway producer starving the rest | Broker config |

On the shared cluster you configure **your topics**. Broker-level settings are
the operator's job — and in Lab 02 you become the operator, on your own
cluster.

---

## Checkpoint questions

<details>
<summary>1. You removed <code>retention.ms</code> with <code>--delete-config</code>. What value does the topic use now, and how would you prove it?</summary>

The topic falls back to the next layer down the precedence chain: the
cluster-wide dynamic default, then the broker's static `server.properties`
value, then Kafka's built-in default (7 days). Prove it with
`kafka-configs.sh --describe --entity-type topics --entity-name <topic>`: the
property disappears from the dynamic list entirely, which is the fallback —
there is no "deleted" state left behind.
</details>

<details>
<summary>2. Your topic shows <code>min.insync.replicas=2</code> in <code>Configs:</code> even though a teammate claims nobody ever set it on the topic. Where did the value come from?</summary>

From the broker layer. Read the synonyms in
`kafka-configs.sh --describe`: the winning label tells you — a cluster-wide
dynamic default (`DYNAMIC_DEFAULT_BROKER_CONFIG`) or the static
`STATIC_BROKER_CONFIG` layer. Kafka's built-in default for
`min.insync.replicas` is 1, so any other value came from somewhere above it.
</details>

<details>
<summary>3. Why did <code>kafka-topics.sh --list</code> print nothing on your first run, and why is that the healthy result?</summary>

Prefix ACLs limit you to your own `lNN.*` topics, and `--list` only returns
topics you are allowed to describe. An empty list means authentication
worked (a broken config file would have thrown an authentication error
instead) and that you have no topics yet — it says nothing about the 17 other
learners' topics, which you can neither see nor touch.
</details>

<details>
<summary>4. The alter in Part 4 applied instantly with no restart. Where is the change actually stored in a KRaft cluster?</summary>

The active controller appends a `CONFIG_RECORD` to the `__cluster_metadata`
log, commits it to the quorum, and every broker fetches the metadata log and
updates its config cache. There is no ZooKeeper and no restart — that is why
`kafka-configs.sh` and `kafka-topics.sh` reflect the change immediately.
</details>

<details>
<summary>5. <code>l07.metrics.raw</code> was created with no <code>--config</code> flags at all. What is its effective <code>retention.ms</code>?</summary>

Whatever the broker layer provides: the operator's cluster-wide dynamic
default if one is set, otherwise the static broker value, otherwise Kafka's
built-in 604800000 ms (7 days). The exact answer lives in
`kafka-configs.sh --describe --entity-type topics --entity-name l07.metrics.raw`
with `--all` (or in the broker's `--entity-default` output) — and because the
answer depends on the cluster, that is exactly why production topics should
set their own values explicitly.
</details>

<details>
<summary>6. Why does <code>kafka-log-dirs.sh</code> list the same partition several times for one topic?</summary>

It reports per broker, per log directory, per replica. A topic with
RF = 3 has three copies of every partition, each on a different broker, and
the tool lists all of them. The `size` value is therefore per **replica**, and
your topic's total footprint is roughly 3 × the sum over one broker's copies
of its partitions.
</details>

---

## Clean up

Leave `$ME.cdr.voice` and `$ME.metrics.raw` in place — they are yours, they are
small, and seeing `cdr.voice` behave as configured over the coming days is the
point. There is nothing to tear down on the shared cluster.

**Next:** [Lab 02 — Retention, segments & storage management](lab-02-retention-segments-storage-management.md), where
you become the operator of your own cluster and watch retention, compaction
and compression act on real segment files.
