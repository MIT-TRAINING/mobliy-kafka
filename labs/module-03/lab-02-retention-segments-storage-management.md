# Lab 02 — Retention, Segments & Storage Management

| | |
| --- | --- |
| **Level** | Beginner → Intermediate |
| **Duration** | ~70 minutes (includes a few 1–2 minute observation windows) |
| **Guide sections** | §3 Log directories and segment management · §4 Topic-level configuration overrides · §5 Retention and cleanup policies: delete vs. compact · §7 Storage optimization techniques |
| **You will need** | Docker running, `labs/module-02/docker-compose.yml` from Module 2, this folder's [`docker-compose.override.yml`](docker-compose.override.yml), two terminals |

## Learning objectives

By the end of this lab you will be able to:

1. Start the Module 2 cluster with a **compose override file** and explain what
   the override changes and why.
2. Locate partition data on disk and read the **per-partition, per-replica**
   sizes with `kafka-log-dirs.sh`.
3. Watch **time-based retention** delete whole segments, on **every broker's
   replica independently**, and see the log start offset move.
4. Configure **size-based retention** (`retention.bytes`) and explain why it
   lands in whole-segment steps and why it is **per partition**.
5. Configure **compaction** and observe latest-value-per-key, tombstones,
   offset gaps and the space it reclaims.
6. Measure the effect of **producer compression** (`zstd`) on identical data.

Unlike Lab 01, this lab runs on **your own 3-node cluster**, where you are the
operator: you can read the segment files, watch the retention loop and break
things without affecting anyone else.

---

## Part 1 — Start your own cluster with fast retention checks (5 min)

The cluster is the Module 2 one, started together with this module's override
file. The override changes exactly one broker setting:
`log.retention.check.interval.ms` from the 5-minute default to **15 seconds**,
so the exercises in this lab take seconds instead of minutes. Open
[`docker-compose.override.yml`](docker-compose.override.yml) and confirm that
is all it does, then start:

```bash
# (host) - from labs/module-03
docker compose -p kafka-m2 -f ../module-02/docker-compose.yml \
  -f docker-compose.override.yml up -d
docker compose -p kafka-m2 -f ../module-02/docker-compose.yml \
  -f docker-compose.override.yml ps
```

**Expected:** three containers, all `(healthy)`:

```
NAME      IMAGE                COMMAND                  SERVICE   CREATED          STATUS                    PORTS
kafka-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-1   30 seconds ago   Up 29 seconds (healthy)   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
kafka-2   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-2   30 seconds ago   Up 29 seconds (healthy)   0.0.0.0:9094->9092/tcp, [::]:9094->9092/tcp
kafka-3   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-3   30 seconds ago   Up 29 seconds (healthy)   0.0.0.0:9096->9092/tcp, [::]:9096->9092/tcp
```

Enter node 1 and set your prefix for the rest of the lab:

```bash
# (host)
docker exec -it kafka-1 bash
```

```bash
# (container)
ME=lNN
echo $BS        # kafka-1:29092,kafka-2:29092,kafka-3:29092  (preset from Module 2)
```

> **Note:** the `-p kafka-m2` flag fixes the project name so all later
> `stop` / `start` / `down` commands find the same containers and volumes,
> no matter which folder you run them from. Use the same long command every
> time (it is in the README's "Everyday commands").

> **Why a check interval this short is only for labs:** in production the
> retention loop runs every 5 minutes, and a busy broker can have hundreds of
> thousands of segments. Checking 20× more often costs real CPU and I/O — and
> it never makes data expire "faster" in a meaningful sense, because retention
> is about days, not seconds.

---

## Part 2 — Where does data actually live? (10 min)

### 2.1 A small topic with replicas everywhere

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create \
  --topic $ME.storage.test --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BS --describe --topic $ME.storage.test
```

**Expected:**

```
Topic: l07.storage.test	TopicId: TOaTnqvcTpm5KYn4oTASCQ	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: l07.storage.test	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: l07.storage.test	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 
	Topic: l07.storage.test	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
```

Produce a little data:

```bash
# (container)
for i in $(seq 1 30); do echo "96650000000$i:call-start:msc-central"; done | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.storage.test \
  --reader-property parse.key=true --reader-property key.separator=: 2>/dev/null
```

### 2.2 The administrator's map of the disk

```bash
# (container)
kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list $ME.storage.test
```

**Expected** — a single JSON document, one entry per broker:

```
Querying brokers for log directories information
Received log directory information from brokers 1,2,3
{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"l07.storage.test-2","size":477,"offsetLag":0,"isFuture":false},{"partition":"l07.storage.test-1","size":394,"offsetLag":0,"isFuture":false},{"partition":"l07.storage.test-0","size":563,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/var/lib/kafka/data"}]},{"broker":2,"logDirs":[ ...same three partitions... }]},{"broker":3,"logDirs":[ ...same three partitions... }]}],"version":1}
```

With 3 brokers and RF = 3, **every broker holds every partition** — each
partition appears three times, once per replica. That redundancy is the whole
point of RF = 3: `size` here is per replica, not per topic. (On the course VM
you can pipe this through `jq`; the container image has no `jq`.)

### 2.3 The files behind one partition

Partition directories live under `log.dirs`, one per partition, named
`<topic>-<partition>` (guide §3.1):

```bash
# (container)
ls -lh /var/lib/kafka/data/$ME.storage.test-2/
```

**Expected:**

```
total 12K
-rw-r--r-- 1 appuser appuser 10M Sep 30 03:02 00000000000000000000.index
-rw-r--r-- 1 appuser appuser 477 Sep 30 03:03 00000000000000000000.log
-rw-r--r-- 1 appuser appuser 10M Sep 30 03:02 00000000000000000000.timeindex
-rw-r--r-- 1 appuser appuser   8 Sep 30 03:02 leader-epoch-checkpoint
-rw-r--r-- 1 appuser appuser  43 Sep 30 03:02 partition.metadata
```

| File | What it holds |
| ---- | ------------- |
| `00000000000000000000.log` | The records. The file name is the segment's **base offset** (guide §3.2) |
| `.index` / `.timeindex` | Offset → byte-position and timestamp → offset maps. **Pre-allocated at 10 MB** for the active segment — that is not leaked space |
| `leader-epoch-checkpoint` | Leader history, used for safe truncation after leader changes |
| `partition.metadata` | The partition's topic ID |

The `*.log` is where retention, compaction and compression actually happen.
The rest of this lab watches that file.

---

## Part 3 — Time-based retention in action (15 min)

A low-traffic topic with the default 1 GiB segments could keep a segment open
for days, making "retention is broken" tickets (guide §3.2). This exercise
compresses that timeline: segments roll every **10 seconds**, retention is
**30 seconds**.

### 3.1 Create it, configure it live

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.expire.test \
  --partitions 1 --replication-factor 3 \
  --config segment.ms=10000 \
  --config retention.ms=30000

kafka-configs.sh --bootstrap-server $BS --describe \
  --entity-type topics --entity-name $ME.expire.test
```

**Expected:**

```
Dynamic configs for topic l07.expire.test are:
  retention.ms=30000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=30000}
  segment.ms=10000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:segment.ms=10000}
```

Both are `DYNAMIC_TOPIC_CONFIG` — set on this topic only, effective
immediately, no restart (Lab 01 Part 4).

### 3.2 Write three batches, 12 seconds apart

```bash
# (container)
for b in 1 2 3; do
  seq 1 5 | sed "s/^/batch$b-msg/" | kafka-console-producer.sh --bootstrap-server $BS --topic $ME.expire.test 2>/dev/null
  echo "batch $b sent at $(date +%T)"; sleep 12
done
```

Each batch lands in its own segment because `segment.ms=10000` rolls the
segment 10 s after its first append. Check the segments and offsets
immediately:

```bash
# (container)
ls /var/lib/kafka/data/$ME.expire.test-0/ | grep -E "\.log$"
kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.expire.test --time -2
kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.expire.test --time -1
```

**Expected:** three `.log` files (base offsets `0`, `5`, `10`), earliest
offset `0`, latest `15`. All 15 records exist.

> On a slow machine the first segment can already be renamed `.deleted` by the
> time these commands run (retention is 30 s and the check runs every 15 s).
> If you see only two files and `--time -2` reports `5`, nothing is wrong —
> retention simply got there first. Continue to 3.3 and watch the rest.

### 3.3 Watch retention delete them

Now watch the retention loop, which checks every **15 s** thanks to the
override (`log.retention.check.interval.ms`):

```bash
# (container) - Ctrl+C once the story has played out
while true; do date +%T; ls /var/lib/kafka/data/$ME.expire.test-0/ | grep -E "log|deleted"; echo; sleep 10; done
```

The story you should see, in order:

1. At ~30 s after its newest record, segment `0` is renamed
   **`00000000000000000000.log.deleted`** (plus `.index.deleted`,
   `.timeindex.deleted`).
2. The log start offset advances — **at rename time**:
   `kafka-get-offsets.sh --time -2` now reports `5`.
3. About **60 s later** (`file.delete.delay.ms`, guide §5.4) the
   `*.deleted` files are unlinked and the space is returned.
4. Segments `5` and `10` follow the same path, ~15 s apart, until the
   partition is empty.

Check the end state:

```bash
# (container)
kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.expire.test --time -2
kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.expire.test --time -1
kafka-console-consumer.sh --bootstrap-server $BS --topic $ME.expire.test \
  --group brand-new-group --from-beginning --timeout-ms 5000 2>/dev/null
```

**Expected:**

```
l07.expire.test:0:15
l07.expire.test:0:15
```

…and **no messages**. The log start offset moved from `0` to `15`; offsets are
never reused; and a consumer that arrives after retention has nothing to read.

### 3.4 Retention runs on every replica, independently

Open a **second terminal** on the host and look at the broker logs:

```bash
# (host) - terminal 2
docker compose -p kafka-m2 -f ../module-02/docker-compose.yml \
  -f docker-compose.override.yml logs 2>/dev/null \
  | grep "expire.test" | grep -E "Deleting segment" | cut -c1-140
```

**Expected** — three sets of lines, one per broker (`kafka-1 |`, `kafka-2 |`,
`kafka-3 |`):

```
kafka-1  | [...] Incremented log start offset to 5 due to segment deletion
kafka-1  | [...] Deleting segment LogSegment(baseOffset=0, size=151, ...) due to log retention time 30000ms breach based on the largest record timestamp in the segment
kafka-2  | [...] Incremented log start offset to 5 due to segment deletion
kafka-2  | [...] Deleting segment LogSegment(baseOffset=0, size=151, ...) due to log retention time 30000ms breach based on the largest record timestamp in the segment
kafka-3  | [...] Incremented log start offset to 5 due to segment deletion
kafka-3  | [...] Deleting segment LogSegment(baseOffset=0, size=151, ...) due to log retention time 30000ms breach based on the largest record timestamp in the segment
```

Each broker deletes its **own replica** of the partition. There is no
coordination and no single "cleanup node" — this is why a full ISR on all
three brokers means data is only truly gone when all three have done it.

| Observation | Concept | Guide |
| ----------- | ------- | ----- |
| Deletion happened only after segments rolled | Retention never touches the **active segment** | §3.2, §5.1 |
| Whole files disappeared, in steps | Deletion is **segment-granular** | §5.1 |
| `.deleted` files lingered ~60 s | `file.delete.delay.ms` recovery window | §5.4 |
| Log start offset moved, offsets not reused | `--time -2` moves forward | §5.1, Module 1 §6.2 |
| A brand-new consumer read nothing | Retention does **not** wait for consumers | §5.6 |

---

## Part 4 — Size-based retention: `retention.bytes` (15 min)

Time is not the only trigger. `retention.bytes` caps disk **per partition** —
not per topic, not per broker (guide §5.3).

### 4.1 A mistake worth making once

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.size.test \
  --partitions 1 --replication-factor 3 \
  --config segment.bytes=16384 --config retention.bytes=32768
```

**Expected:**

```
Error while executing topic command : Invalid value 16384 for configuration segment.bytes: Value must be at least 1048576
```

`segment.bytes` has a **1 MiB minimum** — Kafka refuses to fragment a log into
smaller pieces. This is the kind of constraint you only learn by hitting it;
config values always have validation ranges (the full list is in the guide's
§2.3 table).

### 4.2 The real thing

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.size.test \
  --partitions 1 --replication-factor 3 \
  --config segment.bytes=1048576 \
  --config retention.bytes=2097152 \
  --config retention.ms=-1
```

| Setting | Value | Why |
| ------- | ----- | --- |
| `segment.bytes` | 1 MiB (the minimum) | Make segments small enough for the cap to act in lab time |
| `retention.bytes` | 2 MiB | The size cap — **per partition** |
| `retention.ms` | `-1` | Never expire by age: only the size limit applies |

Produce enough data to fill several segments (~4 MiB):

```bash
# (container)
for i in $(seq 1 30000); do echo "96650000$i:sms:central:delivered:timestamp-$(date +%s%3N)-placeholder-text-to-make-this-line-longer-than-usual"; done | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.size.test 2>/dev/null

kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.size.test --time -1
ls -lh /var/lib/kafka/data/$ME.size.test-0/ | grep -E "\.log$"
du -sh /var/lib/kafka/data/$ME.size.test-0/
```

**Expected** (your numbers will differ): 30,000 records across ~4 segments:

```
l07.size.test:0:30000
-rw-r--r-- 1 appuser appuser 1020K ... 00000000000000007497.log
-rw-r--r-- 1 appuser appuser 1019K ... 00000000000000014942.log
-rw-r--r-- 1 appuser appuser 1020K ... 00000000000000022366.log
-rw-r--r-- 1 appuser appuser   29K ... 00000000000000029790.log
4.1M	/var/lib/kafka/data/l07.size.test-0/
```

### 4.3 Watch the cap act

Wait ~40 seconds (the 15 s check interval plus margin), then look again:

```bash
# (container)
kafka-get-offsets.sh --bootstrap-server $BS --topic $ME.size.test --time -2
ls /var/lib/kafka/data/$ME.size.test-0/ | grep -E "log|deleted"
du -sh /var/lib/kafka/data/$ME.size.test-0/
```

**Expected** (example numbers):

```
l07.size.test:0:14942
00000000000000007497.log.deleted
00000000000000014942.log
00000000000000022366.log
00000000000000029790.log
3.1M	/var/lib/kafka/data/l07.size.test-0/
```

A minute later (after the 60 s delete delay) the `.deleted` file is gone and
the partition settles just under the cap:

```
l07.size.test:0:14942
00000000000000014942.log
00000000000000022366.log
00000000000000029790.log
2.1M	/var/lib/kafka/data/l07.size.test-0/
```

What this shows:

- The **log start offset jumped from 0 to 14,942**: the oldest ~15,000 records
  were deleted to respect the cap, whether or not anyone consumed them.
- Deletion lands in **whole-segment steps**, so the partition stops within one
  segment of the cap — it does not trim to exactly 2 MiB.
- The cap is **per partition**: a 12-partition topic with
  `retention.bytes=2GiB` can occupy up to 24 GiB. Multiply when sizing disks
  (guide §5.3).
- Time and size limits combine as **whichever is breached first**. Here time
  is disabled (`retention.ms=-1`), so only size acts.

---

## Part 5 — Compaction: latest value per key, space reclaimed (15 min)

With `cleanup.policy=compact`, the **log cleaner** rewrites segments so that
only the latest record per key survives — a replayable table, like the current
plan of every subscriber (guide §5.5).

### 5.1 Create a compacted topic

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.subscriber.plan \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=5000
```

| Setting | Lab value | Why |
| ------- | --------- | --- |
| `cleanup.policy=compact` | — | Keep the latest record per key instead of deleting by age |
| `segment.ms` | 5 s | The **active segment is never compacted**, so it must roll quickly |
| `min.cleanable.dirty.ratio` | 0.01 | Clean as soon as 1 % of the log is "dirty" (default 50 %) |
| `delete.retention.ms` | 5 s | Minimum lifetime of tombstones (default 24 h) |

### 5.2 Write plan changes, including a delete

`NULL` maps to a real **null value** (a tombstone):

```bash
# (container)
printf "sub-001:PREPAID_10\nsub-002:POSTPAID_50\nsub-003:PREPAID_10\nsub-001:PREPAID_30\nsub-002:POSTPAID_100\nsub-001:POSTPAID_50\nsub-003:NULL\n" | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.subscriber.plan \
  --reader-property parse.key=true --reader-property key.separator=: \
  --reader-property null.marker=NULL 2>/dev/null

kafka-console-consumer.sh --bootstrap-server $BS --topic $ME.subscriber.plan \
  --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null
```

**Expected:** all seven records — they are in the **active segment**, which
the cleaner never touches:

```
Offset:0	sub-001	PREPAID_10
Offset:1	sub-002	POSTPAID_50
Offset:2	sub-003	PREPAID_10
Offset:3	sub-001	PREPAID_30
Offset:4	sub-002	POSTPAID_100
Offset:5	sub-001	POSTPAID_50
Offset:6	sub-003	null
```

### 5.3 Roll the segment and let the cleaner run

Wait more than 5 s, then write one more record to force the roll:

```bash
# (container)
sleep 6
echo "sub-004:PREPAID_10" | kafka-console-producer.sh --bootstrap-server $BS \
  --topic $ME.subscriber.plan --reader-property parse.key=true --reader-property key.separator=: 2>/dev/null
```

Wait **30–60 s** for the log cleaner, then read again:

```bash
# (container)
kafka-console-consumer.sh --bootstrap-server $BS --topic $ME.subscriber.plan \
  --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null
```

**Expected:**

```
Offset:4	sub-002	POSTPAID_100
Offset:5	sub-001	POSTPAID_50
Offset:6	sub-003	null
Offset:7	sub-004	PREPAID_10
```

> If you still see seven or eight records, wait another 30 s and try again.
> The cleaner runs in the background.

Only the **latest value per key survives**, and **offsets keep their original
numbers** — gaps remain, nothing is renumbered. The tombstone (`sub-003 →
null`) is still there so lagging consumers learn about the deletion; a later
cleaning pass may remove it once `delete.retention.ms` has passed.

### 5.4 Space reclaim at scale

Compaction's storage benefit is proportional to how many superseded records
exist. Produce 300 plan updates cycling over 5 keys:

```bash
# (container)
for i in $(seq 1 300); do echo "sub-00$(( (i % 5) + 1 )):plan-$i"; done | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.subscriber.plan \
  --reader-property parse.key=true --reader-property key.separator=: 2>/dev/null
sleep 6
echo "sub-009:PREPAID_99" | kafka-console-producer.sh --bootstrap-server $BS \
  --topic $ME.subscriber.plan --reader-property parse.key=true --reader-property key.separator=: 2>/dev/null

kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list $ME.subscriber.plan 2>/dev/null \
  | grep -oE '"partition":"l07.subscriber.plan-0","size":[0-9]+' | head -1
```

Wait **45–75 s** for the cleaner, then read the log and measure again:

```bash
# (container)
kafka-console-consumer.sh --bootstrap-server $BS --topic $ME.subscriber.plan \
  --from-beginning --timeout-ms 4000 \
  --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null

kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list $ME.subscriber.plan 2>/dev/null \
  | grep -oE '"partition":"l07.subscriber.plan-0","size":[0-9]+' | head -1
```

**Expected** (your offsets will differ; the shape will not):

```
Offset:303	sub-002	plan-296
Offset:304	sub-003	plan-297
Offset:305	sub-004	plan-298
Offset:306	sub-005	plan-299
Offset:307	sub-001	plan-300
Offset:308	sub-009	PREPAID_99
```

```
"partition":"l07.subscriber.plan-0","size":383
```

The size dropped from **~7 KB to 383 bytes** (~18×). The five keys' latest
values survived; the earlier records — including the old `POSTPAID_*` values
and the tombstone, all superseded — are gone. This is why state topics cost
disk proportional to their **distinct keys**, not their throughput.

> **What if the compacted topic keeps growing anyway?** Keyless records can
> never be deduplicated and accumulate forever, and a dirty ratio that is
> rarely reached leaves the cleaner idle (guide §5.5). Fixes: require keys,
> lower `min.cleanable.dirty.ratio`, set `max.compaction.lag.ms`, or raise
> `log.cleaner.threads` — the guide's §7.5 checklist.

---

## Part 6 — Compression: the highest-leverage storage knob (10 min)

Compression applies to the **whole batch** and shrinks both disk and
replication traffic (guide §7.2). Measure it on identical data:

```bash
# (container)
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.cdr.none \
  --partitions 1 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --create --topic $ME.cdr.zstd \
  --partitions 1 --replication-factor 3

for i in $(seq 1 1000); do echo "96650000000$((i % 100)):call-start:msc-central:region-riyadh:voice:duration-180:quality-4"; done | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.cdr.none 2>/dev/null

for i in $(seq 1 1000); do echo "96650000000$((i % 100)):call-start:msc-central:region-riyadh:voice:duration-180:quality-4"; done | \
  kafka-console-producer.sh --bootstrap-server $BS --topic $ME.cdr.zstd \
  --compression-codec zstd 2>/dev/null
```

Same payload, same producer, one difference: the codec. Check which codec is
stored and what the files weigh:

```bash
# (container)
kafka-dump-log.sh --print-data-log --files /var/lib/kafka/data/$ME.cdr.zstd-0/00000000000000000000.log 2>/dev/null \
  | grep -m1 -oE "compresscodec: [a-z]+"

ls -l /var/lib/kafka/data/$ME.cdr.none-0/*.log /var/lib/kafka/data/$ME.cdr.zstd-0/*.log | awk '{print $5, $9}'
```

**Expected:**

```
compresscodec: zstd
88882 /var/lib/kafka/data/l07.cdr.none-0/00000000000000000000.log
 4863 /var/lib/kafka/data/l07.cdr.zstd-0/00000000000000000000.log
```

**~18× smaller** on disk for identical records, and the replication traffic
between brokers shrinks by the same factor, because replicas transfer the
compressed batches. Three rules from the guide (§7.2):

1. Compress at the **producer** (`--compression-codec` here; the
   `compression.type` producer property in real applications).
2. Leave the topic's `compression.type` at `producer`: brokers then store
   batches as sent and **never recompress**. Setting a codec at topic level
   makes brokers decompress-and-recompress every differently-coded batch — a
   CPU tax on the data path.
3. `zstd` or `lz4` are the modern choices; `gzip` is rarely worth the CPU.

---

## Checkpoint questions

<details>
<summary>1. A topic has <code>retention.ms=30000</code> but its first segment is still on disk ten minutes later. Name two reasons, both shown in this lab.</summary>

Retention only acts on **closed** segments, so the active segment is exempt —
on a low-traffic topic with the default 1 GiB `segment.bytes` it can stay open
for days. And even after a segment is renamed `.deleted`, the file survives
`file.delete.delay.ms` (60 s by default) before the space is returned. Lower
`segment.ms`/`segment.bytes` for such topics if deletion precision matters.
</details>

<details>
<summary>2. Why did the <code>.deleted</code> files appear on all three brokers at about the same time, and who coordinated that?</summary>

Nobody. The retention loop runs independently inside every broker, and each
broker deletes its **own replica** of the partition. They appeared together
because all three received the same topic config at the same time and all
three hold segments with the same timestamps. There is no cleanup coordinator.
</details>

<details>
<summary>3. <code>retention.bytes=2097152</code> left the partition at ~2.1 MB rather than exactly 2 MiB. Why is it "close but not exact", and how do you budget disk for a 12-partition topic?</summary>

Deletion is segment-granular: the broker removes whole segments until the
next removal would drop the log under the cap (the active segment is never a
candidate), so the log lands within one segment of the limit. And the cap is
**per partition**: budget 12 × the cap for a 12-partition topic, plus the
internal topics.
</details>

<details>
<summary>4. After the bulk update, the compacted topic kept offsets 303–308 and dropped 0–302. Why do consumers care that offsets are preserved rather than renumbered?</summary>

Committed offsets and offset-based references (seeks, reset tools, CDC
checkpoints) are written against the original numbers. If compaction
renumbered records, every committed offset would silently point at the wrong
record. Consumers must instead expect **gaps** — contiguous offsets are not
guaranteed on any Kafka topic.
</details>

<details>
<summary>5. Why did the identical payload compress ~18× with <code>zstd</code>, and what is the catch with setting <code>compression.type=gzip</code> on the topic itself?</summary>

The payload is highly repetitive text, and compression works on the whole
record **batch**, so redundancy across records is exploited too. A topic-level
codec tells brokers to decompress and recompress every batch that arrives in a
different codec — a large CPU cost on every write path. Set the codec in the
producer and leave the topic at `compression.type=producer`.
</details>

<details>
<summary>6. The override file changed the retention check interval from 5 minutes to 15 seconds. Why would you never do that on a production cluster?</summary>

The retention loop evaluates every partition's segments on each run. Running
it 20× more often multiplies CPU and I/O across all brokers for no meaningful
benefit — production retention windows are days, so a 5-minute evaluation
granularity is already far finer than the business needs.
</details>

---

## Clean up

Delete the lab topics (your own cluster, your own data), but leave the
cluster running — Lab 03 needs it:

```bash
# (container)
for t in storage.test expire.test size.test subscriber.plan cdr.none cdr.zstd; do
  kafka-topics.sh --bootstrap-server $BS --delete --topic $ME.$t 2>/dev/null
done
kafka-topics.sh --bootstrap-server $BS --list
```

**Expected:** `__consumer_offsets` (the internal topic) and nothing else.

**Next:** [Lab 03 — Durability under acks & min.insync.replicas](lab-03-durability-acks-min-insync-replicas.md), where
you stop a broker and watch `acks=all` and `min.insync.replicas` decide which
writes survive.
