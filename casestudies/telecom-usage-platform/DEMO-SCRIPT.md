# Demo Script — TelcoPulse on Kafka

> **Audience:** learners who have finished, or are finishing, Modules 1–3
> **Duration:** about 70 minutes with discussion (parts can be run on their own)
> **Companion:** [`README.md`](README.md) explains the design; this file is the walk-through
> **Every number below was observed on the lab cluster.** Yours will differ in
> detail (offsets, byte counts) but not in shape.

## How to use this script

Each part has the same shape:

| Block | Purpose |
| ----- | ------- |
| **Say** | The point to make, in a sentence or two |
| **Do** | Buttons to press or commands to run |
| **Expect** | What the audience should see |
| **Then show the CLI** | The same fact from the tool they know, so the app is not magic |
| **Ask** | A question to check understanding |

Two windows side by side work best: the dashboard on
<http://localhost:8090/>, and a terminal with `./scripts/cluster.sh cli` (a shell in
`kafka-1`, where `$BS` is set). Keep the application log visible too.

| # | Part | Module | Time |
| - | ---- | ------ | ---- |
| 0 | Set-up | 2 | 5 min |
| 1 | The design on paper | 3 §4 | 8 min |
| 2 | Keys, partitions and order | 1 §5 | 10 min |
| 3 | Consumer groups, offsets and lag | 1 §6 | 12 min |
| 4 | Where a setting comes from, and changing it live | 3 §2, §4 | 8 min |
| 5 | Compaction: the topic as a table | 1 §7, 3 §5.5 | 10 min |
| 6 | Retention and segments | 3 §3, §5 | 8 min |
| 7 | Durability under failure | 3 §6 | 10 min |
| 8 | Wrap-up and clean-up | — | 5 min |

---

## Part 0 — Set-up (5 min)

Do this **before** the session. Starting the cluster takes about a minute.

```bash
# (host) - from casestudies/telecom-usage-platform
./scripts/cluster.sh up
mvn -q package
java -jar target/telecom-usage-platform-1.0.0.jar
```

Open <http://localhost:8090/>. Within about 15 seconds the header shows the
cluster ID, the active controller and three brokers as **up**, and the log shows:

```
... billing: partitions assigned: [cdr.data-0, cdr.data-1, cdr.sms-0, cdr.voice-0, cdr.voice-1]
... fraud-detection: partitions assigned: [cdr.voice-0, cdr.voice-1]
... usage-analytics: partitions assigned: [...]
```

Press **Seed subscriber plans** once. It gives the 20 demo subscribers
(`966500000001` … `966500000020`) a starting plan.

> **Fresh state matters.** If you rehearsed earlier, run `./scripts/cluster.sh reset`
> and start again so topics and counters begin empty.

---

## Part 1 — The design on paper (8 min) · Module 3 §4

**Say.** "One cluster, many kinds of data. Before any traffic flows, every topic
has a written contract. Billing data must survive a broker failure. Telemetry
must be cheap. Subscriber state must keep only the latest value. The broker
defaults are just a baseline."

**Do.**
1. In the **Topic designs** table, click `cdr.voice`, then `network.telemetry`,
   then `subscriber.plan`. The **Why these settings?** panel changes each time.
2. Point at the columns: partitions / RF, min ISR, retention, cleanup, segment.

**Expect.** Three different contracts, each with a reason:

| Topic | Points to make |
| ----- | -------------- |
| `cdr.voice` | RF 3 + `min.insync.replicas=2`: one broker can fail and writes still succeed |
| `network.telemetry` | RF 2, min ISR **1**, 1 h retention, `compression.type=producer` |
| `subscriber.plan` | `cleanup.policy=compact`: retention shows "kept (compacted)" |

**Then show the CLI.** The app created these topics at startup (Module 2, Lab 03:
`NewTopic` beans). Confirm with the tool:

```bash
# (container)
kafka-topics.sh  --bootstrap-server $BS --describe --topic cdr.voice
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name cdr.voice
```

The `Configs:` line shows exactly the values in the table.

**Ask.**
- *Why does `cdr.data` have `retention.bytes` of 5 GiB and yet the topic can hold 30 GiB?* (It is per partition: 6 × 5 GiB.)
- *Why is telemetry's min ISR 1 when the cluster default is 2?* (A deliberate opt-down. Explicit beats inherited.)

---

## Part 2 — Keys, partitions and order (10 min) · Module 1 §5

**Say.** "Every CDR uses the subscriber's number as its key. The key decides the
partition. So one subscriber always lands in one partition, and Kafka keeps the
order inside a partition."

**Do.**
1. Press **Send 100 CDRs** and read the response in the **Activity log**:
   `cdr.voice-0: 12, cdr.voice-1: 9, ...`. Different subscribers spread over
   partitions.
2. Press **Fraud burst (966500000007)**. It sends 8 international calls for one
   subscriber.
3. In **Downstream results**, watch **fraud alerts** become 1. The log shows
   `FRAUD ALERT 966500000007 made 5 international calls within 60s`.
4. Press **Send 100 CDRs** again and compare the partition counts.

**Expect.** After the burst, one voice partition gained 8 more records than
the others. The alert fires on the **5th** call, not the 8th and not once per
call. The rule keeps a per-subscriber window, and it only works because the
calls arrive in order.

**Then show the CLI.**

```bash
# (container) the same subscriber, always the same partition
kafka-console-consumer.sh --bootstrap-server $BS --topic cdr.voice \
  --from-beginning --timeout-ms 5000 \
  --formatter-property print.partition=true --formatter-property print.key=true \
  | grep 966500000007
```

Every line shows the same partition number.

**Ask.**
- *What would break in the fraud rule if producers used a random key?* (Calls of one subscriber would be spread over consumers; each would see too few and the burst would go unnoticed.)
- *What is the cost of key = MSISDN?* (A very heavy subscriber makes a hot partition.)

---

## Part 3 — Consumer groups, offsets and lag (12 min) · Module 1 §6

**Say.** "Three applications read the same CDR topics. Each has its own group,
so each gets **every** record. Inside a group the partitions are shared. That is
both a topic and a queue, at once."

### 3.1 Groups side by side

**Do.** Read the **Consumer groups** panel.

**Expect.**

| Group | Members | What to notice |
| ----- | ------- | -------------- |
| `billing` | 3 threads, 5 partitions each | All 15 partitions of `cdr.voice`, `cdr.sms`, `cdr.data` covered, none twice |
| `fraud-detection` | 3 threads, 2 partitions each | Only `cdr.voice` |
| `usage-analytics` | 2 threads | Reads everything, with a different split |

All three show **lag 0** and state **Stable**.

```bash
# (container)
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group billing
```

### 3.2 Lag appears in one group only

**Do.**
1. Press **Start 50 CDR/s**.
2. In *Consumers*, press **Slow (200 ms/record)**.
3. Watch **Consumer groups** for about 20 seconds.

**Expect.** `billing` lag climbs (hundreds, then thousands). `fraud-detection`
and `usage-analytics` stay at 0: a slow billing system cannot delay fraud
detection.

### 3.3 Stop and resume

**Do.**
1. Press **Stop** next to `billing`. Its state changes to **Empty**, members
   disappear, lag keeps growing.
2. Press **Full speed**, then **Start** next to `billing`.

**Expect.** Billing rejoins as **Stable**, resumes from its committed offsets
(no record is skipped) and the lag drains to 0. Press **Stop** in *The network*
when done.

**Then show the CLI.** `kafka-consumer-groups.sh --describe --group billing`
shows `CURRENT-OFFSET`, `LOG-END-OFFSET` and `LAG`, the same three numbers the
dashboard shows.

**Ask.**
- *If billing was down for 3 days, what would happen?* (It catches up, as long as `retention.ms` on `cdr.voice` (7 days) is longer than the outage. Retention does not wait for consumers.)
- *What happens if I add a fourth billing thread?* (Nothing extra to do: 3 threads share 15 partitions; a fourth only helps up to the partition count.)

---

## Part 4 — Where a setting comes from, and changing it live (8 min) · Module 3 §2, §4

**Say.** "Every effective value has a source. Topic overrides win over
everything. And changing one needs no restart and no downtime."

**Do.**
1. Click `cdr.sms` in *Topic designs*. In *Why these settings?* the
   `retention.ms` row shows **3 d** with source `DYNAMIC TOPIC` (set explicitly);
   `segment.bytes` shows `DEFAULT` (nobody set it).
2. In **Change a setting, live** choose `cdr.sms`, `retention.ms`, value
   `86400000`, press **Apply (no restart)**.
3. The row now reads **1 d** with a red **drift: catalog says 3 d** badge.

**Then show the CLI, and the audit trail.**

```bash
# (container)
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name cdr.sms
kafka-console-consumer.sh --bootstrap-server $BS --topic audit.events \
  --from-beginning --timeout-ms 5000
```

The audit topic holds a `TOPIC_CONFIG_CHANGED` event: `retention.ms: 259200000 -> 86400000`.
Point out that this record lives 90 days.

**Then restart the application** (Ctrl+C, run it again). Refresh the page.
`cdr.sms` is back to **3 d**: `KafkaAdmin` restored the value the catalog declares
(`spring.kafka.admin.modify-topic-configs=true`).

> **The boundary of that safety net.** `KafkaAdmin` sets what the catalog
> **declares**. It never **removes** an override the catalog does not declare.
> Part 6 shows this happening. The dashboard flags such a setting as
> **override not in catalog**.

**Ask.**
- *Try to set `log.retention.ms` on the topic.* (The API refuses: topic-level names have no `log.` prefix. Module 3 §4.2.)
- *Which of these needs a broker restart: `retention.ms`, `log.dirs`, `min.insync.replicas`?* (Only `log.dirs`.)

---

## Part 5 — Compaction: the topic as a table (10 min) · Module 1 §7, Module 3 §5.5

**Say.** "`subscriber.plan` holds the *current* plan of each subscriber. Plans
change, but the topic keeps only the latest record per key. A new service
instance rebuilds the whole table by reading it. No database dump needed."

In the lab, `subscriber.plan` uses shortened timings (`segment.ms` 30 s,
dirty ratio 0.01) so this works in about two minutes. Production would use hours.

**Do.**
1. *State on a compacted topic*: press **Rewrite every plan 5×**. The topic now
   holds 20 seeded + 100 rewritten records for 20 keys.
2. Press **Count what is really in the topic**. Read the Activity log:

   ```json
   { "recordsRead": 120, "distinctKeys": 20, "liveKeys": 20, "tombstones": 0 }
   ```

3. Wait **35 seconds**. Then press **Rewrite every plan once**. This is the
   step people miss: compaction only cleans **closed** segments, and a segment
   rolls only when a new record arrives after `segment.ms`. Your 120 records
   sit in segments that this one write closes.
4. Wait **about 60 seconds**, then press **Count what is really in the topic**
   again.

**Expect.** `distinctKeys` is still **20**, but `recordsRead` has dropped from
140 to about **40**: one surviving record per key, plus the 20 newest ones in
the active segment, which is never cleaned. The cleaner runs on its own
schedule, so if you still see a higher number, wait 30 seconds and count again.

**Then the tombstone.** Press **Terminate 966500000020**, then *Count*:
`tombstones: 1`, `liveKeys: 19`. A `null` value means "this key no longer
exists"; compaction removes it once it has been kept for `delete.retention.ms`.

**Then the rebuild.** Restart the application. In *State on a compacted topic*:

> In memory: **20** subscribers. Records read from the topic since this app
> started: **~40**.

The service rebuilt its full table from the topic and read far fewer than
the 120+ records ever written.

**Then show the CLI.**

```bash
# (container) count records physically in the topic
kafka-console-consumer.sh --bootstrap-server $BS --topic subscriber.plan \
  --from-beginning --timeout-ms 5000 --formatter-property print.key=true | wc -l
```

**Ask.**
- *Why did nothing shrink right after the first rewrite?* (Everything was still in the active segment, which is never compacted.)
- *What if the producer forgot the key?* (Records without a key cannot be compacted. They stay forever.)

---

## Part 6 — Retention and segments (8 min) · Module 3 §3, §5

**Say.** "Retention deletes whole closed segments, not individual records, and
it never asks whether anyone has read them. Watch the log start offset."

**Do.**
1. *Retention on telemetry*: press **Shorten retention (60 s) + small segments**.
   This sets `retention.ms=60000`, `segment.bytes=65536`, `segment.ms=20000`
   dynamically. No restart.
2. Press **Send 20,000 points** in the same panel. The table shows log start 0
   and the log end growing to about 20,000 in total.
3. Wait **about 75 seconds** so the first batch is older than 60 s.
4. Press **Send 2,000 telemetry points** in *The network*.

**Expect.** The first batch is gone: **log start offset** has moved forward to
where the second batch begins, and *records still readable* equals only the
second batch. **Log end** never moves back: offsets are not reused. (When
*every* record in a partition is older than retention, the broker rolls the
active segment and removes it too, so log start jumps to the log end.) Look at
*Storage on the brokers*: the telemetry footprint shrinks.

**Then show the lifecycle.** `retention.check.interval.ms` is 15 s on this
cluster (the Module 3 override), so deletion happens within about 15–30 s after
the data expires, then files are removed after `file.delete.delay.ms` (60 s).
On a default cluster this check runs every 5 minutes.

**Then the restore, and the lingering override.** Press **Restore**. Open
`network.telemetry` in *Why these settings?*: `retention.ms` and `segment.bytes`
are back to the declared values, and the extra `segment.ms` override (which
the catalog does not declare) has been removed. Had you only restarted the
application, `segment.ms` would still show **override not in catalog**.

**Also point at the storage panel.** CDR topics show equal bytes on each
broker (RF 3). Telemetry is smaller per record: `lz4` at the producer, stored
as sent because the topic says `compression.type=producer`. Ask: *what would
happen if the topic said `zstd` while the producer sent `lz4`?* (The broker
would decompress and recompress every batch: CPU cost on the data path.)

```bash
# (container)
kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list network.telemetry
kafka-get-offsets.sh --bootstrap-server $BS --topic network.telemetry --time earliest
```

**Ask.**
- *Retention is 1 h but I see 3-hour-old data on a quiet topic. Why?* (The active segment has not rolled: lower `segment.ms`.)
- *A colleague sets `retention.bytes` to 10 GiB on a 12-partition topic and expects 10 GiB in total. What do they get?* (Up to 120 GiB: the limit is per partition.)

---

## Part 7 — Durability under failure (10 min) · Module 3 §6

**Say.** "Durability is a contract between the producer's `acks` and the topic's
`min.insync.replicas`, and the only moment it matters is during a failure.
Let's create one."

**Do.**

1. *Durability drill* → press **Write acks=all**. The panel shows
   `ISR [1,2,3]`, `min.insync.replicas 2`, and the record is readable.
2. Stop a broker:

   ```bash
   # (host)
   docker stop kafka-2
   ```

   Within about 15 seconds the header shows **broker 2 DOWN**, and the drill
   panel shows the ISR shrinking to two members.
3. Press **Write acks=all** again. **It still succeeds**: ISR of 2 meets the
   minimum of 2.
4. Raise the bar. In *Change a setting, live* choose `drill.durability`,
   `min.insync.replicas`, value **3**, press **Apply**. The drill panel turns red:
   **ISR below minimum: acks=all is rejected**.
5. Press **Write acks=all**. The Activity log shows an error:
   `NotEnoughReplicasException`. The cluster refuses the write, by design.
6. Press **Write acks=1**. It **succeeds** (an offset comes back).
7. Look at the drill panel: the **high watermark did not move**, and the
   record you just wrote is **not readable**. Kafka 4.x holds the high
   watermark while the ISR is below the minimum (strict min ISR rule).
8. Restore. Set `min.insync.replicas` back to **2** and start the broker:

   ```bash
   # (host)
   docker start kafka-2
   ```

   After about 20 seconds the ISR is `[1,2,3]` again, the high watermark
   advances, and the `acks=1` record becomes readable.

**Expect.** The same three facts the guide states (§6.4): `acks=all` is only
as strong as `min.insync.replicas`; `min.insync.replicas` gates only `acks=all`;
an `acks=1` write can be acknowledged and still be lost.

> **Why raise to 3 and not 4?** Kafka caps the effective minimum at the
> replication factor, so `min.insync.replicas=4` on an RF 3 topic behaves like
> 3. With all brokers up, ISR = 3 satisfies it and nothing is rejected. The
> rejection needs a **real** shortage of in-sync replicas: hence the stopped broker.

**Then show the CLI.**

```bash
# (container) run against kafka-1 while kafka-2 is stopped
kafka-topics.sh --bootstrap-server $BS --describe --topic drill.durability
```

`Isr:` lists two brokers, `Replicas:` still lists three.

**Ask.**
- *Which topics in this platform would you never let a producer write to with `acks=1`?* (Billing, charges, audit, plans.)
- *Telemetry uses `acks=1`. What did that buy?* (Lower latency and no dependence on followers, at the price of possible loss.)

---

## Part 8 — Wrap-up and clean-up (5 min)

### Recap

| You saw | Feature | Module |
| ------- | ------- | ------ |
| Subscriber in one partition, fraud rule correct | Key → partition, order per partition | 1 |
| Three groups, independent lag | Consumer groups, offsets | 1 |
| Billing catches up after an outage | Retained log, committed offsets | 1 |
| Topics created at start-up from code | `KafkaAdmin`, `NewTopic` | 2 |
| Every panel from the Admin API | Same API as the CLI tools | 2 |
| A contract per topic, with reasons | Topic-level overrides | 3 |
| Setting source, live change, audit event | Config precedence, dynamic config | 3 |
| Plan table rebuilt from far fewer records | Compaction, tombstones | 1, 3 |
| Log start offset moving forward | Retention, segments | 3 |
| Write rejected, `acks=1` accepted but invisible | `acks` + `min.insync.replicas`, strict min ISR | 3 |

### Questions to leave with the class

1. Billing currently rates a CDR twice if the consumer crashes after rating and before committing. What would you change? (Module 4.)
2. Add a fourth consumer application, say `roaming-partners`. Which parts of the platform do you touch? (None but the new app: give it a group id.)
3. The regulator now wants 12 months of CDRs but disks are sized for 7 days. What do you look at? (Tiered storage, Module 3 §7.6.)

### Clean-up

```bash
# (host) stop the app with Ctrl+C, then
./scripts/cluster.sh down          # keep data
./scripts/cluster.sh reset         # or: delete all topics and data
```

---

## Facilitator notes

| Situation | What to do |
| --------- | ---------- |
| A part depends on a wait (compaction, retention) | Start the wait, then talk through the design of the topic while it runs |
| The alert does not fire | The simulator may have run for a while; the alert triggers once per window per subscriber. Use a different `msisdn` in the API |
| Lag does not grow in 3.2 | The simulator is off: press **Start 50 CDR/s** first |
| A learner asks "why not exactly-once?" | Park it: Module 4. Point at `chargeId` as the reason duplicates are detectable |
| Only one laptop cluster and many learners | Each learner starts the case study on their own machine, or on their VM with the `shared` profile (Parts 1–6 only) |
| `docker stop kafka-2` shows a different broker as leader | Expected: leadership moves. Note the new leader in `kafka-topics.sh --describe` |
