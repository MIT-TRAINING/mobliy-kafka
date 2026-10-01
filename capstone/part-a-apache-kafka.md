# Capstone Part A — Building on Apache Kafka

> **Back to:** [Capstone overview](README.md) · **Next:** [Part B — Confluent Kafka](part-b-confluent-kafka.md)
> **Phases:** A1 (after Module 2) · A2 (after Module 3) · A3 (after Module 4) · A4 (after Module 5)

In Part A you design the platform, build it on your own 3-node cluster, move
it to the shared Apache Kafka cluster on AWS, write the applications, and
prove it survives failures. Requirement IDs (`BR-…`, `NFR-…`) refer to
[§4 of the overview](README.md#4-requirements).

Every phase has the same shape:

- **Goal** — what you are trying to achieve.
- **Requirements** — what must be true when you finish (`A1.1`, `A1.2`, …).
- **Acceptance criteria** — how the trainer checks it. Each needs evidence.
- **Questions to answer** — put the answers in your design doc or runbook.
- **Stretch** — optional, for extra credit.

Environment variables used below (define them in every terminal):

```bash
APACHE=apache-kafka.lab.internal:9092   # shared Apache Kafka cluster
CFG=~/kafka/apache.properties           # your SASL/SCRAM settings
ME=lNN                                  # your learner prefix
```

---

## Phase A1 — Design & local cluster

| | |
| --- | --- |
| **Opens after** | Module 2 |
| **Modules applied** | 1 (messaging concepts, architecture, KRaft), 2 (installation, CLI, topic admin) |
| **Environment** | Your VM: a 3-node KRaft cluster in Docker |
| **Effort** | 2–3 hours |

### Goal

Turn the business problem into a Kafka design, and stand up a cluster you
fully control to try it on.

### Requirements

**A1.1 — Messaging design note.** One page in `docs/design.md`:

- Why Kafka rather than a queue (IBM MQ or similar) for this platform. Use
  BR-10 and BR-05 in the argument; be specific about what a queue would and
  would not give you.
- Which topics are **pub-sub** (several independent consumer groups) and
  where a **queue-like** consumer group is used. Name the groups.

**A1.2 — Topic catalog.** For each of the 8 topics in
[§3.3](README.md#33-topic-catalog-names-are-fixed-settings-are-yours), a table
row with: key, partitions, replication factor, `min.insync.replicas`,
`cleanup.policy`, retention, compression, and **the requirement IDs that
drove each choice**. Partition counts must support NFR-01.

**A1.3 — Partitioning rules.** Explain in your own words:

- How the key gives you BR-02.
- Which topics the charging engine reads **together** and why their
  partition counts and keys must line up (**co-partitioning**). What breaks
  if someone later adds partitions to only one of them?

**A1.4 — Local cluster.** A 3-node KRaft cluster on your VM in Docker
Compose. You may start from the Module 2 compose file, but your copy must:

- Have **every** setting explained by a comment in your own words.
- Disable automatic topic creation.
- Use cluster defaults that suit the money topics (RF, `min.insync.replicas`).
- Be reachable both from containers and from your VM.

**A1.5 — Topic creation script.** `scripts/create-topics.sh` creates all 8
topics with the settings from A1.2. It takes the bootstrap server, a
`--command-config` file and the prefix as parameters, and is **safe to
re-run** (a second run does not fail and does not change anything).

**A1.6 — Contracts by hand.** Using only the Kafka CLI:

- Produce 10 subscriber profiles and 20 usage events **with keys**, following
  the JSON contracts in [§3.4](README.md#34-event-contracts).
- Show that all events for one MSISDN landed in one partition.
- Read `usage` with **two different consumer groups** and show both get every
  event (BR-10); then start two members in **one** group and show they split
  the partitions.

### Acceptance criteria

- [ ] `docker compose ps` shows 3 healthy nodes; the KRaft quorum shows 3 voters and a leader.
- [ ] `kafka-topics.sh --describe` output for all 8 topics matches your catalog table.
- [ ] Running `create-topics.sh` twice in a row succeeds both times with no changes the second time.
- [ ] Console output proving one-key-one-partition for at least two MSISDNs.
- [ ] `kafka-consumer-groups.sh --describe` output for two groups on `usage` (pub-sub) and for one group with two members (partitions split).
- [ ] `docs/design.md` has A1.1–A1.3 with requirement IDs.

### Questions to answer

1. Your cluster runs combined broker+controller nodes. Why would production separate them? What does KRaft remove compared with ZooKeeper?
2. Which of your topics would you **never** give RF = 1, even in a test environment? Why?
3. A colleague proposes keying `usage` by `cellId` "to balance load". What requirement breaks?

### Stretch

- Add a `scripts/describe-platform.sh` that prints a one-screen health summary of your topics (partitions, leaders, ISR size per topic).

---

## Phase A2 — Topic contracts & durability

| | |
| --- | --- |
| **Opens after** | Module 3 |
| **Modules applied** | 3 (broker/topic configuration, segments, retention, compaction, `acks`, `min.insync.replicas`, storage) |
| **Environment** | Shared Apache cluster (your `$ME.*` topics) + your local cluster for anything that needs broker control |
| **Effort** | 2–3 hours |

### Goal

Make each topic's configuration a **contract** the business can rely on, and
prove with experiments that the contracts hold.

### Requirements

**A2.1 — Platform on the shared cluster.** Run your `create-topics.sh`
against `$APACHE` with `$CFG`. Record which broker-level defaults your topics
inherit and which you override at topic level (`kafka-configs --describe`
shows the source of each value).

**A2.2 — Retention contracts.** Topic-level settings implement BR-06, BR-07,
BR-09 and BR-11. For the KPI topic, also choose a segment size/time so that
1-hour retention can actually be enforced (explain why segment settings
matter for retention).

**A2.3 — Compaction in action.** On `subscribers` or `balance`:

- Produce several versions of the same keys, including a **tombstone**
  (deleting a subscriber).
- Use topic-level settings to make compaction run within minutes on your
  topic, observe it, then **restore** production-appropriate values.
- Show the before/after: what a new consumer reading from the beginning sees.

**A2.4 — Durability matrix.** On your **local** cluster, measure what happens
to writes for each combination below, with all 3 brokers up and with **one**
and **two** brokers stopped. Record: did the write succeed, what error, could
data be lost?

| Producer `acks` | Topic `min.insync.replicas` |
| --------------- | --------------------------- |
| `0` | 2 |
| `1` | 2 |
| `all` | 1 |
| `all` | 2 |

From the matrix, state which combination each topic uses and why (BR-08, BR-09).

**A2.5 — Storage sizing.** In `docs/capacity-plan.md`, estimate the disk
needed **per broker** for production (§4.3 volumes) for `usage`, `charges`,
`balance` and `network.kpi`, given your RF, retention and an assumed
compression ratio (state your assumption, then measure a real ratio in A3 and
update the estimate). Show the formula.

### Acceptance criteria

- [ ] `kafka-configs --describe --all` excerpts for every topic, showing the overrides and their source (`DYNAMIC_TOPIC_CONFIG` vs defaults).
- [ ] Compaction evidence: dumped segments or consumer output before and after, including the tombstone disappearing.
- [ ] The durability matrix table with the actual error names you saw (e.g. `NOT_ENOUGH_REPLICAS…`).
- [ ] A worked storage estimate with numbers and units.
- [ ] Your topics are back on production-appropriate settings at the end of the phase.

### Questions to answer

1. With RF 3 and `min.insync.replicas` 2, how many brokers can fail before (a) money writes stop, (b) acknowledged money data can be lost?
2. Why is `acks=all` with `min.insync.replicas=1` not enough for BR-08?
3. `balance` is compacted. Why must it **not** also be time-retained at 7 days? Is `compact,delete` ever right here?
4. Where on a broker do your topic's segments live, and which files make up one segment?

### Stretch

- Measure the effect of `compression.type` on one day's simulated `usage` size (`kafka-log-dirs.sh`) for `none`, `lz4` and `zstd`.

---

## Phase A3 — The charging pipeline

| | |
| --- | --- |
| **Opens after** | Module 4 |
| **Modules applied** | 4 (producer tuning, consumer groups, offsets, rebalancing, delivery semantics, error handling) |
| **Environment** | VS Code on your VM; develop against your local cluster, then run against the shared Apache cluster |
| **Effort** | 5–6 hours |

### Goal

Build components C1–C6 from [§3.1](README.md#31-components-you-will-build)
and prove BR-01 to BR-06 hold — including when things crash.

### Functional requirements

**C1 Subscriber loader**

- **A3.1** Loads N subscribers (configurable, default 500) into `subscribers`, spread across all three plans and all regions, and gives each an opening top-up (e.g. 50 SAR) on `topups`.

**C2 Network event simulator**

- **A3.2** Produces usage events and top-ups at a configurable **rate** (events/s) for a configurable **duration** (or until `Ctrl+C`), following the contracts exactly.
- **A3.3** Configurable mix: % top-ups, % roaming. Realistic units (calls 10–600 s, data 1–200 MB).
- **A3.4** A `bad-every=N` option sends every Nth usage event as one of: malformed JSON, missing `msisdn`, an MSISDN that is not a subscriber.
- **A3.5** Producer settings are configurable at start-up without recompiling, and the chosen settings are logged.

**C3 Cell KPI feed** (may be part of C2)

- **A3.6** One sample per cell per second for at least 30 cells, with producer settings that match BR-09 — deliberately different from the money producers.

**C4 Online charging engine** — the heart of the capstone

- **A3.7** Runs as a **consumer group**; you can start 1 to 6 instances (NFR-01). Each instance has a unique, stable instance number.
- **A3.8** Prices usage with the tariffs in [§3.5](README.md#35-tariffs-halalas), debits/credits the balance, and emits `charges`, `balance` and `notifications` as specified.
- **A3.9** Keeps balances **in memory** for the partitions it owns. On start-up and on every rebalance, it **rebuilds** them for newly assigned partitions from `balance` — not from `usage` (BR-05).
- **A3.10** Sends bad records to `usage.dlt` with the reason and origin in headers, and carries on (BR-06).
- **A3.11** Charges **exactly once** (BR-01): reading input, writing all outputs, and committing input offsets must be one atomic unit. A crash at any moment must not produce a duplicate or missing charge, nor a wrong balance.
- **A3.12** Has a switch to run **at-least-once** instead, so you can demonstrate the difference.
- **A3.13** Refuses to start, with a clear message, if the topics it reads together are **not co-partitioned** (check with the Admin API).
- **A3.14** Logs partitions assigned/revoked, where each partition starts, and a periodic throughput summary (NFR-06). Shuts down gracefully (NFR-05).

**C5 Notification service**

- **A3.15** Its own consumer group on `notifications`; prints one "SMS" line per notification. Must never print notifications from aborted transactions.

**C6 Reconciliation tool**

- **A3.16** Reads `usage`, `topups`, `charges`, `balance` and `usage.dlt` to the end and reports:
  - valid usage events, charges, DLT records;
  - **duplicates**: usage events with more than one charge;
  - **missing**: valid usage events with no charge and not in the DLT;
  - **balance mismatches**: subscribers whose latest balance ≠ top-ups − charged amounts;
  - total revenue in SAR.
- **A3.17** Exits non-zero if any duplicate, missing or mismatch is found, so it can be used in scripts.

### Non-functional requirements for this phase

- NFR-03 and NFR-04: the connection file is the **only** thing that changes between your local cluster and the shared cluster.
- Measure and record the producer's batching and compression metrics (`batch-size-avg`, `records-per-request-avg`, `compression-rate-avg`, throttle time) for at least two `linger.ms`/`batch.size` settings. Pick one and justify it against BR-04.

### Required experiments

| # | Experiment | Expected result |
| - | ---------- | --------------- |
| E1 | Seed, simulate 2 min at 20 events/s with 3 charging instances, stop the simulator, wait for lag 0, reconcile | Clean reconciliation |
| E2 | Start with 1 instance, scale to 3, then stop one with `Ctrl+C` | Partition movement visible in logs and `--describe --members`; clean reconciliation |
| E3 | **At-least-once mode**: `kill -9` an instance mid-run, restart it, reconcile | You find (and can explain) duplicates and balance mismatches |
| E4 | **Exactly-once mode**: repeat E3 at least three times | Zero duplicates, zero missing, zero mismatches every time |
| E5 | Simulate with `bad-every=50` | Charging never stalls; every bad record is in the DLT with a reason; reconciliation clean |
| E6 | Stop all charging instances for 2 minutes while the simulator runs, then start them | Lag grows, then recovers; record the catch-up time |

### Acceptance criteria

- [ ] All 6 components build and run against the **shared Apache cluster** under your prefix.
- [ ] Evidence for E1–E6: commands, relevant log excerpts, consumer-group describes and reconciliation output.
- [ ] A short explanation (half a page) of **why** E3 produced duplicates and **what** in your code prevents them in E4.
- [ ] The producer tuning table and your chosen settings.
- [ ] Code review readiness: you can explain the transaction boundaries, the rebalance listener and the state-rebuild logic line by line.

### Questions to answer

1. In E3, which exact window between which two operations caused the duplicates?
2. Why must consumers of `charges` and `notifications` use `read_committed`? What would they see otherwise?
3. What would happen to balances if two instances owned the same partition for a moment? Which mechanism prevents that "zombie" from writing?
4. Why does the charging engine rebuild balances from `balance` rather than replaying `usage`? What topic setting makes that fast?
5. The notification service is at-least-once. Is that acceptable against BR-11? Why?

### Stretch

- Add a p99 "event-to-charge" latency measurement (event `ts` → charge `ts`) and report it at 20 and 50 events/s (BR-04).
- Implement the charging engine with Kafka Streams as a second variant and compare.

---

## Phase A4 — Operations & high availability

| | |
| --- | --- |
| **Opens after** | Module 5 |
| **Modules applied** | 5 (adding/removing brokers, partition reassignment, ISR, leader election, broker failure, rolling restart, capacity planning) |
| **Environment** | Shared Apache cluster (reassignment plans for your topics) + local cluster (failures, adding a broker) |
| **Effort** | 2–3 hours |

### Goal

Show that the platform keeps charging correctly through the operations an
administrator performs every month — and plan for Hajj.

### Requirements

**A4.1 — Hajj capacity plan.** Extend `docs/capacity-plan.md` for the peak
in [§4.3](README.md#43-production-volumes-for-sizing-and-capacity-planning-only):
broker count, partitions per topic, network in/out per broker, disk per
broker with headroom, and the number of charging instances. State every
assumption (per-partition throughput, per-broker limits, replication traffic).
Explain why partition counts must be decided **before** the season, not during.

**A4.2 — Reassignment plan (shared cluster).** For your `usage` and
`charges` topics, generate a reassignment plan that moves replicas off one
broker, review it, and verify it after the trainer executes it. Include a
throttle value and justify it.

**A4.3 — Broker failure under load (local cluster).** With the simulator and
3 charging instances running in exactly-once mode against your local cluster:

- Stop the broker that leads the most `usage` partitions. Record leader
  election, ISR shrink, any client errors and how long they lasted.
- Bring it back; record ISR expansion and whether leadership returns.
- Reconcile.

**A4.4 — Rolling restart runbook.** A step-by-step procedure in
`docs/runbook.md` to restart all brokers one at a time (e.g. for a config
change or upgrade) with **no** charging interruption beyond client retries.
Include the pre-checks and "do not continue if…" conditions. Execute it on your
local cluster with load running.

**A4.5 — Add a broker.** Add a 4th node to your local cluster and rebalance
your topics onto it. Document the steps and the before/after replica
distribution.

### Acceptance criteria

- [ ] Capacity plan with numbers, formulas and assumptions.
- [ ] Reassignment JSON (proposed and final) and `--verify` output.
- [ ] Broker-failure timeline: what you did, what the cluster did, what the clients logged, with timestamps.
- [ ] Reconciliation is **clean** after A4.3, A4.4 and A4.5.
- [ ] Runbook reviewed by another learner (name them and note one improvement they suggested).

### Questions to answer

1. During the broker failure, did any producer request fail permanently? Which producer settings decided that?
2. What is an under-replicated partition, and which metric/command shows it? When should it page someone?
3. Why can't you simply add partitions to `usage` for Hajj on the day? (Hint: A1.3.)
4. Clean vs unclean leader election: which setting, what risk, which topics (if any) could ever allow it?

### Stretch

- Simulate a whole-rack (AZ) loss with `broker.rack` on your local cluster and show replica placement protected you.

---

## End of Part A checkpoint

Before Module 6, your `README.md` should show all A1–A4 criteria ticked with
evidence, and the trainer will run your reconciliation tool against your
shared-cluster topics. Part B moves this exact platform to Confluent Kafka —
the less you hard-coded, the easier it will be.
