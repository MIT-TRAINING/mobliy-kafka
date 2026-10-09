# Capstone Part A — Operating TelcoPulse on Apache Kafka

> **Back to:** [Capstone overview](README.md) · **Next:** [Part B — Confluent Kafka](part-b-confluent-kafka.md)
> **Phases:** P1 (after Module 2) · P2 (after Module 3) · P3 (after Module 4) · P4 (after Module 5)

In Part A you take over the platform as it runs today, on **Apache Kafka**.
You review its design, turn the topic contracts into code, prove what
survives a failure, set the standards for the applications, and write the
runbooks for scaling and for broker loss. Requirement IDs (`BR-…`, `NFR-…`,
`SLO-…`) are defined in [§4 of the overview](README.md#4-requirements).

Every phase has the same shape:

| Block | Content |
| ----- | ------- |
| **Goal** | The outcome in one paragraph, and why the business needs it |
| **Environment** | Where you work and what you may do there |
| **Tasks** | What to achieve, with the requirement IDs it serves. *How* is yours to decide |
| **Acceptance** | Checks that must pass. Each has a verifiable command or observable result |
| **Evidence** | What to keep in `evidence/phase-N/` |
| **Toolbox** | The labs and guide sections to reread. Not step-by-step instructions |
| **Clean-up** | What to remove before the phase is done |

Work from `~/capstone` on your VM. `lNN` is your learner ID and `$ME` its prefix
(`lNN`), as in the labs.

---

## Phase 1 — Design review and staging cluster

**Opens after Module 2 · about 1.5 h · serves BR-1, BR-2, NFR-1**

### Goal

Before you change anything, understand what you are taking over. Review the
topic design the developers shipped, record how each of your four clusters is
really configured (not how a README says it is), and build the two things you
will use in every later phase: a staging cluster you may break, and a health
check you can trust.

### Environment

Own VM. The **staging cluster** is the Module 2 three-node KRaft cluster
([`labs/module-02/docker-compose.yml`](../labs/module-02/docker-compose.yml)).
Read-only on the shared clusters in this phase.

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T1.1** | **Topic design review.** For each of the eight TelcoPulse topics, write down: data class, key, partitions, RF, `min.insync.replicas`, retention, cleanup policy, producers, consumer groups. Then challenge it. Is each partition count justified by consumer parallelism and by the production inputs ([overview §4.4](README.md#44-workload-profiles))? What breaks if one subscriber is very busy? Which settings are lab-only? List **at least five** findings, each marked *risk*, *question for developers* or *acceptable*, and ranked | BR-1, BR-2 |
| **T1.2** | **Naming and ownership convention** (one page). A topic name pattern, an owner per data class, a data classification, a rule for who may create topics, and how a new application requests one. State why topic auto-creation is off on both shared clusters | BR-4, BR-6 |
| **T1.3** | **KRaft decision note** (one page, for a manager). What does each of your clusters use for metadata? What changes for an operator compared with a ZooKeeper-based cluster (processes, ports, failure of the metadata layer, upgrades)? Confirm the live facts with commands on the shared Apache and Confluent clusters | BR-9 |
| **T1.4** | **Cluster inventory.** One table covering the staging cluster, the shared Apache cluster, the Confluent Platform cluster and your Confluent Cloud cluster: version, node roles and count, listener and authentication mechanism, authorisation model, default replication settings, and **what you may and may not do**. Every cell comes from a command, not from documentation. Note the command in the cell | NFR-5 |
| **T1.5** | **Staging cluster up.** Start it, run TelcoPulse with the `local` profile, and confirm that the eight topics exist exactly as the catalog declares | BR-1 |
| **T1.6** | **Health check.** One script, `health-check.sh <local\|shared\|cp>`, that prints a PASS / WARN / FAIL line per check and exits non-zero on any FAIL. It must check at least: reachable and authenticated; expected broker count; controller quorum has a leader and the expected voters; no offline (unavailable) partitions; no under-replicated or under-min-ISR partitions among **your** topics; the eight required topics exist; every consumer group of yours is in a sane state | NFR-1, BR-9 |
| **T1.7** | **Environment kit.** `env.sh` that sets `BS`, `CFG` and `ME` per target without containing a secret (it points to the config file). A short `docs/cli-cheatsheet.md` with the dozen commands you expect to use most, per target | NFR-1, NFR-4 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A1.1** | `health-check.sh` prints all-PASS on the staging, shared Apache and Confluent Platform clusters while TelcoPulse runs |
| **A1.2** | With **one staging broker stopped**, the script reports a WARN or FAIL that **names the cause** (a missing broker or under-replicated partitions), and exits non-zero on FAIL. With the broker back, it returns to PASS within two minutes |
| **A1.3** | With a deliberately wrong password in a copy of the config file, the script reports an authentication FAIL and does not hang longer than 30 seconds |
| **A1.4** | The inventory in T1.4 has no empty cell and no cell without a command or output reference |
| **A1.5** | `docs/p1-topic-review.md` has at least five ranked findings; at least two are *risk* items with a proposed change |

### Evidence

`evidence/phase-1/`: the three documents, `health-check.sh` output for each target (PASS case and the two failure cases), the inventory with its commands, and the output of `kafka-topics.sh --describe` for the eight topics on the staging cluster.

### Toolbox

[Module 1 labs](../labs/module-01/README.md), [Module 2 labs](../labs/module-02/README.md), the Module 3 guide on topic design, and the case study's [topic design](../casestudies/telecom-usage-platform/README.md#4-topic-design-module-3-section-41) and
[design decisions](../casestudies/telecom-usage-platform/README.md#8-design-decisions-worth-discussing).

### Clean-up

Keep the staging cluster running for Phase 2; stop the app.

---

## Phase 2 — Topic contracts and durability

**Opens after Module 3 · about 2 h · serves BR-1, BR-2, BR-4, SLO-1**

### Goal

Turn the topic design into a **contract the cluster enforces**, prove each
promise with an experiment, and size the storage the contracts imply. The
business wants to know three things: what happens to an acknowledged record
when a broker dies, how long data survives when billing is down, and how much
disk that costs.

### Environment

The **shared Apache cluster** with your `lNN.` prefix: you may create topics,
alter their configuration and read their offsets and sizes. You may **not**
stop a broker there. Destructive tests run on the **staging cluster**.

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T2.1** | **Catalog v2.** Check version 1 of the topic design ([overview §4.4](README.md#44-workload-profiles)) against BR-1, BR-2 and BR-4. Where it falls short, change it. For **every** non-default value in v2, write the reason and the requirement it serves. Treat settings that exist only for classroom speed as lab-mode and say what the production value should be | BR-1, BR-2, BR-4 |
| **T2.2** | **Topics as code.** `config/topics.yaml` (or CSV) holds the v2 catalog. `scripts/apply-topics.sh` makes the cluster match it: creates missing topics, corrects drift on listed settings, **never deletes**, and has a `--diff` mode that only reports. Decide who owns the truth, your file or the application's own startup logic (TelcoPulse also creates and corrects its topics), and document how you stop them fighting | NFR-1, BR-4 |
| **T2.3** | **Retention proof.** On the shared cluster, prove that time-based deletion works on a topic you created for the purpose: log start offset moves forward, and you can explain **how long it took and why**, using the broker's retention-check interval, the segment settings and the rule that only closed segments are deleted. Restore or delete the topic afterwards | BR-2 |
| **T2.4** | **Compaction proof.** On a compacted topic, show the record count falling while the latest value of each key survives, and a tombstone removing a key after its retention. Explain what the compaction settings trade off (disk against CPU and I/O) | BR-4 |
| **T2.5** | **Durability matrix** on the **staging** cluster, for a topic with RF 3. Test `acks` ∈ {0, 1, all} × `min.insync.replicas` ∈ {1, 2} in three cluster states: all brokers up, one broker stopped, two brokers stopped. For each cell record: do sends succeed, which error appears, and how many *acknowledged* records are missing afterwards. Finish with a table that tells a developer which combination to use for billing and which for telemetry, and what each costs | BR-1, SLO-1 |
| **T2.6** | **Storage sizing.** From the production inputs ([overview §4.4](README.md#44-workload-profiles)), calculate for each topic the disk needed across the cluster: rate × record size × compression ratio × retention × RF, plus headroom. State your compression assumption and measure it: produce a known number of records with the app's codec and read the size from the brokers. Compare your estimate with the measured size of your own topics on the shared cluster | BR-5 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A2.1** | `apply-topics.sh --diff` on a clean state prints "no drift". After you change one setting by hand with `kafka-configs.sh`, it **reports exactly that setting**. Running without `--diff` fixes it. Running twice changes nothing |
| **A2.2** | `kafka-topics.sh --describe` on the shared cluster shows the v2 values for all eight topics. No topic violates BR-2 (retention ≥ 2 × 72 h) or BR-4 (charges ≥ 30 d, audit ≥ 90 d) |
| **A2.3** | The retention proof shows two `kafka-get-offsets.sh` readings (earliest) taken before and after deletion, with timestamps, and an explanation that matches the measured delay |
| **A2.4** | The durability matrix has all 18 cells filled with observed results. The `acks=1`, one-broker-down and `min.insync.replicas=2` cells and the `acks=all` cells **with two brokers down** are explained in terms of ISR |
| **A2.5** | The sizing worksheet's estimate for your measured topics is within **±30 %** of the observed size, or the difference is explained |
| **A2.6** | `docs/p2-contract-guide.md` (one page for developers) states which `acks` and idempotence setting each topic class requires and what error the application will see if the contract is not met |

### Evidence

`evidence/phase-2/`: `topics.yaml`, the `--diff` runs, the retention and compaction readings, the matrix (with raw command output for at least the six cells that matter most), the sizing worksheet and the developer guide.

### Toolbox

[Module 3 labs](../labs/module-03/README.md) (retention, segments, `acks` and `min.insync.replicas`), the Module 3 guide, the case study's
[`TopicCatalog`](../casestudies/telecom-usage-platform/src/main/java/com/training/kafka/telco/topics/TopicCatalog.java)
and `OpsService` (each method names its CLI equivalent).

### Clean-up

Delete experiment topics on the shared cluster. Bring the staging cluster back to three healthy brokers.

---

## Phase 3 — Client operations

**Opens after Module 4 · about 1.5 h · serves BR-1, BR-3, BR-9**

### Goal

Applications do not run themselves. As the platform team you set the
standard for how clients are configured, you operate their consumer groups
when something goes wrong, and you must be able to say honestly what delivery
guarantee the billing path gives. You do this without editing application
code.

### Environment

Shared Apache cluster, `shared` profile of TelcoPulse, your `lNN.` prefix.
Offset operations on **your** groups only.

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T3.1** | **Client configuration standard.** For three client classes, **billing** (must not lose or reorder), **telemetry** (cheap, loss-tolerant) and **fraud** (low latency), write the required producer and consumer settings: `acks`, idempotence, retries and `delivery.timeout.ms`, `linger.ms`, `batch.size`, compression, `max.in.flight`, `enable.auto.commit`, `auto.offset.reset`, session and poll timeouts, assignor. Give each value a reason and the symptom you would see if a team set it wrongly. Check that TelcoPulse follows the standard, and list every deviation | BR-1, BR-3 |
| **T3.2** | **Consumer-group runbook.** Operate the `billing` group on the shared cluster: read state, members, per-partition lag and which member owns what; add a second instance and show the rebalance; stop one cleanly and kill one hard and compare what the group does and how long it takes. Then write the **offset operations** you will allow in production: reset to a timestamp, shift by N, skip one poisoned offset, full replay. Each needs a **dry run first**, a stated precondition (group stopped) and a rollback note | BR-3, BR-9 |
| **T3.3** | **Delivery-semantics audit.** Which guarantee does the billing path give today: at-most-once, at-least-once or exactly-once? Prove it. Force a duplicate (kill the app between processing and commit, or force a rebalance) and show **how a downstream system would detect it** (the case study derives `chargeId` from topic, partition and offset). State what you would require from the developers to reach effectively-once billing, as a change request, not as code | BR-1 |
| **T3.4** | **Error-handling findings.** Send a record the application cannot parse. Record what TelcoPulse does with it (the case study counts and skips). Is that acceptable for billing? Propose the production pattern (retry, dead-letter topic, alert) and the topic design it needs | BR-1, BR-9 |
| **T3.5** | **Throughput check from the operator's side.** With the Kafka perf tools (not the app), measure produce throughput and latency for two producer settings of your choice on a scratch topic, inside your quota, and recommend one for each client class | SLO-3 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A3.1** | `docs/p3-client-standard.md` covers all three classes and **every** listed setting, with a reason. The deviations list is present (empty is allowed only with proof that you checked) |
| **A3.2** | The runbook shows a rebalance triggered by adding a member and the group's state through it, with timestamps for the clean leave and for the hard kill |
| **A3.3** | An offset reset is **refused** while the group is active, then succeeds after you stop it, and you show the **dry-run output before** the execute |
| **A3.4** | A duplicated charge is shown with two records that share the detection key, and the audit states the delivery guarantee in one sentence |
| **A3.5** | The unparseable record test names the exact log line or counter that shows it was skipped |

### Evidence

`evidence/phase-3/`: the standard, the runbook with command output, the audit with the duplicate pair, the error-handling findings and the perf measurements.

### Toolbox

[Module 4 labs](../labs/module-04/README.md) (producer tuning, consumer groups, delivery semantics), the Module 4 guide, the case study's `BillingConsumer` and `ListenerControl` (stop or slow a consumer to build lag).

### Clean-up

Stop all your TelcoPulse instances. Delete scratch topics. Leave `lNN.billing` in a clean, stopped state.

---

## Phase 4 — Operations and high availability

**Opens after Module 5 · about 2.5 h · serves BR-1, BR-5, SLO-1, SLO-4, SLO-5**

### Goal

Plan capacity for the peak, then prove the cluster stays up and loses nothing
when you move partitions, lose a broker, or restart every node. This is where
a platform team earns trust: with numbers and tested runbooks, not
reassurance.

### Environment

- **Shared Apache cluster:** read-only at cluster level. You inspect, **generate** and **verify** reassignment plans. The **trainer executes** them, because executing needs cluster `Alter`.
- **Staging cluster:** full control. You execute every operation and break brokers on purpose.
- The trainer also stops a real broker on the shared cluster once, as a class demo. You watch it from your own TelcoPulse run.

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T4.1** | **Capacity plan.** From the production and telemetry inputs ([overview §4.4](README.md#44-workload-profiles)), compute for the peak: ingress and egress MB/s per topic class, disk per broker over the retention windows including replication, partitions per broker, and the number of brokers needed so that **losing one broker still leaves 70 % disk and the SLOs intact (N+1)**. Show formulas and every assumption. Compare with the 4-broker shared cluster: where would it run out first, and when? | BR-5 |
| **T4.2** | **Where do my replicas live?** Map the replicas and leaders of all your topics to the brokers and racks (availability zones) of the shared cluster. Does any partition lose all replicas if one zone fails? Does rack awareness work as intended? | BR-1 |
| **T4.3** | **Reassignment plan, reviewed.** Generate a plan that **drains broker 11** of your replicas. Review it against the cluster's racks, and reject or fix it if it concentrates replicas in one rack or leaves RF unchanged but placement worse. Produce three files: the plan, the **rollback plan** (current state), and a **verify** command. Hand all three to the trainer and watch the execution. Record how long it ran, what throttle was in force, and the clean-up the move leaves behind | BR-5 |
| **T4.4** | **Throttled reassignment on staging, under load.** While a producer runs at `acks=all`, raise the RF of one topic and spread its leaders, with a replication throttle. Show the throttle configs appear and, after `--verify`, disappear. Show a move that stalls because the throttle is below the write rate, and fix it | BR-5, SLO-1 |
| **T4.5** | **Failure drills on staging**, under live `acks=all` traffic with an idempotent producer: (a) graceful stop of a broker, (b) `kill -9` of a broker, (c) loss of the **active controller**. For each: time to leader re-election, number of failed sends, under-replicated duration, recovery after restart, and anything unexpected. Check the result against SLO-1 and SLO-5 | BR-1, SLO-5 |
| **T4.6** | **Rolling-restart runbook and script.** Restart all three staging brokers one at a time with **no failed sends**. The script has a **health gate** between nodes (it does not continue until under-replicated partitions are zero), a stop timeout long enough for a clean shutdown, and a preferred-leader election at the end. The runbook states what to do when the gate does not clear | SLO-4, BR-9 |
| **T4.7** | **Upgrade and change policy** (one page). What do you check before upgrading brokers (feature levels, client compatibility, KRaft controller version)? In what order do you upgrade? When do you refuse a change request? What is the rollback? | BR-9 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A4.1** | The capacity plan states the broker count and shows that with **one broker lost** disk stays at or below 70 % and per-broker throughput stays inside a stated limit |
| **A4.2** | `docs/p4-replica-map.md` shows leaders and replicas per rack and names any partition at risk from a zone loss |
| **A4.3** | `kafka-reassign-partitions.sh --verify --preserve-throttles` on the shared cluster shows the trainer's execution **completed**, and after the move none of your partitions has a replica on broker 11 |
| **A4.4** | The staging reassignment evidence shows throttle configs present during the move and **absent** after `--verify`; a stalled move is shown and corrected |
| **A4.5** | The three failure drills have a measurement table. SLO-5 is met or the miss is explained. The controller-loss drill shows a new leader in the quorum |
| **A4.6** | The rolling-restart script, run from a clean shell, completes with **zero failed sends** (producer log or perf-test summary attached) and with a health gate that visibly waited at least once |
| **A4.7** | Your notes from the trainer's shared-cluster failure demo show what your TelcoPulse dashboard displayed and how long the cluster took to recover |

### Evidence

`evidence/phase-4/`: capacity worksheet, replica map, the three reassignment files and their verify output, the drill measurements, `rolling-restart.sh` with its run log, the upgrade note.

### Toolbox

[Module 5 labs](../labs/module-05/README.md) (reassignment, throttles, failure and rolling restart), the Module 5 guide (§5 reassignment, §6 rolling restarts, §7 capacity planning), [`infra/cluster/PLAN.md`](../infra/cluster/PLAN.md) for the shared cluster's racks and sizes.

### Clean-up

Remove throttles, delete scratch topics, bring the staging cluster back to three healthy brokers. **Keep** your plan, rollback and verify files until the trainer confirms all learners' moves are done.

---

## End of Part A: the gate to Part B

Before Phase 5 you should hold, in `~/capstone`:

- a reviewed topic catalog (v2) applied to the shared Apache cluster by script (P2),
- a health check that works on three targets (P1),
- tested durability contracts, a client standard and an offset runbook (P2, P3),
- a capacity plan, a verified reassignment, and a rolling-restart runbook that has been run (P4).

Part B reuses all of it: the health check and the topic script grow a Confluent
target, the durability and client standards become the migration's acceptance
criteria, and the runbooks are what you follow during the game day.

Next: [Part B — Confluent Kafka](part-b-confluent-kafka.md).
