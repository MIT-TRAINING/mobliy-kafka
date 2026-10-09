# Capstone Trainer Guide

> **Audience:** trainer and lab/infra team. **Not for learners**: §2, §4 and §5 contain the answers.
> **Spec:** [`README.md`](README.md) · [Part A](part-a-apache-kafka.md) · [Part B](part-b-confluent-kafka.md) · [Submission template](submission-template.md)

> **Status of this guide.** It is written from the repository's documents and
> scripts ([`infra/`](../infra/README.md), the labs, the case study). **Nothing
> in it has been run against the live clusters.** Every command in §4 and every
> "to verify" in §1 must be rehearsed on `lab-l19` before the capstone opens.
> Anything the infrastructure scripts do not already do is listed as a **gap**
> with an owner, not assumed to work.

| Section | Content |
| ------- | ------- |
| [1](#1-readiness-checks-before-the-capstone-opens) | Readiness checks, gaps and fallbacks |
| [2](#2-answer-key-and-expected-findings) | Expected findings and check values per phase |
| [3](#3-schedule-and-sessions) | Schedule, slots and waves |
| [4](#4-fault-injection-kit) | Fault catalog, injection and restore, kit to build |
| [5](#5-peak-night-run-sheet) | Peak Night run sheet |
| [6](#6-marking-guide) | Marking guide |
| [7](#7-teardown-and-cost) | Teardown and cost |
| [8](#8-open-risks) | Open risks |

---

## 1. Readiness checks before the capstone opens

The capstone adds **no new infrastructure**. It does use parts of the existing
infrastructure that the module labs use lightly or not at all. Do these checks
with a learner login (`l19`), not the trainer login: several gaps only show up
for a learner.

| ID | Check | How to verify | If it fails |
| -- | ----- | ------------- | ----------- |
| **R1** | Shared Apache cluster up; every VM has `~/kafka/apache.properties`; learner quotas present | `infra/cluster/scripts/cluster-power.sh start`, `check-cluster.sh`, `distribute-client-config.sh`; `kafka-configs.sh --describe --entity-type users --entity-default` shows 1 MB/s produce | Re-run the scripts. Phase 2–4 cannot start without it |
| **R2** | Confluent Platform cluster up; Control Center reachable; learner can `confluent login --url $MDS_URL` | `infra/confluent/scripts/cp-power.sh start`, then [`infra/guides/confluent-platform-cluster-connect.md`](../infra/guides/confluent-platform-cluster-connect.md) §6 | Start order: LDAP → controllers → brokers → services |
| **R3** | **Trial licence covers the dates.** Replicator, RBAC and Control Center need the licence. The 30-day trial counts from first start | Write the expiry date into this guide. P8 and P9 must fall inside it | Buy or request a subscription key (`infra/confluent/README.md` B.1), or move the capstone earlier |
| **R4** | **Gap: learner access to Schema Registry, Connect and ksqlDB.** `cp-rbac-learners.sh` binds only Kafka resources (topics and groups by prefix, `Operator`, a cluster ACL). It does not bind Schema Registry subjects, Connect connectors or ksqlDB | As `l19`: register a subject `l19.cdr.voice-value`; create and delete a connector named `l19.*`; run a ksqlDB statement against `l19.*` topics | Extend the script with bindings scoped by prefix on the Schema Registry, Connect and ksqlDB clusters. **Confirm the exact role and resource names against the CP 8.3 RBAC documentation in the dry run.** Until fixed, T9.1 (Platform part), T9.3 and the Replicator part of T9.5 are blocked |
| **R5** | **Replicator is installed and can reach the Apache cluster.** The services node must resolve and reach `apache-kafka.lab.internal:9092`, and the Apache brokers' security group must allow it | `GET https://cp-services.lab.internal:8083/connector-plugins` lists Replicator; from `cp-services` (via SSM) open a TCP connection to `apache-kafka.lab.internal:9092` | Allow the CP services node in the Apache brokers' security group. **Fallback:** learners run **MirrorMaker 2** on their VM for T9.5 (the VM already reaches both clusters). Marks are the same; the report must say which tool was used |
| **R6** | **Capacity of the services node.** SR, Connect, ksqlDB and Control Center share 8 GB and run with 0.5, 1, 1 and 2 GB heaps. Eighteen Replicators or queries at once will not fit | Watch `free -m` and the three services during the dry run | Run Replicator in **waves of six** (§3); `tasks.max=1`; one ksqlDB query per learner at a time. Use the MM2 fallback for the rest |
| **R7** | **Gap: no learner quota on Confluent Platform.** The Apache cluster has default user quotas; `cp-rbac-learners.sh` sets none on Platform, so the caps in README §8 are only asked for, not enforced | `kafka-configs.sh --describe --entity-type users --entity-default` on `$CP` as trainer | Set a default user quota on Platform before P7: `--alter --entity-type users --entity-default --add-config producer_byte_rate=1048576,consumer_byte_rate=2097152`. It also makes fault F2 an explicit per-user override |
| **R8** | **Cloud environments ready.** `env-lNN` with Schema Registry, `sa-lNN-app`, learner `EnvironmentAdmin`. Can a learner create **more** service accounts? | `infra/confluent/README.md` A.4–A.6; as `l19`: `confluent iam service-account create` | If not, pre-create `sa-lNN-fraud` and `sa-lNN-audit` for every learner (T6.3). The LAB-SETUP §6 note says to confirm this in the dry run |
| **R9** | **Managed connectors allowed**, and a billing guard exists | As `l19`, create and delete a Datagen Source in `env-l19`. Billing notifications are on (`infra/confluent/README.md` A.3) | Pre-create the connector for learners from the trainer's side, or demonstrate and skip T9.2's create step |
| **R10** | **Learner VM tools.** Kafka 4.x CLI including `connect-mirror-maker.sh`, Confluent CLI v4, `jq`, `curl`, `openssl`, Maven, Java, Docker with the staging images. **The `ksql` CLI is not installed**, so T9.3 uses the ksqlDB REST API | `which connect-mirror-maker.sh ksql confluent jq` on `l19` | Add to the golden image, or document the REST calls in a short helper sheet |
| **R11** | **Memory on the VM.** P9 runs the staging cluster (about 3 GB), two TelcoPulse JVMs and MirrorMaker 2 on an 8 GB `t3a.large` | Run T9.5 and T9.8 on `l19` with all of them up; check `free -m` | Stagger the steps (README §8 says one thing at a time); tell learners to stop idle JVMs and the staging cluster when not in use |
| **R12** | **Fault-injection kit exists and has been dry-run** (§4, K1–K5) | All eight faults injected and restored on `l19` | Do not run P8 or Peak Night without it |
| **R13** | **Reassignment hand-off (P4 / T4.3).** You execute up to 18 plans on the shared cluster | Collect plans in a shared folder; execute **one at a time** with a throttle; run `--verify` for each | Agree the order and the throttle; announce; do not run during another learner's Phase 4 load test |
| **R14** | **Broker-failure demo on the shared Apache cluster** is scheduled for P4 (A4.7) | `infra/cluster/scripts/broker-failure-demo.sh stop broker-12`, then `start broker-12` | Announce it first: it affects every learner |
| **R15** | **Learner config files on the VMs**: `apache.properties`, `cp.properties`, `cp-ca.pem`, `confluent.env`, Cloud logins from Modules 6–8 | `ls -l ~/kafka/` on `l19` | `distribute-client-config.sh`, `distribute-confluent-config.sh` |

---

## 2. Answer key and expected findings

Use these to **mark**, not to hint. Where the spec leaves a choice, accept any
choice that is stated and consistent.

### P1 — Topic design review (T1.1)

Expect at least five of:

- **Key = MSISDN** gives per-subscriber order but a very busy subscriber makes a **hot partition**; a fraud burst shows it.
- **`audit.events` has one partition**: one total order, but also one write path and no parallelism.
- **`network.telemetry` is RF 2 with `min.insync.replicas=1`**: acceptable by design for lossy data, but it is a *named* exception to BR-1 and must be recorded as such.
- **Lab-mode compaction timings** on `subscriber.plan` (`segment.ms` 30 s, dirty ratio 0.01) are not production values.
- **`cdr.sms` and `cdr.data` retain 3 days**, which is **less than BR-2** (72 h outage + 100 % margin = **6 days**). `cdr.voice` at 7 days passes.
- **`cdr.data` has `retention.bytes` 5 GiB per partition** (6 × 5 GiB = 30 GiB). At peak the size cap can delete data **before** the time limit, silently shortening the window. Raising `retention.ms` to 6–7 days without checking the size cap does not meet BR-2.
- The partition count of 6 for voice against 3 billing threads: parallelism ceiling and rebalance cost.
- `drill.durability` is a lab-only topic and should not exist in production.

### P2 — Contracts (T2.1, T2.5, T2.6)

- **Catalog v2** must raise `cdr.sms` and `cdr.data` time retention to at least 6 days **and** reconcile `retention.bytes` with the volume (see above).
- **Durability matrix** (18 cells; Module 3 Lab 03 shows the pattern). Typical results for RF 3: with all brokers up, every combination succeeds; with one broker down, `acks=all` with `min.insync.replicas=2` still succeeds, with `min.insync.replicas=3`-style settings it would not; with two brokers down, `acks=all` with `min.insync.replicas=2` fails with `NotEnoughReplicas`, while `acks=1` and `acks=0` still "succeed" for partitions whose leader survives, which is the danger. Accept observed results; mark the **explanation in terms of the ISR**.
- **Sizing check values** (decimal units, **before** compression and RF): 20 M × 25 = 500 M CDR/day = **5,787 CDR/s** average; peak at 4× = **23,148 CDR/s**; busy hour (10 % of the day) = 13,889 CDR/s. At 400 B: **2.3 MB/s** average, **9.3 MB/s** peak, **200 GB/day**; 7 days = **1.4 TB**, × RF 3 = **4.2 TB**. Telemetry: 30,000 × 150 B = **4.5 MB/s**, 1 h = **16.2 GB**, × RF 2 = **32.4 GB**. Accept any mix across voice, SMS and data if it is stated, and any compression ratio that was **measured**.

### P3 — Client operations

- **Delivery guarantee:** TelcoPulse billing is **at-least-once** (the case study says so). A forced rebalance or a crash between rating and commit yields a duplicate; `chargeId` (`topic-partition-offset`) makes it detectable.
- **Unparseable record:** counted and skipped, **not** retried or dead-lettered. Expected recommendation: retry for transient errors, a dead-letter topic for poison records, an alert on the counter.

### P4 — Capacity and HA

- N+1 reasoning: with the production inputs the **4-broker shared cluster with 50 GB disks cannot hold 7 days of CDRs**. Marks go to the method and the stated assumptions, not one broker count.
- Plan review: reject plans that concentrate replicas in one rack (the cluster uses racks `ap-south-1a/b/c/a` for brokers 11, 12, 13, 14).
- Failure drills: graceful stop should show **no failed sends** with an idempotent producer; `kill -9` shows a short error burst and an `Recovering … logs` line on restart; controller loss shows a new leader in `kafka-metadata-quorum.sh describe --status`. **Combined-mode staging**: a broker failure also costs the quorum a voter, unlike the shared cluster (Module 5 README).

### P5 — Decision record

A defensible answer: **core charging data and telemetry → Confluent Platform** (data control, directory-based RBAC, Replicator from Apache); **partner feed → Confluent Cloud** (managed connectors, Schema Registry, Metrics API, no operations); **nothing stays on Apache** except as the migration source and the rollback. Mark the **reasoning and the reversal conditions**, not the placement.

Differences to expect in the setting map: Cloud's fixed replication factor, no `compression.type` per topic, `segment.ms` of at least 10 minutes, topic-level settings refused (the case study's `application-ccloud.yml` adapts them); Platform's `min.insync.replicas=2` as a **dynamic** cluster default; `SASL_SSL` with PLAIN against LDAP instead of SCRAM over plaintext.

### P6 — Security

- Narrowest roles: **applications** `DeveloperRead` / `DeveloperWrite` on named topics and groups; **`noc-operator`** needs metrics and group visibility without data (on Platform the course's `Operator` is the narrowest role that Control Center accepts, and note it **adds Describe on every topic**, a residual exposure the learner should record); **auditor** `DeveloperRead` on charges and audit only; **partner** `DeveloperWrite` on one topic. Over-broad choices (`ResourceOwner`, `EnvironmentAdmin`) lose marks unless justified.
- **Findings on Apache:** `SASL_PLAINTEXT` (credentials and data in clear on the wire), plaintext controllers, per-learner ACLs that cannot be delegated. All fixed on Platform by TLS and MDS, except where the learner notes that PLAIN-over-TLS passes the LDAP password to the broker.

### P7 — Monitoring

- SLO-2 in records: 30 s × 50 CDR/s = **1,500**; at peak 30 s × 200 = **6,000**.
- Tuning experiment: marks for **one change at a time**, **three runs**, and an honest "no significant effect" when that is what the data shows.
- The scaling answer should follow the evidence: lag from a slow consumer → consumers (up to the partition count) or processing; throttle time → quota; broker disk or network → brokers.

### P8 — Game day: see §4 (symptom, root cause, expected fix)

### P9 — Migration and recovery

- **Offset decision:** a new group on the target has **no committed offsets**. `auto.offset.reset=latest` loses the backlog; `earliest` replays everything. The expected answer sets positions **by timestamp** from the old group's last committed record, and accepts a **bounded duplicate window**.
- **Billing reconciliation without `chargeId`:** compare **records produced** (end offsets on the source since the test started) with **records rated** by the old and the new instance (`/api/insights/billing`); the difference is loss (negative) or duplication (positive).
- **Replicator vs MirrorMaker 2:** Replicator is a Connect source connector that runs in the Connect cluster of the **destination**, needs the enterprise licence, and (with its timestamp interceptor) supports offset translation for consumers; MM2 is open source, runs as its own Connect-based process anywhere, renames topics by default (the learner must choose the identity policy to keep names) and writes checkpoint and offset-sync topics. The shared Connect cluster is attached to **Platform**, so it can only be the destination for Apache → Platform.
- **Realistic RPO** for MM2 under the normal profile: seconds. A multi-minute gap means the learner's MM2 is starved or throttled; ask them to explain.

---

## 3. Schedule and sessions

| When | What | Trainer action |
| ---- | ---- | -------------- |
| After Module 2 | **P1 opens** | Hand out the README. Run R1 |
| After Module 3 | **P2 opens** | — |
| After Module 4 | **P3 opens** | — |
| After Module 5 | **P4 opens** | Collect reassignment plans (R13); run the **broker-failure demo** (R14); execute plans one at a time |
| After Module 6 | **P5 opens** | R2, R8, R15 done |
| After Module 7 | **P6 opens** | Partner pairs from Module 7 stay |
| After Module 8 | **P7 opens** | **Set the Platform quota (R7).** Publish **three load-test slots** of 6 learners each; one slot at a time on Platform |
| Module 9 session | **P8 in class (2.5 h)** | R12 done. Inject per learner (§4). 4 × 25 min, 30 min debrief and hand-in. Snapshot state before and after |
| Module 10 session | **P9 Parts 1–3 as homework, Peak Night in class** | R4, R5, R6, R10, R11 done. **Replicator waves** of six (§1 R6) |
| Peak Night | **75 min, all learners at once** | One trainer injects for all (§5); a second trainer or assistant answers access requests. Orals after: 5 min each |
| After the course | Marking, teardown | §6, §7 |

Remember the effort split: about **21 h** in total, mostly homework. P8 and Peak Night are the only sessions that need the trainer live.

---

## 4. Fault-injection kit

> **Status: specified, not built.** Eight faults, each with an injection, a
> restore, a symptom card and the expected diagnosis. The commands below are
> the standard Kafka and Confluent CLI forms and **must be rehearsed** (R12).
> Marked *(confirm)* where the behaviour depends on the cluster version.

### 4.1 Rules for injection

1. **Only on one learner's prefix**, `lNN.` or user `lNN`. Never a cluster-wide setting, never a broker stop (that is the P4 demo, announced).
2. **Snapshot first, diff after**, for A8.2: topic descriptions and configs, role bindings, user quotas, group states for the prefix.
3. **Always restore.** `restore.sh` returns every injected setting to the catalog v2 values (or v1 if the learner has none).
4. **Trainer credentials:** `~/kafka/cp.properties` and `~/kafka/apache.properties` on the trainer VM. For role bindings: `confluent login --url $MDS_URL` as `trainer`, and the Kafka cluster ID from `confluent cluster describe`.
5. Variables used below: `L=lNN`, `BS=$CP` (or the Apache bootstrap), `TR=~/kafka/cp.properties`, `CP_ID=<kafka-cluster id>`.

### 4.2 Catalog

| ID | Family | Used in | Symptom card (what the learner is told) | Injection | Expected diagnosis and fix | Restore |
| -- | ------ | ------- | ---------------------------------------- | --------- | -------------------------- | ------- |
| **F1** | Message **loss** (retention shorter than the outage) | Game day | *Setup:* "Stop billing now (maintenance) and keep sending 50 CDR/s for ten minutes." *Card:* "After maintenance, billing resumed with no errors, but the totals are short. Finance asks how many calls were never rated." | `kafka-configs.sh --bootstrap-server $BS --command-config $TR --alter --entity-type topics --entity-name $L.cdr.voice --add-config retention.ms=60000,segment.ms=10000` (deletion happens at the next retention check; check the broker's `log.retention.check.interval.ms`) | Log start offset is **past** the group's committed offset. Records deleted before they were consumed. Fix: restore config, count the gap from offsets, replay from the source or the Apache copy if one exists, set `retention.ms` per BR-2. **Prevent:** alert on `committed offset − log start offset` approaching zero | `--alter … --add-config retention.ms=<v2>` and `--delete-config segment.ms` |
| **F2** | **Throttling** and quotas | Game day | "Since 14:05 ingestion is minutes late. Producer logs show timeouts. Brokers look idle." | `kafka-configs.sh … --alter --entity-type users --entity-name $L --add-config producer_byte_rate=2048` | Throttle time on the producer, a per-user quota at the broker (`--describe --entity-type users`). The learner **cannot** change a quota: correct action is an **escalation package** (T8.4) with the evidence. **Prevent:** alert on produce throttle time; size quotas from the capacity plan | `--delete-config producer_byte_rate` |
| **F3** | **Replication** and write failures | Game day | "No new charges since 14:10. Billing consumers are healthy. The log shows an error on every write." | `kafka-configs.sh … --alter --entity-type topics --entity-name $L.billing.charges --add-config min.insync.replicas=4` *(confirm: self-managed brokers accept a value above the replication factor)* | `NotEnoughReplicasException` on `acks=all`; topic config shows `min.insync.replicas` above RF; producers with `acks=1` would still "work" and hide it. Fix: topic owner restores 2. **Prevent:** a topic-config drift check (`apply-topics.sh --diff`) on a schedule | `--add-config min.insync.replicas=2` |
| **F4** | **Access** drift | Game day | "Billing stopped consuming at 14:15. No code changed. You can still list topics." | `confluent iam rbac role-binding delete --principal User:$L --role ResourceOwner --resource Group:$L. --prefix --kafka-cluster $CP_ID` | `GroupAuthorizationException` while `Describe` still works; `role-binding list` shows the group binding missing. The learner **cannot** restore it (it needs the binding they lost), so: escalation package. **Prevent:** run `access-tests.sh` on a schedule; alert on any role-binding change | `role-binding create` with the same arguments |
| **F5** | **Consumer group** instability | Peak Night | "Billing lag rises and falls in a saw-tooth and throughput has halved. No errors." | A foreign member joins and leaves the group repeatedly, **without** committing: `while true; do timeout 20 kafka-console-consumer.sh --bootstrap-server $BS --command-config $TR --topic $L.cdr.voice --group $L.billing --consumer-property enable.auto.commit=false >/dev/null 2>&1; sleep 3; done` | Group state cycles through rebalances; `--describe --members --verbose` shows an **unknown member** (different client id and host). Fix: identify and stop it (the trainer's job to confirm), then show how **static membership** and a cooperative assignor limit the damage. **Prevent:** group `Read` only for the application's identity | Kill the loop |
| **F6** | **Connectivity** and TLS | Game day (rotate: the trainer leaves one of F1–F4 or F6 out per learner) | "A new batch job on your VM cannot connect to the cluster. Here is its client file." | The trainer **places** `broken-client-N.properties` (K5) in `~/capstone/gameday/`. Three variants: wrong truststore path; bootstrap set to a **broker IP** instead of `cp-kafka.lab.internal` (fails hostname verification); bootstrap set to the broker-to-broker port **9091** | `SSLHandshakeException` / `PKIX` / hostname mismatch / timeout, each diagnosed with `openssl s_client` and the listener table. No cluster change; fix is client-side. **Prevent:** client config linting in the standard | Remove the file |
| **F7** | **Hot partition** | Peak Night | "Billing lag is high but only on one partition; the other five are at zero. Adding consumers did not help." | One subscriber floods: `awk 'BEGIN{for(i=1;i<=20000;i++) printf "966500000001|{\"cdrId\":\"hot-%d\",\"type\":\"VOICE\",\"msisdn\":\"966500000001\",\"counterparty\":\"966500000099\",\"destinationCountry\":\"AE\",\"durationSec\":60,\"bytes\":0,\"eventTime\":\"2026-01-01T00:00:00Z\",\"cellId\":\"C1\"}\n", i}' \| kafka-console-producer.sh --bootstrap-server $BS --producer.config $TR --topic $L.cdr.voice --property parse.key=true --property key.separator='\|'` *(confirm the app's JSON codec accepts this shape)* | Per-partition lag shows one partition; one consumer thread is saturated; more consumers cannot help because one key = one partition. Findings, not code: key design, salting, a fraud rule for floods. **Prevent:** per-partition lag alert and a producer-side rate limit | Let it drain, or reset the group's offsets after the test |
| **F8** | **Accidental deletion** of state | Peak Night | "The plan table is empty after a restart and many charges show 'plan unknown'." | `kafka-topics.sh --bootstrap-server $BS --command-config $TR --delete --topic $L.subscriber.plan` | Topic missing; `ratedWithoutPlan` rises. Recovery depends on **Phase 9**: restore the compacted topic from **site 2** (the MM2 copy) or re-seed with the app's seed call. A learner without a recovery copy shows why BR-8 matters. **Prevent:** deletion protection by access rule, backup copy | The learner restores; if not, trainer re-seeds |

Game day: **four of F1, F2, F3, F4, F6** per learner, in a **random order**,
leaving a different one out for neighbours. Peak Night: **two of F5, F7, F8**.

### 4.3 Kit to build

| ID | Item | Notes |
| -- | ---- | ----- |
| **K1** | `inject.sh <F-id> <lNN>` | One function per fault above. Refuses any target outside `lNN` |
| **K2** | `restore.sh <lNN>` | Returns the prefix to catalog v2 values; idempotent |
| **K3** | `capture-state.sh <lNN> <before\|after>` | Dumps topic descriptions and configs, role bindings, user quotas, group states. `diff` of before and after is the evidence for A8.2 |
| **K4** | Assignment sheet | Which learner gets which faults, in which order, for Game day and Peak Night |
| **K5** | `broken-client-1..3.properties` and a script that places them | The three F6 variants, copied to `~/capstone/gameday/` by SSH with the admin key |
| **K6** | Symptom cards | One page each, printed or posted; no root cause on them |
| **K7** | Extension of `cp-rbac-learners.sh` (R4) | Schema Registry, Connect and ksqlDB bindings by prefix; set up the Platform quota (R7) |

---

## 5. Peak Night run sheet

| Time | Trainer action |
| ---- | -------------- |
| T−30 | Platform up (R2). Quota in place (R7). `capture-state.sh … before` for all learners. Announce the exercise |
| T+0 | "Go." Learners run health checks and start the normal profile |
| T+10 | "Peak begins." (Learners start 200 CDR/s) |
| T+12 | Inject **fault 1** for every learner (K1), per the assignment sheet |
| T+32 | Inject **fault 2** |
| T+50 | Read the **decision card** aloud to everyone: *"Two of the three Confluent Platform brokers are reported unreachable. Recovery time unknown. Decide: fail over to site 2, or hold."* Do **not** actually stop brokers |
| T+65 | Stop the clock for the decision. Learners write the closing status |
| T+75 | End. Run `restore.sh`, `capture-state.sh … after`. Collect `peak-night/` folders |
| After | Orals, 5 minutes each: three questions from the learner's own evidence |

**Observation checklist per learner** (feeds A9.9–A9.12): time to detect fault 1 and fault 2; whether the learner's own alert or check fired; whether they followed their runbook; any collateral change (diff); whether the decision cites **their own** RPO and RTO; whether the status updates are on the ten-minute marks and readable by a non-engineer.

---

## 6. Marking guide

Every phase: **50 % it works**, **25 % evidence and reproducibility**,
**25 % reasoning** ([README §9.2](README.md#92-how-each-phase-is-marked)).

| Phase (marks) | Full marks look like | Typical deductions |
| ------------- | -------------------- | ------------------ |
| **P1** (8) | Health check proves its own failure modes (A1.2, A1.3); review findings are ranked and tied to BR IDs; inventory cells come from commands | Inventory copied from READMEs; health check only prints "OK" |
| **P2** (10) | `--diff` catches a hand-made change; v2 fixes the BR-2 trap **and** reconciles the size cap; matrix cells observed, not recalled; sizing within ±30 % | Retention raised without checking `retention.bytes`; matrix filled from memory; sizing without a measured compression ratio |
| **P3** (8) | Standard has a reason and a symptom for every value; offset runbook shows refuse → stop → dry run → execute; duplicate **shown** with its detection key | "At-least-once" asserted without a duplicate; no dry run |
| **P4** (12) | N+1 capacity with formulas; plan **rejected or fixed** when it concentrates racks; throttle appears and disappears; drills measured; rolling restart has a gate that waited once | Plan handed over without rollback; drills without numbers; script restarts without a gate |
| **P5** (6) | Decision record has reversal conditions; ≥ 6 real differences with their revealing command | Decision without criteria; table of generic differences |
| **P6** (12) | 24+ checks, ≥ 12 DENY, zero mismatches; bindings equal the matrix; rotation with no gap; audit trail present | Only ALLOW tests; leftover binding for the partner; broad role without justification; a secret in the repo (−10) |
| **P7** (10) | Alert fires **and** resolves; SLO report prints PASS/FAIL with values; tuning has three runs and a real conclusion; peak verdict with extrapolation | Dashboard-only SLOs; one run per condition; extrapolation without formulas |
| **P8** (14) | Correct root cause each time; no collateral change; reports have all eight parts; checklist has ≥ 3 own failures; one alert shown firing | Fix by trial and error (diff shows collateral); checklist with no failing items; no escalation package for F2 or F4 |
| **P9** (20) | **Migration:** ≤ 5 min pause, zero loss, duplicates counted, rollback timed, verifier detects an injected loss. **Recovery:** RPO chart, timed failover, failback plan. Ecosystem tasks with compatible and incompatible evolution, connector lifecycle, ksql torn down. **Peak Night** operated well. Clean-up output | Target missed without explanation; failover without a stated position method; connector left running (also −5 for cost) |

**Suggested split of P9's 20 marks:** ecosystem T9.1–T9.3 = 4; migration T9.4–T9.6 = 7; recovery T9.7–T9.8 = 4; memo and clean-up T9.9, T9.11 = 1; Peak Night and oral = 4.

**Calibration.** Before marking, all markers score the same three submissions
(one weak, one average, one strong) and agree on the bands. Re-run the learner's
scripts from a clean shell for P1, P2, P6 and P9; do not rely on the report.

---

## 7. Teardown and cost

| Item | Action |
| ---- | ------ |
| Learner resources on Apache and Platform | Each learner's `cleanup.sh` (T9.11) removes theirs. Check with `kafka-topics.sh --list` as trainer for any `lNN.*` left; `infra/cluster/scripts/reset-learner.sh` for the Apache cluster |
| Replicator and MM2 | Delete the Replicator connector (`DELETE /connectors/<name>` on the Connect REST API) and confirm no `lNN` connectors remain. MM2 runs on VMs and ends with the VMs |
| ksqlDB | `SHOW QUERIES` empty for every learner |
| Schema Registry subjects | Delete `lNN.*` subjects |
| **Confluent Cloud** | `confluent connect cluster list` empty in every `env-lNN`. Managed connectors bill per task-hour; check **before** leaving each session ([`infra/confluent/README.md`](../infra/confluent/README.md) A.3, Part D) |
| Quotas and bindings added for the capstone | Remove the F2 override, role bindings added in K7, and the Platform default quota if it was added only for this |
| Submissions | Export what the client wants to keep (LAB-SETUP §13). Scan each submission for secrets before copying it anywhere |
| Cluster shutdown | `cluster-power.sh stop` and `cp-power.sh stop` (the trial clock keeps running while the Platform hosts are stopped) |

---

## 8. Open risks

| Risk | Effect | Mitigation |
| ---- | ------ | ---------- |
| Trial licence expires inside P8–P9 (R3) | Replicator, RBAC and Control Center stop | Check the date now; plan P9 first if the window is tight |
| RBAC gap for Schema Registry, Connect and ksqlDB (R4) | Half of Phase 9 is blocked for learners | Dry-run as `l19` before P5 opens, so there is time to fix |
| Services node (8 GB) under 18 learners (R6) | Replicator, ksqlDB or Control Center restart; everyone is affected | Waves of six, `tasks.max=1`, MM2 fallback, watch `free -m` |
| No quota on Platform (R7) | One learner's load test slows everyone | Set the default quota before P7; stagger slots |
| Peak Night injects for 18 learners at once | A script error affects everyone | Dry-run K1 on `l19`; start with three learners; have `restore.sh` ready |
| VM memory during P9 (R11) | The VM swaps; measurements are polluted | Stagger steps; document the order in the learner README |
| The spec asks for scripts that do not exist yet (K1–K7) | P8, Peak Night and part of P9 cannot run | Build K1–K7 and run them on `l19` before Module 9 |
| Learners' evidence copied from each other | Marks do not reflect work | Prefix-specific names and timestamps appear in the evidence; the oral uses the learner's own numbers |
