# Capstone Part B — Moving to Confluent and Operating It

> **Back to:** [Capstone overview](README.md) · [Part A](part-a-apache-kafka.md) · **Next:** [Submission template](submission-template.md)
> **Phases:** P5 (after Module 6) · P6 (Module 7) · P7 (Module 8) · P8 (Module 9) · P9 (Module 10)

In Part B management has chosen Confluent. You decide what goes where, secure
it, make it observable, learn to fix it when it breaks, and then **move the
production platform from Apache Kafka to Confluent** with a rehearsed
cutover, a rollback and a second site. The phase blocks have the same shape
as in [Part A](part-a-apache-kafka.md). Requirement IDs are defined in
[§4 of the overview](README.md#4-requirements).

Part B reuses what you built: `health-check.sh` and `apply-topics.sh` gain
Confluent targets, the Part A client standard and durability contracts become
the migration's acceptance criteria, and your runbooks are what you follow
when the trainer breaks something.

| Target | TelcoPulse profile | Config you use | Your rights |
| ------ | ------------------ | -------------- | ----------- |
| **Confluent Platform** (self-managed, AWS) | `cp` | `~/kafka/cp.properties`, `$CP`, `$MDS_URL`, `$C3_URL` | `ResourceOwner` on `Topic:lNN.` and `Group:lNN.`, `Operator` on the cluster, cluster `Describe`. Control Center, Schema Registry, Connect and ksqlDB as the trainer enables them (see [trainer guide §1](trainer-guide.md#1-readiness-checks-before-the-capstone-opens)) |
| **Confluent Cloud** (`env-lNN`, `lNN-basic`) | `ccloud` | `~/kafka/ccloud.properties`, `~/kafka/sa-app.properties`, Confluent CLI | `EnvironmentAdmin` in your environment |

---

## Phase 5 — Move to Confluent: decide, map, smoke-test

**Opens after Module 6 · about 1 h · serves BR-6, BR-7, BR-9**

### Goal

Decide **where each workload belongs**, find every setting that changes
between Apache, Confluent Platform and Confluent Cloud, and prove the
platform runs on both Confluent targets. The output is the written basis for
the migration in Phase 9.

### Environment

Confluent Platform and Confluent Cloud, your prefix, your own topics.

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T5.1** | **Deployment decision record.** For three workloads, decide *Confluent Platform*, *Confluent Cloud* or *stay on Apache Kafka*, and defend it: (a) the **core charging data** (`cdr.*`, `billing.charges`, `subscriber.plan`, `audit.events`), (b) **network telemetry** from 30,000 cells, (c) a **new partner-roaming usage feed** from external partners. Weigh data control, operations effort, identity integration (the company uses a directory), the features each option offers, cost behaviour at volume, guardrails and licensing. State what would make you change the decision | BR-6, BR-7 |
| **T5.2** | **Evidence-based comparison.** Ten capabilities (for example authorisation model, UI, rebalancing, metrics, connectors, schema governance, stream processing, who patches, who can change broker settings, licence). For each, one line each for Apache, Platform and Cloud, and **how you verified it** with a command or a screenshot | BR-9 |
| **T5.3** | **Setting map.** Apply catalog v2 to Confluent Platform with `apply-topics.sh` (add a `cp` target) and to Confluent Cloud (a `ccloud` target). Every difference you hit (a setting refused, a different default, a different name, a fixed replication factor) goes into one table: *setting · Apache · Platform · Cloud · what you did and why*. Also compare the **effective** configuration: a topic described on each target | BR-1, NFR-1 |
| **T5.4** | **Control Center as an operator.** Answer six operator questions in Control Center and show the same fact on the command line: which broker is the controller; is anything under-replicated; which of my topics is largest; what is `billing`'s lag per partition; what are this topic's effective settings; what is the cluster's throughput now. Note what your `Operator` role lets you see and what it hides | BR-9 |
| **T5.5** | **Smoke runs.** Run TelcoPulse with `cp` and with `ccloud` at the normal profile for five minutes each. Extend `health-check.sh` so `cp` is a full target and `ccloud` is a reduced one (what can and can't an operator read on a managed cluster?). Show the dashboard and the health output | NFR-1 |
| **T5.6** | **Client cutover inventory.** List **everything a client must change** to move from the Apache cluster to Confluent Platform: bootstrap, protocol, SASL mechanism, credentials source, truststore, group and topic naming, and settings your Part A standard requires. This list becomes the client checklist in Phase 9 | BR-7 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A5.1** | The decision record covers all three workloads with criteria, a choice and a reversal condition for each |
| **A5.2** | `apply-topics.sh --diff cp` and `--diff ccloud` report no drift after apply, or list the **accepted** differences with a reason |
| **A5.3** | The setting map has at least six real differences found by you, each with the command or error that revealed it |
| **A5.4** | `health-check.sh cp` is all-PASS while TelcoPulse runs on `cp`; `health-check.sh ccloud` runs and explains what it cannot check |
| **A5.5** | Each Control Center answer has a screenshot **and** a CLI command that gives the same fact |

### Evidence

`evidence/phase-5/`: decision record, comparison table, setting map, the two `--diff` runs, health output for `cp` and `ccloud`, Control Center screenshots, client cutover inventory.

### Toolbox

[Module 6 labs](../labs/module-06/README.md), the Module 6 guide, the case study guides for
[Confluent Platform](../casestudies/telecom-usage-platform/CONF-PLATFORM-CLUSTER.md) and
[Confluent Cloud](../casestudies/telecom-usage-platform/CONF-CLOUD-CLUSTER.md).

### Clean-up

Stop the app. Keep the topics on both targets for Phases 6–7.

---

## Phase 6 — Secure the platform

**Opens after Module 7 · about 2 h · serves BR-4, BR-6**

### Goal

Replace "everyone uses one credential" with **least privilege that you can
prove**. The proof is not that the right things work, but that the wrong
things are **refused**, and that credentials can be rotated and revoked with
nobody noticing.

### Environment

- **Confluent Platform:** you are `ResourceOwner` on your prefix and may grant roles on it. Your identity is your LDAP user `lNN`; you also have a **partner** `lMM` from Module 7 whom you may use to play one persona at a time.
- **Confluent Cloud:** you are `EnvironmentAdmin` in your environment. Your service account is `sa-lNN-app` (more service accounts if the trainer's dry run shows you may create them, otherwise the trainer pre-creates `sa-lNN-fraud` and `sa-lNN-audit`).

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T6.1** | **Access matrix.** A table of personas × resources × operations covering at least: `billing-app`, `fraud-app`, `analytics-app`, `noc-operator` (sees health, groups and lag, **never** data), `auditor` (reads charges and audit only), `partner-gateway` (Cloud: may only write the partner feed). For each: the allowed set, the explicit denied set, the **narrowest** built-in role that gives it, and why a broader role (`ResourceOwner`, `EnvironmentAdmin`, `Operator`) is not used | BR-6 |
| **T6.2** | **Platform role bindings as code.** `scripts/rbac-platform.sh` creates exactly the bindings of the matrix on your prefix (idempotent), and `--list` prints what exists. Use your partner as one persona at a time: grant, test, revoke. Leave **no** binding for your partner at the end | BR-6, NFR-1 |
| **T6.3** | **Cloud identities and keys.** One service account and one API key **per application**. Role bindings scoped to named topics and groups. Keys are stored in mode-600 files outside the repository | BR-6, NFR-4 |
| **T6.4** | **Access tests as code.** `scripts/access-tests.sh` runs at least **24 checks** against Platform and Cloud, each *(principal, operation, resource, expected ALLOW or DENY)*, and reports every mismatch. At least **12 are DENY**. Include: a consumer using another application's group; the billing application writing a CDR topic; the auditor reading CDRs; the NOC operator reading messages; the partner gateway writing a billing topic; any access outside your prefix; any cluster-level alteration | BR-6 |
| **T6.5** | **Rotation with no downtime, and revocation.** On Cloud, rotate the key of a **running** TelcoPulse: create the second key, move the application, delete the first, with **no gap in produced and consumed records**. Then revoke by removing a role binding and show the denial, and show failure after deleting a key. Record how long each took to take effect | BR-6 |
| **T6.6** | **Security model review.** For each of the four targets, one row: encryption in transit on each listener, how clients authenticate, how access is authorised, and where an auditor would find *who was denied what*. Mark every finding on the **Apache** cluster that the move to Confluent fixes and any gap that **remains** | BR-4, BR-6 |
| **T6.7** | **Change audit trail.** Wrap your scripts so every change they make appends a record (who, when, what, ticket reference) to `lNN.audit.events`. Then ask the trainer for the Confluent Platform audit-log entries for your denied tests and compare what each source records and what it misses | BR-4 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A6.1** | `access-tests.sh` reports **0 mismatches** and prints the counts: at least 24 checks, at least 12 DENY |
| **A6.2** | The live bindings equal the matrix: `rbac-platform.sh --list` and `confluent iam rbac role-binding list` show **no extra binding**. Your partner has none at the end |
| **A6.3** | The rotation evidence shows a producer and a consumer running continuously with **no failed send, no authentication error and no gap in offsets** across the key change |
| **A6.4** | After you remove a role binding the matching test flips from ALLOW to DENY, and the time it took is recorded |
| **A6.5** | No two applications share a credential, and no application holds a role broader than its matrix row |
| **A6.6** | `lNN.audit.events` contains a record for each change you made in this phase |

### Evidence

`evidence/phase-6/`: matrix, `rbac-platform.sh` and its `--list` output, `access-tests.sh` output (full), rotation timeline, security review, audit-trail comparison. **No keys or secrets** (NFR-4).

### Toolbox

[Module 7 labs](../labs/module-07/README.md), the Module 7 guide, the Apache cluster's ACL model (`kafka-acls.sh --list`) for the comparison.

### Clean-up

Revoke partner bindings. Delete keys you no longer use. Keep the service accounts, the final keys and the topics for Phase 7.

---

## Phase 7 — Monitor and tune

**Opens after Module 8 · about 2 h · serves BR-3, BR-5, SLO-2, SLO-3, SLO-6**

### Goal

You can only keep a promise you can measure. Turn the SLOs into numbers on a
screen, set an alert that fires **before** the promise breaks, find out
whether one tuning change is real, and run the 4× peak in advance.

### Environment

Confluent Platform (Control Center, your own workload), Confluent Cloud
(Metrics API), and read-only on the shared Apache cluster's JMX endpoint.
Load stays within the caps in [§8 of the overview](README.md#8-rules-of-engagement-on-shared-infrastructure).
**The trainer assigns your load-test slot.**

### Tasks

| ID | Task | Serves |
| -- | ---- | ------ |
| **T7.1** | **Measurement plan.** For SLO-2, SLO-3, SLO-6 and the BR-5 disk rule: which metric, from which source, sampled how often, and the threshold that means *warning* and *breach* | SLO-2, SLO-3, SLO-6 |
| **T7.2** | **Baseline.** Run the normal profile for ten minutes on Platform. Record steady-state throughput, produce latency (the Kafka perf tools at `acks=all` for p99) and lag per group. State pass or fail per SLO | SLO-2, SLO-3 |
| **T7.3** | **Lag alert on Platform.** Create a Control Center alert on `lNN.billing` lag with a threshold and a buffer that you can justify from SLO-2. Make it **fire** by stopping the billing listener, then **resolve** by starting it. Record time to fire and to resolve. Write the runbook the alert links to | SLO-2, BR-9 |
| **T7.4** | **SLO report from the Metrics API.** Run the workload on Cloud for ten minutes. `scripts/slo-report.sh` queries the Metrics API for throughput, retained bytes and consumer lag and prints PASS or FAIL per SLO. Add a watch mode that raises a console alert when lag passes the warning threshold | SLO-2 |
| **T7.5** | **Monitoring comparison.** Control Center, the Metrics API and the Apache JMX/Prometheus endpoint (`http://broker-11.lab.internal:7071/metrics`): what does each show, how fresh is it, at what granularity, who needs which permission? Read at least five broker metrics from the JMX endpoint (for example bytes in, under-replicated partitions, request latency). Decide what TelcoPulse operations get in production, and why | BR-9 |
| **T7.6** | **Controlled tuning experiment.** One change at a time: one **producer** setting (batching, linger or compression) and one **consumer** setting (fetch size, poll size or the number of threads against partitions). Each with a hypothesis, three repeated runs before and after, and a table of throughput, p99 latency and broker-side effect. Conclude with a recommendation and what it costs | SLO-3 |
| **T7.7** | **Peak load test.** In your slot, run **200 CDR/s for 10 minutes** on Platform. Report SLO-2 and SLO-6 through the peak, the disk growth rate and what you extrapolate for the production inputs. Say what you would scale **first**: client settings, consumers, partitions or brokers, and what evidence from the run supports it | BR-5, SLO-2, SLO-6 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A7.1** | The measurement plan names a source and a threshold for every SLO. No SLO is "measured by looking at the dashboard" |
| **A7.2** | The alert **fired and resolved**: Control Center screenshots show both states, with timestamps within five minutes of your recorded actions |
| **A7.3** | `slo-report.sh` prints a PASS or FAIL line per SLO with the observed value, and its watch mode raises the alert in a test |
| **A7.4** | The tuning table has **three runs** per condition and a stated conclusion even if the change did nothing |
| **A7.5** | The peak test shows SLO-2 and SLO-6 verdicts, an extrapolation with formulas, and a scaling recommendation that cites the run |

### Evidence

`evidence/phase-7/`: measurement plan, baseline numbers, alert screenshots and runbook, `slo-report.sh` output, comparison table, tuning table with raw runs, peak report.

### Toolbox

[Module 8 labs](../labs/module-08/README.md), the Module 8 guide, the Module 4 guide on tuning.

### Clean-up

Stop all load. Delete the alert trigger if the trainer asks. Delete scratch topics.

---

## Phase 8 — Game day: troubleshoot and prevent message loss

**Opens after Module 9 · about 2.5 h · in class · serves BR-1, BR-9**

### Goal

Fix real faults quickly and correctly, and turn each one into a prevention
control. The trainer **injects the faults** into your prefix without telling
you what they are. You get symptoms, your runbooks and the tools you already
know.

### Environment

Confluent Platform and the shared Apache cluster, your prefix. The trainer
holds the rights that some fixes need. Knowing **when to stop and escalate**,
with good evidence, is part of the exercise.

### Before the day

| ID | Task | Serves |
| -- | ---- | ------ |
| **T8.0** | **First-response runbook** (two pages). A triage order for "something is wrong" that separates the **client**, the **network and connectivity**, the **access model**, the **topic configuration** and the **broker**. For each: the first command you run and the output that rules it in or out. Include how to read the application log, the broker-side evidence you are allowed to see, and when to escalate. Bring it; you will follow it | BR-9 |

### On the day

| ID | Task | Serves |
| -- | ---- | ------ |
| **T8.1** | **Four incidents**, 25 minutes each. Each arrives as a **symptom card**: what the business sees, when it started, nothing about the cause. Each learner gets **four of five** incident families from the ones the outline names: data **loss**, **throttling** and quotas, **replication** and write failures, **access**, and **connectivity**. The trainer leaves a different one out for neighbouring learners, and the order is random. For each incident, work in this order: *stabilise and assess, hypothesise, test, fix or escalate, verify, record* | BR-1, BR-9 |
| **T8.2** | **Incident report** for each (blameless, at most one page): impact, detection (who or what noticed, and how late), timeline with timestamps, root cause, what you changed, how you verified, **why it was not caught earlier**, and one prevention action with an owner | BR-9 |
| **T8.3** | **Message-loss prevention checklist.** At least **20 items**, grouped by *producer*, *topic*, *broker and replication*, *consumer*, *quotas and capacity*, *operations and change control*, and *Cloud versus Platform differences*. Each item has a **verification command or query**, a pass or fail result **for your platform today**, and an owner | BR-1 |
| **T8.4** | **Escalation package.** For any incident you could not finish yourself, prepare the ticket you would send to vendor support: environment and versions, a precise problem statement, timeline, configuration and log excerpts, what you tried, business impact and a **severity** with reason. State what the Confluent support model expects from the customer and what you can expect back | BR-9 |
| **T8.5** | **Close the loop.** For each incident, add one **alert or automated check** that would have caught it earlier, and verify at least one of them for real | BR-9, SLO-2 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A8.1** | Each of the four incidents shows: the **correct root cause** (the trainer confirms), a **fix that restores service** (or a complete escalation package when the right fix needs rights you lack), and a verification (`health-check.sh` back to PASS and the application processing) |
| **A8.2** | **No collateral change.** The trainer's before-and-after comparison of your prefix shows only changes that the fix required |
| **A8.3** | Each report has all eight parts, and at least one prevention action per incident is concrete (a metric and a threshold, a setting and a value, or a process step with an owner) |
| **A8.4** | The checklist has at least 20 items, each with a runnable verification, and **at least three items you failed** on your own platform with a plan |
| **A8.5** | At least one new alert or check from T8.5 is shown to fire when the fault is re-created |

### Evidence

`evidence/phase-8/`: the first-response runbook, four reports with timelines, the checklist with results, escalation package(s), the alert or check you added and its test.

### Toolbox

[Module 9 guide and labs where available](../guides/), your Part A client standard and runbooks, your Phase 7 measurement plan.

### Clean-up

The trainer restores anything left in an injected state. Run `health-check.sh cp` and keep the PASS output.

---

## Phase 9 — Ecosystem, migration, recovery and Peak Night

**Opens after Module 10 · about 6 h, of which 1¼ h is Peak Night in class · serves BR-1, BR-7, BR-8, BR-9**

### Goal

Use the rest of the Confluent ecosystem as an operator, **move the production
platform off Apache Kafka without losing a record**, make sure a failure of
the new cluster does not end the business, and then **run the platform live**
while the trainer breaks it.

### Environment

- **Confluent Platform:** Schema Registry, Connect (with Replicator) and ksqlDB on the services node. Everything on your prefix.
- **Confluent Cloud:** Schema Registry and managed connectors in `env-lNN`.
- **Shared Apache cluster:** the migration **source**. You have read on your prefix.
- **Staging cluster:** **site 2**, the recovery site. Full control.

### Part 1 — The ecosystem

| ID | Task | Serves |
| -- | ---- | ------ |
| **T9.1** | **Schema governance.** On Confluent Platform **and** Confluent Cloud, define a schema for the CDR (Avro or JSON Schema, justify the choice), register it under subjects named after your topics, set a **compatibility mode** and justify it. Then evolve it: a **compatible** change (accepted) and **two incompatible** changes (refused), each shown with the compatibility check. Write the evolution policy for developers: what changes are allowed, which side deploys first under your mode, and how a breaking change is introduced | BR-1, BR-9 |
| **T9.2** | **Connectors, managed and self-managed.** On Cloud, create a managed **Datagen Source** that stands in for the partner feed. Create it from a file, check its status, read its throughput, pause and resume it, then **delete it in the same session**. On Platform, list the Connect plugins (confirm Replicator is present), list connectors and read a connector's status through the REST API. Write a short comparison: who patches, who scales, who pays, how networking differs, and what each makes you responsible for | BR-9 |
| **T9.3** | **ksqlDB overview.** On Platform, create a stream over `lNN.cdr.voice` and a windowed table of international calls per subscriber per minute. Read a result and compare it with what TelcoPulse's fraud detector raised for the same period. Then **terminate the query and drop** both objects. State where a ksqlDB query would and wouldn't replace the fraud consumer, and what you would monitor if it ran in production (state stores, memory, the command topic) | BR-3 |

### Part 2 — Migrate from Apache Kafka to Confluent

| ID | Task | Serves |
| -- | ---- | ------ |
| **T9.4** | **Migration plan (paper first).** An assessment (topics, configs, access rules mapped to RBAC, groups and their positions, clients and versions, volumes). Three strategies compared in a decision matrix: **replicate then cut over consumers first**, **dual write**, **big bang**. The chosen runbook has a timeline with numbered steps, **go/no-go criteria** with numbers, validation steps, a **rollback** that fits BR-7's 15 minutes, and who must be told what. Link it to your client cutover inventory (T5.6) and your Part A client standard | BR-7 |
| **T9.5** | **Migration rehearsal.** With TelcoPulse running on the Apache cluster at the normal profile, replicate your core topics **into Confluent Platform with Replicator**: same topic names, same partition counts, v2 settings pre-created on the target by your script. Then cut over: a second TelcoPulse on profile `cp` takes over **consumption first**, then production, with the offset decision for the new group made **explicitly** (you must say where it starts and prove it). Stop replication only when it has caught up. Run the **rollback** once and time it. Measure: billing pause, records lost, records duplicated and replication lag. Your Apache credentials end up in the connector configuration: keep that file out of the submission (NFR-4) and say who can read the configuration of a running connector | BR-7, BR-1 |
| **T9.6** | **Verification script.** `scripts/verify-migration.sh` compares source and target for each replicated topic: record counts (end offsets), the **set of CDR IDs** for `cdr.voice` (a known `cdrId` is in every record) and the partition each key landed in. It prints PASS or FAIL per topic. The case study derives `chargeId` from topic, partition and offset, so it **cannot** be compared across clusters; say how you reconcile billing instead | BR-7 |

### Part 3 — Recovery: a second site

| ID | Task | Serves |
| -- | ---- | ------ |
| **T9.7** | **Second-site copy.** Replicate the core topics from **Confluent Platform to the staging cluster (site 2)** with **MirrorMaker 2** running on your VM, with the same topic names on site 2 and consumer-position information carried across. Explain in one paragraph **why the shared Connect cluster is the wrong place to run Replicator for this direction** (which cluster is it attached to, and where does Replicator expect to run?) and write the comparison table *Replicator versus MirrorMaker 2* (where it runs, licence, offset translation, configuration and access-rule sync, schema handling, monitoring) | BR-8 |
| **T9.8** | **Measure and rehearse recovery.** (a) **RPO:** while the platform runs at the normal profile for ten minutes, sample the newest record on both sites every 10 seconds and chart the gap. (b) **Failover drill:** stop the production application, start TelcoPulse on `local` against site 2 with the right prefix, **set consumer positions with a stated method**, and resume billing. Record decision-to-first-charge (RTO) and the data-loss window. (c) Write the **failback** plan (paper) | BR-8 |

### Part 4 — Close-out and Peak Night

| ID | Task | Serves |
| -- | ---- | ------ |
| **T9.9** | **Recommendation memo** (two pages, for management). What moves and when, what is the migration verdict (go or no-go) **with your measured numbers**, what is the recovery position (RPO and RTO you demonstrated), the main risks, the cost drivers, and the support model | BR-7, BR-8, BR-9 |
| **T9.10** | **Peak Night** (in class, 75 minutes) described in the next section | BR-3, BR-5, BR-9 |
| **T9.11** | **Final clean-up.** `scripts/cleanup.sh` removes everything you created on all four targets under your prefix, **including** Cloud connectors, the Replicator connector, MirrorMaker 2 processes, ksqlDB objects and subjects, and prints what it removed. Run it last and keep the output | NFR-3 |

### Acceptance

| ID | Check |
| -- | ----- |
| **A9.1** | The schema evolution shows one compatible change accepted and **two** incompatible changes refused, each with the registry's response, on Platform **and** Cloud |
| **A9.2** | The Cloud connector's lifecycle (created, running, paused, resumed, deleted) is shown with timestamps, and `confluent connect cluster list` is empty at the end of the session |
| **A9.3** | The ksqlDB result and the fraud alerts for the same window are shown side by side with the difference explained; `SHOW QUERIES` is empty afterwards |
| **A9.4** | The migration rehearsal reports **billing pause ≤ 5 minutes**, **zero lost** CDRs, **duplicates counted and explained**, and **rollback ≤ 15 minutes**. If a target is missed, the report says so and says why |
| **A9.5** | `verify-migration.sh` prints PASS for every replicated topic. A deliberately removed record on a copy makes it print FAIL for that topic |
| **A9.6** | The RPO chart covers ten minutes and shows the worst gap. The **drill** shows a charge produced on site 2 and an RTO from decision to that charge |
| **A9.7** | The memo gives a clear recommendation with the measured numbers of A9.4 and A9.6 |
| **A9.8** | `cleanup.sh` output lists removals on all four targets and a final `health-check` or `list` shows nothing of yours left beyond what the trainer asked you to keep |

### Evidence

`evidence/phase-9/`: schema files and registry responses, connector and ksqlDB transcripts, the migration plan and rehearsal log, `verify-migration.sh` output, RPO samples and chart, failover log, recommendation memo, Peak Night folder, `cleanup.sh` output.

### Toolbox

[Module 10 guide and labs where available](../guides/), the Module 5 guide (rolling restarts, balancing), your Phase 4 runbooks, [`infra/LAB-SETUP.md`](../infra/LAB-SETUP.md) §5 (the Replicator design: Apache → Platform).

---

## Peak Night

**In class · 75 minutes · T9.10 · serves BR-3, BR-5, BR-9**

This is the capstone's practical exam. You run TelcoPulse on **Confluent
Platform** at the peak profile. Everything you built is allowed; nothing
new is explained. The trainer injects **two faults** into your prefix that
you have not met in the game day, and gives you **one decision card**.

| Time | What happens |
| ---- | ------------ |
| 0–10 min | Run `health-check.sh cp`, open Control Center, start the **normal** profile, confirm your SLOs are green |
| 10 min | **Peak begins.** Start **200 CDR/s** and keep it running |
| 10–30 min | **Fault 1** appears (from the Peak Night catalog). Detect, diagnose, fix or escalate, verify |
| 30–50 min | **Fault 2** appears. Same procedure |
| 50–65 min | **Decision card:** the trainer announces that the Confluent Platform brokers may be unavailable for an unknown time. Decide **fail over to site 2 or hold**, and justify it **with your measured RPO and RTO**. If you fail over, do it |
| 65–75 min | Write the closing status. Run your health check. Stop the load |

Throughout, post a **status update every ten minutes** in this form, in
`peak-night/status.md`:

```
HH:MM  Impact: <what the business sees>  |  Cause: <known / suspected / unknown>
       Doing: <current action>  |  Next update: HH:MM
```

### Peak Night acceptance

| ID | Check |
| -- | ----- |
| **A9.9** | Each fault was **detected by your alert or check** or the report states why not. Time to detect and time to resolve are recorded |
| **A9.10** | Root causes are correct and each fix is verified. No collateral change |
| **A9.11** | The decision card has a written decision with numbers drawn from T9.8, made within its time box |
| **A9.12** | Status updates exist for every ten-minute mark, are factual and are written for a non-technical reader |

### Oral review (about 5 minutes each, after the exercise)

The trainer asks three questions from your own evidence, for example: *Why
this compatibility mode? What would you do differently if the Replicator lag
had not reached zero? What in your checklist is still failing?* Answers
that cite your own measurements score; general statements do not.

---

## Clean-up of the whole capstone

Run `scripts/cleanup.sh` (T9.11). Then confirm with the trainer which of your
resources stay until the course teardown
([`infra/confluent/README.md` Part D](../infra/confluent/README.md#part-d--teardown)).
Submit as described in [§7 of the overview](README.md#7-deliverables-and-submission)
using [`submission-template.md`](submission-template.md).
