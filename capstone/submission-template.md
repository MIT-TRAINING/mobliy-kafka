# Capstone Submission — TelcoPulse Platform Operations

> Copy this file to `capstone-lNN/README.md` and fill it in. Delete the
> guidance lines (in italics) as you go. Specification:
> [`README.md`](README.md), [Part A](part-a-apache-kafka.md), [Part B](part-b-confluent-kafka.md).
> **Never put a password, API secret or key in this repository** (NFR-4).

| Field | Value |
| ----- | ----- |
| Learner ID (`lNN`) | |
| Name | |
| Submission date | |
| Repository or archive | |
| Trial licence / cluster state when you worked (anything unusual) | |

---

## 1. Summary (half a page)

*In plain language for a manager: what you took over, what you changed, what
you proved, what is still open. Quote three numbers from your own
measurements (for example: failover time, migration pause, worst replication
gap).*

---

## 2. Requirement coverage

*One row per requirement. "Evidence" is a path under `evidence/`. Say **Met**,
**Partly** or **Not met**, never leave a cell empty.*

| Requirement | Met? | Evidence | One-line justification |
| ----------- | ---- | -------- | ---------------------- |
| BR-1 No acknowledged record lost on one broker failure | | | |
| BR-2 72 h outage tolerated, retention with margin | | | |
| BR-3 Fraud lag under 60 s, normal and peak | | | |
| BR-4 Retention for regulation; changes traceable | | | |
| BR-5 4× peak for two hours | | | |
| BR-6 Least privilege, rotation without downtime | | | |
| BR-7 Migration: pause ≤ 5 min, no loss, rollback ≤ 15 min | | | |
| BR-8 Second site: RPO ≤ 5 min, rehearsed failover | | | |
| BR-9 Runbooks, alerts, incident reviews | | | |
| NFR-1 Reproducible by script | | | |
| NFR-2 Isolation by prefix | | | |
| NFR-3 Etiquette and cost | | | |
| NFR-4 No secrets in the submission | | | |
| NFR-5 Evidence, not claims | | | |
| SLO-1 … SLO-6 (one row each, with the measured value) | | | |

---

## 3. Phase reports

*For each phase: tick every acceptance check you ran, write the **result**,
and point to the evidence. If a check failed or you skipped it, say so and
why. A failed check with a good explanation scores better than a missing one.*

### Phase 1 — Design review and staging cluster

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A1.1 Health check all-PASS on three targets | | | |
| A1.2 Health check reports a stopped broker by cause | | | |
| A1.3 Wrong password gives an authentication FAIL within 30 s | | | |
| A1.4 Inventory complete, every cell has a command | | | |
| A1.5 Topic review: ≥ 5 ranked findings, ≥ 2 risks with a change | | | |

**Decisions and why** (trace to BR/SLO IDs):

**What I would do differently:**

### Phase 2 — Topic contracts and durability

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A2.1 `--diff` catches a manual change, apply is idempotent | | | |
| A2.2 v2 meets BR-2 and BR-4 on all eight topics | | | |
| A2.3 Retention proof with two timed readings | | | |
| A2.4 Durability matrix: 18 cells, explained by ISR | | | |
| A2.5 Sizing estimate within ±30 % of measured | | | |
| A2.6 Developer contract guide | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 3 — Client operations

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A3.1 Client standard for three classes, every setting justified | | | |
| A3.2 Rebalance on join, clean leave and hard kill timed | | | |
| A3.3 Reset refused while active, dry run before execute | | | |
| A3.4 Duplicate shown with its detection key; guarantee stated | | | |
| A3.5 Unparseable record: where it is counted | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 4 — Operations and high availability

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A4.1 Capacity plan with N+1 and 70 % disk | | | |
| A4.2 Replica map by rack | | | |
| A4.3 Trainer's reassignment verified; broker 11 drained | | | |
| A4.4 Staging throttle appears and disappears; stalled move fixed | | | |
| A4.5 Three failure drills measured against SLO-1 and SLO-5 | | | |
| A4.6 Rolling restart: zero failed sends, gate waited | | | |
| A4.7 Notes from the trainer's shared-cluster failure demo | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 5 — Move to Confluent

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A5.1 Decision record for three workloads | | | |
| A5.2 `--diff` on `cp` and `ccloud` | | | |
| A5.3 Setting map: ≥ 6 real differences | | | |
| A5.4 Health check on `cp` and `ccloud` | | | |
| A5.5 Control Center answers with CLI equivalents | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 6 — Secure the platform

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A6.1 Access tests: 0 mismatches, ≥ 24 checks, ≥ 12 DENY | | | |
| A6.2 Live bindings equal the matrix; partner has none | | | |
| A6.3 Rotation with no failed send and no gap | | | |
| A6.4 Revocation flips ALLOW to DENY; time recorded | | | |
| A6.5 No shared credential; no over-broad role | | | |
| A6.6 Change audit records in `lNN.audit.events` | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 7 — Monitor and tune

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A7.1 Measurement plan: source and threshold per SLO | | | |
| A7.2 Alert fired and resolved | | | |
| A7.3 `slo-report.sh` PASS/FAIL with values; watch mode tested | | | |
| A7.4 Tuning: three runs per condition, conclusion | | | |
| A7.5 Peak test: SLO verdicts, extrapolation, scaling advice | | | |

| SLO | Normal | Peak | Verdict |
| --- | ------ | ---- | ------- |
| SLO-2 billing lag | | | |
| SLO-3 produce p99 | | | |
| SLO-6 fraud lag | | | |

**Decisions and why:**

**What I would do differently:**

### Phase 8 — Game day

| Incident | Time to detect | Time to resolve | Root cause | Fixed or escalated | Report |
| -------- | -------------- | --------------- | ---------- | ------------------ | ------ |
| 1 | | | | | |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A8.1 Four correct root causes, fixes or escalations, verified | | | |
| A8.2 No collateral change | | | |
| A8.3 Eight-part reports with a concrete prevention action | | | |
| A8.4 Checklist ≥ 20 items, ≥ 3 own failures | | | |
| A8.5 New alert or check fires on re-creation | | | |

**What I would do differently:**

### Phase 9 — Ecosystem, migration, recovery, Peak Night

| Check | Run? | Result | Evidence |
| ----- | ---- | ------ | -------- |
| A9.1 Schema evolution: 1 compatible accepted, 2 incompatible refused, on Platform and Cloud | | | |
| A9.2 Cloud connector lifecycle; none left | | | |
| A9.3 ksqlDB result vs fraud alerts; no queries left | | | |
| A9.4 Migration: pause, loss, duplicates, rollback time | | | |
| A9.5 `verify-migration.sh` PASS, and FAIL on a removed record | | | |
| A9.6 RPO chart and failover drill (RTO) | | | |
| A9.7 Recommendation memo with measured numbers | | | |
| A9.8 `cleanup.sh` output on all four targets | | | |
| A9.9–A9.12 Peak Night | | | |

**Migration and recovery numbers:**

| Measure | Target | Observed |
| ------- | ------ | -------- |
| Billing pause | ≤ 5 min | |
| Records lost | 0 | |
| Records duplicated (explained?) | detectable | |
| Rollback time | ≤ 15 min | |
| Worst replication gap (RPO) | ≤ 5 min | |
| Failover decision → first charge (RTO) | ≤ 30 min | |

**Decisions and why:**

**What I would do differently:**

---

## 4. Checklist and runbooks (index)

| Document | Path |
| -------- | ---- |
| Topic review, naming convention, KRaft note | `docs/` |
| Contract guide and client standard | `docs/` |
| Capacity plan, replica map, upgrade policy | `docs/` |
| Decision records and setting map | `docs/` |
| Access matrix and security review | `docs/` |
| Measurement plan, alert runbook | `docs/` |
| First-response runbook | `docs/` |
| Message-loss prevention checklist | `docs/` |
| Migration plan, failback plan, recommendation memo | `docs/` |

---

## 5. Honest limits

*What did not work, what you could not test and why, and which findings are
change requests for the development team. Short and specific.*

---

## 6. Self-scan and clean-up

- [ ] The secret scan from [README §7](README.md#7-deliverables-and-submission) prints nothing
- [ ] `scripts/cleanup.sh` output is in `evidence/phase-9/`
- [ ] No connector, ksqlDB query, MirrorMaker 2 process or partner binding is left running
- [ ] Every table cell above is filled in, even if with "Not met"
