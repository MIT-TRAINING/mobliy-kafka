# Run the telco case study on the shared AWS Kafka cluster

The application runs unchanged against the course's shared Apache Kafka cluster on AWS
(Kafka 4.3.1, 3 KRaft controllers, 4 brokers, SASL/SCRAM-SHA-512, prefix ACLs). The
`shared` profile in [`src/main/resources/application-shared.yml`](src/main/resources/application-shared.yml)
switches the connection; nothing else changes.

| Item | Value |
| ---- | ----- |
| Where to run | A **lab VM**. Only lab VMs can reach the cluster; your laptop cannot |
| Bootstrap | `apache-kafka.lab.internal:9092` |
| Credentials | `~/kafka/apache.properties` on the VM |
| Name prefix | Your Kafka username plus a dot: `l07.` for a learner, `trainer.` for the trainer |
| Dashboard | `https://<vm-host>/proxy/8090/` (keep the trailing slash) |

Cluster background: [`../../infra/cluster/PLAN.md`](../../infra/cluster/PLAN.md).
Getting onto the cluster nodes: [`../../infra/guides/kafka-cluster-connect.md`](../../infra/guides/kafka-cluster-connect.md).

---

## 1. Check that the VM can reach the cluster

```bash
cat ~/kafka/apache.properties          # must exist
kafka-topics.sh --bootstrap-server apache-kafka.lab.internal:9092 \
  --command-config ~/kafka/apache.properties --list
```

- **Trainer VM:** the file is already there, with the `trainer` super user.
- **Learner VMs:** the trainer writes the file with
  `infra/cluster/scripts/distribute-client-config.sh` (run from the admin laptop, VMs running).
- **`--list` times out:** the cluster is stopped. The trainer starts it from the admin laptop with
  `infra/cluster/scripts/cluster-power.sh start`.

## 2. Build

```bash
cd casestudies/telecom-usage-platform
mvn -q package -DskipTests
```

Use a build that includes the relative-URL fix in `static/index.html`. An older jar shows
"cluster unreachable" when opened through the `/proxy/8090/` path.

## 3. Set the connection from `apache.properties`

```bash
CFG=~/kafka/apache.properties
export TELCO_KAFKA_BOOTSTRAP=$(sed -n 's/^bootstrap.servers=//p' $CFG)
export TELCO_KAFKA_USER=$(sed -n 's/.*username="\([^"]*\)".*/\1/p' $CFG)
export TELCO_KAFKA_PASSWORD=$(sed -n 's/.*password="\([^"]*\)".*/\1/p' $CFG)
export TELCO_PREFIX="${TELCO_KAFKA_USER}."
echo "$TELCO_KAFKA_BOOTSTRAP as $TELCO_KAFKA_USER, prefix $TELCO_PREFIX"
```

Why the prefix matters:

- **Learners** have ACLs only on topics, consumer groups and transactional IDs that start with
  `lNN.`. Without the prefix, startup fails with `TopicAuthorizationException`.
- **The trainer** is a super user, so any prefix works. Use `trainer.` so your topics never
  collide with a learner's.

## 4. Run

```bash
java -jar target/telecom-usage-platform-1.0.0.jar --spring.profiles.active=shared
```

At startup the application creates its topics, all with your prefix:

| Topic | Purpose |
| ----- | ------- |
| `cdr.voice`, `cdr.sms`, `cdr.data` | Call detail records |
| `billing.charges` | Rated charges |
| `network.telemetry` | Radio measurements (RF 2, `acks=1`) |
| `subscriber.plan` | Compacted plan table |
| `audit.events` | Config changes and fraud alerts |
| Drill topic | The `acks` / `min.insync.replicas` experiment |

It also starts the consumer groups `billing`, `fraud-detection`, `usage-analytics` and
`plan-cache`, with the same prefix. The shared cluster has 4 brokers, so RF 3 with
`min.insync.replicas=2` works as designed.

## 5. Open the dashboard

Open `https://<vm-host>/proxy/8090/`, for example
`https://lab-trainer.kafka.supercloudlabs.com/proxy/8090/`. Keep the trailing slash.

The header should show the cluster ID, the active controller (1, 2 or 3) and
`broker 11 up · broker 12 up · broker 13 up · broker 14 up`. Then follow
[`DEMO-SCRIPT.md`](DEMO-SCRIPT.md): **Seed subscriber plans** first, then **Send 100 CDRs**.

Check from the CLI on the VM:

```bash
kafka-topics.sh --bootstrap-server $TELCO_KAFKA_BOOTSTRAP --command-config $CFG --list | grep "^$TELCO_PREFIX"
kafka-consumer-groups.sh --bootstrap-server $TELCO_KAFKA_BOOTSTRAP --command-config $CFG --list | grep "^$TELCO_PREFIX"
```

---

## 6. How the shared cluster differs from the local Docker cluster

| Area | On the shared cluster |
| ---- | --------------------- |
| **Throughput** | Learners are capped at 1 MB/s produce per broker. "Start 500 CDR/s" and "Send 20,000 telemetry points" are throttled. That is the quota, not a bug. The trainer user has a 50 MB/s cap |
| **Topic creation** | Learners have `controller_mutation_rate=5`, so the first start can pause for a few seconds while the partitions are created |
| **Panel 3, change a setting live** | Works on your own prefixed topics. Cluster-wide broker changes are rejected for learners with `ClusterAuthorizationException`, as the course intends |
| **Durability drill (DEMO-SCRIPT part 7)** | Learners cannot stop brokers, so they run this part on the local cluster. The trainer can run it here: from the admin laptop, `infra/cluster/scripts/broker-failure-demo.sh stop broker-12`, watch the dashboard mark the broker DOWN, then `... start broker-12`. Announce it first, because every learner is affected |
| **Clean-up** | Topics stay on the shared cluster after the app stops. See §7 |

## 7. Clean up

```bash
kafka-topics.sh --bootstrap-server $TELCO_KAFKA_BOOTSTRAP --command-config $CFG \
  --delete --topic "${TELCO_PREFIX}.*"
```

The application recreates its topics at the next start.

---

## Troubleshooting

| Symptom | Cause / fix |
| ------- | ----------- |
| App exits at startup with `TimeoutException` | The cluster is stopped, or you are not on a lab VM |
| `SaslAuthenticationException` | Wrong user or password. Check the variables from step 3 |
| `TopicAuthorizationException` / `GroupAuthorizationException` | `TELCO_PREFIX` is missing or does not match the username. It needs the trailing `.` |
| Dashboard shows "cluster unreachable" but the app log is clean | The jar predates the relative-URL fix, or the URL is missing its trailing `/` |
| `Web server failed to start. Port 8090 was already in use` | Another copy is running. Stop it, or add `--server.port=8091` and open `/proxy/8091/` |
