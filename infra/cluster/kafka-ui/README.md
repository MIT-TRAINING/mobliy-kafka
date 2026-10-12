# Kafka UI for the shared cluster

[provectus/kafka-ui](https://github.com/provectus/kafka-ui) `v0.7.2` on the **trainer VM**,
published at **https://kafka-ui.kafka.supercloudlabs.com** through the VM's Caddy (Let's Encrypt).

| Piece | Where |
| ----- | ----- |
| Container `shared-kafka-ui` | `/opt/kafka-ui/` on the trainer VM, `127.0.0.1:18080` (the Docker labs already use the name `kafka-ui` and ports 8080/8088) |
| Kafka UI config | `/opt/kafka-ui/config.yml`: one Kafka UI "cluster" per user in `secrets/users.csv`, named after the user, connecting to `apache-kafka.lab.internal:9092` **as that SCRAM user** |
| Caddy site | `/etc/caddy/kafka-ui.caddy`, imported from `/etc/caddy/Caddyfile` |
| DNS | Route 53 `kafka-ui.kafka.supercloudlabs.com` A → trainer Elastic IP (created by hand, not in Terraform) |

Access model:

- Log in (browser basic-auth prompt) with the **Kafka username and password** from `secrets/users.csv`.
- A learner `lNN` lands on `/ui/clusters/lNN/all-topics`. Caddy returns 403 for any other user's
  cluster and for `/api/config` and `/actuator`. The broker ACLs then apply as usual: own `lNN.*`
  topics and groups only, no cluster-wide changes.
- **Consumers page:** learners have cluster `Describe`, so Kafka lists every learner's groups, and
  v0.7.2 fails the whole page (500, `FindCoordinator ... authorization failure`) on the first group it
  may not describe. Caddy therefore forces `search=lNN.` on a learner's
  `/api/clusters/lNN/consumer-groups/paged`, so only their own groups are described; the page's search
  box has no effect for learners. A topic's **Consumers** tab describes all groups and still fails for
  learners. Remove this once Kafka UI skips undescribable groups (kafbat/kafka-ui PR #1980).
- `trainer` (Kafka super user) can open every cluster entry, including `trainer`, which shows all topics
  and groups. Opening a learner's entry connects as that learner, so its Consumers page fails the same way.
- The dashboard lists all 20 cluster entries with their topic counts; the names and counts are
  visible to everyone, the contents are not.

Deploy or update (idempotent; re-run after `users.csv` changes):

```bash
infra/cluster/scripts/deploy-kafka-ui.sh
```

The trainer VM must be running for the UI to be up (`restart: unless-stopped` brings it back on boot).
