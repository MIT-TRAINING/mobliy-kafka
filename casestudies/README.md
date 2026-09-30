# Case studies

Runnable, instructor-led case studies that apply the Kafka concepts from the training modules to a realistic business problem.

| Case study | Modules | What it shows | Start here |
|---|---|---|---|
| [Telecom Usage Platform](telecom-usage-platform/) | 1, 2, 3 | Call, SMS and data usage records flowing through billing, fraud detection, analytics and a plan cache on a 3-broker KRaft cluster, with live topic configuration, retention, compaction and a durability drill | [README](telecom-usage-platform/README.md) · [Demo script](telecom-usage-platform/DEMO-SCRIPT.md) |

## Quick start (Telecom Usage Platform)

```bash
cd casestudies/telecom-usage-platform
./scripts/cluster.sh up                 # 3-broker cluster from Modules 2 and 3
mvn -q package
java -jar target/telecom-usage-platform-1.0.0.jar
# open http://localhost:8090/
```

Requirements: Docker, JDK 17 or later, Maven 3.9+.
