# Cloud Cost Analysis — GlowCart

Estimated costs if this pipeline is deployed to AWS with the current data volume (~10,000 events/day).

## Cost Breakdown

| Component | Local (current) | AWS Equivalent | Est. Cost/month |
|---|---|---|---|
| Kafka | Docker container | MSK (t3.small, 2 broker) | ~$150 |
| Spark processing | Local PySpark | EMR Serverless | ~$50–200 |
| Storage (Parquet) | Local disk | S3 Standard (100GB) | ~$2.50 |
| Airflow | Docker container | MWAA (small env) | ~$80 |
| Serving API | uvicorn localhost | ECS Fargate (0.25 vCPU) | ~$15 |
| **Total** | **Rp 0 (local)** | | **~$300–450/month** |

## Scaling Analysis

| Scenario | Data Volume | Est. Cost/month | Bottleneck |
|---|---|---|---|
| Current (local) | 10K events/day | Rp 0 | — |
| Early startup | 1M events/day | ~$350 | MSK broker count |
| Growth stage | 10M events/day | ~$900 | EMR Serverless compute |
| Scale | 100M events/day | ~$3,500+ | Storage + compute |

## Cost Optimization Strategies

**Storage:** Replace S3 Standard with S3 Intelligent-Tiering for data > 30 days — can save ~40% of storage costs.

**Compute:** EMR Serverless with auto-scaling is cheaper than a fixed-size EMR cluster for batch workloads that are not continuous.

**Kafka:** For volumes < 5M events/day, Amazon Kinesis is cheaper than MSK (~$50/month vs ~$150/month).

**Serving:** DuckDB on ECS Fargate is sufficient up to ~100GB of data. Above that, migrating to BigQuery (pay-per-query) is more cost-efficient than a ClickHouse cluster.

## If the Interviewer Asks "10x Data?"

From 10K → 100K events/day:
- Storage: +$0.25/month (not significant)
- EMR Serverless: +$20–50/month (auto scale)
- MSK: no upgrade needed until 1M events/day
- **Total delta: ~$25–50/month**

The first bottleneck that will appear is Kafka consumer throughput — the solution is to add partitions and scale consumers horizontally in Kubernetes.
