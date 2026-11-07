# Submission code review · 7 November 2026

Reviewed source revision `68a5e8a` and the running services on all four VMs. The application source passed 30 existing tests and six additional integration scenarios using a disposable PostgreSQL database on an internal Docker network. Notification HTTP responses were simulated during the integration check.

## Verification

| Area | Result |
|---|---|
| API contracts, cache bounds and readiness | 18 tests passed |
| Cost calculations | 1 test passed |
| Batch transactions, offsets and compaction | 6 tests passed |
| Experiment processor | 3 tests passed |
| Notifier readiness | 2 tests passed |
| Full lifecycle, averages, notification transitions and replay | Passed against PostgreSQL |
| Failed-batch rollback | Passed against PostgreSQL |
| Duplicate sensor readings across batches | Passed against PostgreSQL |
| Two concurrent aggregation transactions | Passed against PostgreSQL |
| Real history queries, inclusive filters and active-history freshness | Passed against PostgreSQL |
| Retry scheduling, concurrent outbox claims and five payload fields | Passed against PostgreSQL with simulated HTTP |

The review found no failing case in these checks. This verification covers the application source and the listed scenarios. The existing unit tests include mocks, while the six integration scenarios exercised actual SQL transactions. The private-key and AWS access-key pattern check found no matches in tracked files under 1 MB.

## Submission and deployment differences

1. **The running API and the current source differ.** The VM uses `group17/temperature-api:baseline-final`, whose `app.py` matches revision `0a81e25`. The checkout contains the newer bounded-cache implementation. The live `/health` succeeds and `/ready` returns 404. The source API tests cover the newer implementation. Performance measurements must retain the source/image identity used in their run.
2. **The checked-in Compose file is a development template.** Defaults include four consumers, three notifiers, topic `group17` and the administrator database login. The live deployment overrides these with two consumers, two notifiers, topic `experiment` and restricted roles through root-readable configuration files on the VM. A plain Compose startup from this checkout will not reproduce the live deployment.
3. **The distributed API proposal is a tested candidate.** The three personal VMs served synthetic HTTP traffic behind a temporary Nginx gateway in the comparison test. They currently do not serve the production API. Dedicated read-only database roles, connections and a live-traffic cutover remain to be completed before describing that layout as deployed.

The README now states this deployment snapshot and labels the Kubernetes documentation as historical. The review made documentation changes only.

## Kafka assignment

At inspection, `experiment` had sixteen partitions. Two processes in `group17-assessment-20261008-a` were stable members and each owned eight partitions. Both ran on the group VM. HTTP API replicas do not join Kafka consumer groups.

Adding consumers in that same group redistributes these sixteen partitions. Four consumers typically receive four partitions each. Two consumers on Group plus one on each personal VM would make five consumers, with a distribution such as four plus three plus three plus three plus three. Consumers with different group IDs receive independent copies of the stream.

Before selecting a submission release, record its exact source revision, image identifiers and deployment configuration together. Keep benchmark evidence attached to that release.
