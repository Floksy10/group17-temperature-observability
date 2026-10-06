# Four-VM workflow for Group 17

This is the tested development workflow. Keep the Kafka topic `group17` and
the local Notifications mock selected while generating your own experiments.
The lecturers' `experiment` topic and real Notifications service are for the
course assessment; do not publish local test experiments to that topic.

## Roles

| VM | Initial role | Needs Group 17 Kafka files? | Needs direct database access? |
| --- | --- | --- | --- |
| Group VM (`63.33.242.251`) | Four Kafka consumers, PostgreSQL, REST API, notification workers, monitoring | Yes | Hosts it |
| Nick/client4 (`54.195.153.114`) | Official development producer | Already present | No for producer |
| Ibrahim's VM | HTTP load client, 1,000 requests at concurrency 25 | No | No |
| Yorick's VM | HTTP load client, 1,000 requests at concurrency 25 | No | No |

Nick's VM already has the repository and `/home/ubuntu/group17-auth`. Ibrahim
and Yorick only need their own VM SSH access, the GitHub repository, Python 3,
and HTTP access to the group API. This uses all four VMs without exposing
PostgreSQL or sharing private keys. It moves test traffic generation away from
the VM being measured. A personal VM can later host an extra consumer, but
that requires a restricted database tunnel and a matching performance test;
simply adding consumers was not consistently faster in our measurements.

## Ibrahim and Yorick: connect and get the code

The owner of `Floksy10/group17-temperature-observability` invites each
teammate's **own GitHub account** as a collaborator. Each teammate accepts the
invite. Do not send SSH private keys, database passwords, or Kafka keystores
through chat or Git.

Each teammate runs the following from their own Mac or laptop Terminal, with
their own VM SSH key and public VM address:

```bash
ssh -o IdentitiesOnly=yes -i "<path-to-your-own-vm-private-key>" ubuntu@<your-vm-public-ip>
```

The prompt should now be `ubuntu@...`, not the Mac prompt. Run the following
**on that personal VM**. Give the generated **public** key, ending in `.pub`,
to your own GitHub account under Settings → SSH and GPG keys. This is a
different key from the one used to log into the VM.

```bash
sudo apt-get update
sudo apt-get install -y git python3 curl
ssh-keygen -t ed25519 -f "$HOME/.ssh/group17_github" -C "<your-name>-group17-vm"
cat "$HOME/.ssh/group17_github.pub"
```

After adding the public key to GitHub and accepting the repository invitation:

```bash
GIT_SSH_COMMAND="ssh -i $HOME/.ssh/group17_github -o IdentitiesOnly=yes" \
  git clone git@github.com:Floksy10/group17-temperature-observability.git
cd ~/group17-temperature-observability
git config core.sshCommand "ssh -i $HOME/.ssh/group17_github -o IdentitiesOnly=yes"
git status
curl -fsS http://63.33.242.251:3003/health
```

Expected results: `git status` shows a clean `main` branch, and `/health`
prints `{"status":"ok"}`. If GitHub authentication fails, check that the key
was added to the teammate's GitHub account and the collaborator invite was
accepted. If `/health` fails, check that the group VM is running and its
security group allows port 3003 from that teammate VM. Neither check requires
the group database password or Kafka credentials.

## Run one coordinated experiment

Use four separate SSH terminal windows. On the **group VM**, confirm the
development mode and check that all four consumers and the API are running:

```bash
cd ~/group17-temperature-observability
grep -E '^(KAFKA_TOPIC|NOTIFICATIONS_URL)=' .env
docker compose --profile test --env-file .env -f deploy/compose.yaml ps
curl -fsS http://127.0.0.1:3003/health
```

The expected settings are `KAFKA_TOPIC=group17` and
`NOTIFICATIONS_URL=http://notifications-mock:3000/api/notify`. Record the
current database counts and outbox ID **before** each run:

```bash
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -Atc \
  'SELECT (SELECT count(*) FROM experiments),
          (SELECT count(*) FROM measurements WHERE during_experiment),
          (SELECT coalesce(max(id),0) FROM notification_outbox);'
```

Find an existing experiment ID with historic readings. Send this ID to
Ibrahim and Yorick for their HTTP load commands:

```bash
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -Atc \
  'SELECT experiment_id FROM measurements WHERE during_experiment
   GROUP BY experiment_id HAVING count(*) >= 10
   ORDER BY max(measured_at) DESC LIMIT 1;'
```

On **Nick's VM**, build a load configuration. This example creates 100
simultaneous experiments with four sensors each, one measurement per second,
and 20 historic measurements per experiment:

```bash
cd ~/group17-temperature-observability
python3 scripts/generate_experiments.py --count 100 --sensors 4 \
  --sample-rate-ms 1000 --samples 20 > /tmp/group17-load.json
```

When everyone is ready, Nick starts the producer below. Change the container
name for each run, or remove the old stopped container first. Ibrahim and
Yorick run their commands immediately after Nick starts it.

```bash
docker run -d --name group17-producer-test-01 \
  --mount type=bind,source="$HOME/group17-auth",target=/experiment-producer/auth,readonly \
  --mount type=bind,source=/tmp/group17-load.json,target=/experiment-producer/load.json,readonly \
  dclandau/cec-experiment-producer \
  --config-file /experiment-producer/load.json \
  --topic group17 --brokers kafka.cec.dlandau.nl:19092
docker wait group17-producer-test-01
```

On **Ibrahim's VM** and **Yorick's VM**, each runs the same command with the
experiment ID obtained above. Run both at about the same time. Together they
send 2,000 requests with about 50 concurrent requests:

```bash
cd ~/group17-temperature-observability
python3 scripts/load_api.py --base-url http://63.33.242.251:3003 \
  --experiment-id <existing-experiment-id> --requests 1000 --concurrency 25
```

Each client should report `requests=1000 successful=1000`. Nick's `docker
wait` should return `0`. If a client reports failures, inspect group API logs
and check the VM's network path before comparing performance numbers.

On the **group VM**, after the producer exits, the experiment count should
increase by 100 and the historic measurement count by 2,000. Queues should
drain to zero; allow a few seconds for consumers to catch up:

```bash
docker exec group17-temperature-observability-database-1 \
  psql -U temperature -d temperature -Atc \
  'SELECT (SELECT count(*) FROM experiments),
          (SELECT count(*) FROM measurements WHERE during_experiment),
          (SELECT count(*) FROM pending_readings),
          (SELECT count(*) FROM notification_outbox WHERE sent_at IS NULL);'
```

The temperature graph is at
`http://63.33.242.251:3003/dashboard?experiment-id=<new-experiment-id>`.
To find a newly created experiment ID, use the same SQL query above with
`ORDER BY max(measured_at) DESC`. The group API `/costs` page shows CPU and
memory over time. The course Grafana is separate and does not necessarily
display development-topic notifications sent to our local mock receiver.

For a lower-load reference, change Nick's producer config to `--sensors 2`
and `--sample-rate-ms 1000`. For faster sampling, change it to
`--sample-rate-ms 500`. Keep the number of experiments, sensors, and API
requests in your notes; otherwise runs are not comparable. See
[performance.md](performance.md) for measured results and limits.

## Optional later step: run consumers on personal VMs

Only do this after the load-client workflow works and the group has measured
whether moving consumers helps. Each personal consumer must use the same
`KAFKA_GROUP_ID`, Group 17 Kafka TLS files, and the group database. It must
**not** start a second database. The group database remains bound to
`127.0.0.1` on the group VM. Use a dedicated, restricted SSH local-forward
for each personal VM; never open PostgreSQL port 5432 to the internet.

Nick's optional consumer and tunnel are already configured but stopped.
Ibrahim and Yorick each need their own VM private/public IP, a dedicated
tunnel public key, and secure delivery of the group database password and
Kafka TLS files. The group VM maintainer then restricts each tunnel key to
that VM's source IP and `127.0.0.1:5432`, following the exact commands in
[personal-consumer-setup.md](personal-consumer-setup.md). Set up and verify one
VM at a time. Measure
with the same producer/API workload before keeping extra consumers enabled.
