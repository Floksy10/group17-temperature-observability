# Optional Kafka consumer on Ibrahim's or Yorick's VM

First complete [the four-VM load-client setup](four-vm-workflow.md). Do this
consumer setup only for a VM the group has decided to include in a measured
distributed-consumer run. Apply the steps to Ibrahim and Yorick **separately**:
each VM has its own SSH and GitHub keys, Kafka files, tunnel key, and local
`.env.personal`. PostgreSQL remains on the group VM and is never published on
the internet. Do not copy Nick's VM or GitHub private key.

## 1. Record the VM's network addresses

On the teammate's VM:

```bash
hostname
ip -4 addr show
ip route get 10.0.1.183
```

The last command shows the source address that VM uses to reach the group VM's
private address. It should normally be an address in the same private network.
If the private route does not work, use the group VM's public SSH address
`63.33.242.251` for the tunnel and restrict the authorized key to the actual
source IP seen by group SSH. Check AWS security-group access before continuing.
Record the teammate VM's public IP, private/source IP, SSH username, and the
owner's local path to **their own** VM SSH key. Do not put private keys in Git.

## 2. Install Docker and copy Group 17 Kafka files

On the teammate's VM:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
mkdir -p "$HOME/group17-auth"
```

Log out and reconnect to activate Docker group membership, then check
`docker --version` and `docker compose version`. From the teammate's **own
laptop Terminal**, copy their Group 17 `ca.crt`, `client-ssl.properties`,
`kafka.keystore.pkcs12`, and `kafka.truststore.pkcs12` with `scp -i
<their-own-vm-ssh-key> ... ubuntu@<their-vm-public-ip>:/home/ubuntu/group17-auth/`.
Do not run this `scp` command from inside the Ubuntu VM when the files are on
the laptop. On the VM, run `ls -l ~/group17-auth` to confirm all four files.

## 3. Generate a dedicated database-tunnel key

On the teammate's VM:

```bash
ssh-keygen -t ed25519 -f "$HOME/.ssh/group17_db_tunnel_ed25519" \
  -C "<ibrahim-or-yorick>-group17-db-tunnel" -N ''
cat "$HOME/.ssh/group17_db_tunnel_ed25519.pub"
```

Send **only the `.pub` line** to the group VM maintainer. On the group VM,
the maintainer checks that it begins with `ssh-ed25519`, then adds one line
to `/home/ubuntu/.ssh/authorized_keys` in this form, replacing the address
and public key. Use Ibrahim's source IP for Ibrahim and Yorick's source IP
for Yorick:

```text
from="<teammate-source-ip>",restrict,port-forwarding,permitopen="127.0.0.1:5432",command="/bin/false" ssh-ed25519 <public-key-material> <comment>
```

Keep `authorized_keys` mode `0600`. This key can forward only to the group
VM's local PostgreSQL port; it cannot open a shell. If AWS routes the tunnel
over a public address, use the VM's actual public SSH source IP in `from=`.
The group VM must allow SSH port 22 from that source in its security group.

## 4. Verify the group SSH host and start the tunnel

On the group VM, the maintainer reads the host-key fingerprint with
`ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` and tells the teammate the
fingerprint through the agreed trusted channel. On the teammate VM:

```bash
ssh-keyscan -t ed25519 10.0.1.183 2>/dev/null | ssh-keygen -lf -
```

Compare the fingerprint before accepting the host key. If it matches, add
the key to `~/.ssh/known_hosts`:

```bash
ssh-keyscan -t ed25519 10.0.1.183 >> "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
```

Use the group VM's public address instead of `10.0.1.183` in both commands
if the private route is unavailable. The Docker bridge gateway can vary by
VM; determine it rather than assuming Nick's `172.17.0.1`:

```bash
DOCKER_GATEWAY=$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')
echo "$DOCKER_GATEWAY"
```

The following block creates the same restricted, self-restarting tunnel
service used on Nick's VM. Copy the **whole block** into that VM's terminal,
after setting `GROUP_SSH_ADDRESS` to the address verified above:

```bash
GROUP_SSH_ADDRESS=10.0.1.183
sudo tee /etc/systemd/system/group17-db-tunnel.service >/dev/null <<EOF
[Unit]
Description=Restricted tunnel to Group 17 PostgreSQL
Wants=network-online.target
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=simple
User=ubuntu
ExecStart=/usr/bin/ssh -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -i /home/ubuntu/.ssh/group17_db_tunnel_ed25519 -N -L ${DOCKER_GATEWAY}:15432:127.0.0.1:5432 ubuntu@${GROUP_SSH_ADDRESS}
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now group17-db-tunnel
systemctl is-active group17-db-tunnel
ss -lnt | grep 15432
```

Expected: `active`, and a listener on the Docker gateway's `15432` port.
If the service fails, inspect `journalctl -u group17-db-tunnel -n 30
--no-pager`. Do not bind the tunnel to `0.0.0.0` or open PostgreSQL port
5432 in the AWS security group.

## 5. Start one optional consumer and measure it

The group VM maintainer supplies the database password through a secure
channel. On the teammate VM, create the ignored file
`~/group17-temperature-observability/.env.personal` with mode `0600`:

```bash
cd ~/group17-temperature-observability
install -m 600 /dev/null .env.personal
nano .env.personal
```

Paste the following four lines **inside nano**, replacing the password,
then save with Ctrl+O, Enter, and exit with Ctrl+X. These lines are file
contents, not terminal commands:

```text
POSTGRES_PASSWORD=<password-from-group-maintainer>
KAFKA_TOPIC=group17
KAFKA_GROUP_ID=group17-temperature-observability
KAFKA_CLIENT_ID=<ibrahim-or-yorick>-consumer
KAFKA_AUTH_DIR=/home/ubuntu/group17-auth
```

Check the tunnel from a container, then start the consumer:

```bash
cd ~/group17-temperature-observability
chmod 600 .env.personal
docker run --rm --add-host=host.docker.internal:host-gateway \
  postgres:16-alpine pg_isready -h host.docker.internal -p 15432 -U temperature
docker compose --env-file .env.personal \
  -f deploy/personal-consumer.compose.yaml up -d --build
docker compose --env-file .env.personal \
  -f deploy/personal-consumer.compose.yaml ps
docker compose --env-file .env.personal \
  -f deploy/personal-consumer.compose.yaml logs --tail 30
```

The consumer should log `consumer_started topic=group17` and process a small
development experiment without errors. It joins the same Kafka group as the
group VM consumers; Kafka divides topic partitions among the replicas. Do
not run an independent consumer group for this test, or events may be
processed twice by the application.

For a fair capacity comparison, keep the **total** consumer count constant
first. Example: two consumers on the group VM plus one on Ibrahim's VM and
one on Yorick's VM gives four total. On the group VM:

```bash
cd ~/group17-temperature-observability
docker compose --profile test --env-file .env -f deploy/compose.yaml \
  up -d --scale consumer=2 consumer
```

Repeat the exact same 100-experiment producer run and HTTP load from the
[four-VM workflow](four-vm-workflow.md). Compare notification p95 **and
maximum**, API throughput, error counts, and whether all measurements and
notifications completed. If the personal consumers do not help, stop each
with `docker compose --env-file .env.personal -f
deploy/personal-consumer.compose.yaml stop consumer`, then restore four
consumers on the group VM with `--scale consumer=4`.
