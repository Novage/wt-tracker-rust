# Monitoring wt-tracker-rust

The tracker exports everything needed for monitoring on its own `/metrics` endpoint: peers,
connections, closes and rejections by reason, traffic, memory, CPU per worker thread, file
descriptors, kernel listen-queue overflows and certificate expiry (spec §13.7). No agent or
exporter is needed. This folder has:

- `grafana-dashboard.json`: a Grafana dashboard built on `/metrics` alone;
- `alerts.yaml`: alert rules (Prometheus format) for the problems worth waking up for.

## 1. Enable the metrics listener

In `config.json`, with HTTPS and a password when the endpoint is reachable from the internet
(Grafana Cloud scrapes from its own servers and requires an HTTPS URL):

```json
"metrics": {
  "host": "0.0.0.0", "port": 9000,
  "cert_file_name": "/etc/wt-tracker-rust/tls/fullchain.pem",
  "key_file_name": "/etc/wt-tracker-rust/tls/privkey.pem",
  "username": "grafana", "password": "a-long-random-password"
}
```

- The certificate can be the one of the wss:// listener; `systemctl reload` (SIGHUP) reloads
  both after a renewal.
- The config file now holds a password: make it readable by the service only
  (`chmod 640`, group of the service).
- Open the port in the firewall (and in the cloud provider's security rules).
- Without `cert_file_name` / `key_file_name` the listener is plain HTTP; without `username` /
  `password` it has no password. For a scraper on the same machine, `"host": "127.0.0.1"` and
  plain HTTP are fine.

Check it:

```bash
curl -s -u grafana:a-long-random-password https://tracker.example.com:9000/metrics | head
```

`/stats.json` and `/swarms` (the largest swarms) are served there too; a browser asks for the
password once.

## 2. Grafana Cloud (free tier)

The free tier (no credit card) keeps metrics for 14 days and includes alerting; the tracker
exports about 100 series of its 10k limit.

1. **Scrape job:** Connections → Add new connection → *Metrics Endpoint*. URL
   `https://tracker.example.com:9000/metrics`, basic auth with the username and password above,
   scrape job name **`wt-tracker`** (the alert rules use `job="wt-tracker"`), interval 1 minute
   (enough for these numbers and the cheapest; the dashboard adapts its rate windows to the
   interval, and the alert rules work from 1 minute down).
   Test the connection, then save.
2. **Dashboard:** Dashboards → New → Import → upload `grafana-dashboard.json`, choose your
   stack's Prometheus data source.
3. **Alerts:** create the rules of `alerts.yaml` under Alerting → Alert rules (each rule's
   `expr` is the query and already holds the threshold; `for` is the pending period), or
   load the file into your stack's ruler with
   `mimirtool rules load --address=<your stack's Prometheus URL> --id=<instance id> --key=<token> alerts.yaml`.
4. **Notifications:** Alerting → Contact points (email is preset with your account address;
   Telegram, Slack and others are available), and route the alerts to it.

## 3. Your own Prometheus

```yaml
scrape_configs:
  - job_name: wt-tracker
    scheme: https            # http without cert_file_name
    basic_auth: { username: grafana, password: a-long-random-password }
    static_configs: [{ targets: ["tracker.example.com:9000"] }]
rule_files: [alerts.yaml]
```

Then import the dashboard into your Grafana as above.

## What the alerts mean

| Alert | Fires when | First look |
|---|---|---|
| TrackerDown | no `/metrics` for 5 minutes | `systemctl status wt-tracker-rust`, the journal |
| WorkerNotAnswering | a worker misses the 1 s scrape deadline for 2 minutes | "CPU per worker": a worker near 100% is stuck |
| WorkerCpuBusy | a worker thread above 90% of a core over 3 minutes | load (peers) or a stuck worker |
| TrackerRestarted | the process started within 10 minutes | deploy, crash or out-of-memory kill: the journal |
| MemoryHigh | resident memory above 3 GB for 5 minutes | about 15 KB per peer is normal (1.5 GB at 100k peers); adjust to your server |
| FileDescriptorsNearLimit | over 80% of the descriptor limit | raise `LimitNOFILE` |
| ListenOverflows | the kernel drops more than 10 new connections a minute | a stuck worker, or a CPU-bound server |
| EgressPace | 24 h average egress above 3.5 MB/s | 10 TB a month (e.g. Oracle Cloud Always Free) is 3.86 MB/s: lower `maxOffers` or raise `announceInterval` |
| CertificateExpiring | a served certificate expires in under 14 days | the renewal and its reload hook |

Thresholds are starting points for a 2-core server with up to ~100k peers; adjust them to yours.
