# Install on Oracle Cloud Ampere A1 (Always Free) with Let's Encrypt

This guide sets up wt-tracker-rust as a public `wss://` WebTorrent tracker on an Oracle Cloud
**Always Free** Ampere A1 instance (2 OCPU, 12 GB, aarch64), with a Let's Encrypt certificate
from certbot that renews without restarting the tracker. The same setup runs
`wss://tracker.novage.com.ua` (~45k peers at ~15% CPU).

Replace `tracker.example.com` and `admin@example.com` with your domain and email.

What you get:

- `wss://tracker.example.com` on port 443 (TLS, certificate reloaded on renewal: connections stay).
- Optional: `ws://` and `http://` on port 80 (redirected to the tracker's plain listener).
- Optional: Prometheus `/metrics` and `/swarms` on `127.0.0.1:9100`.

## 1. Create the instance

1. Compute → Instances → Create instance: image **Ubuntu** (24.04 or newer), shape
   **VM.Standard.A1.Flex** with **2 OCPU and 12 GB** (the Always Free allowance), a public IPv4
   address, your SSH public key.
2. Point a DNS `A` record for `tracker.example.com` to the instance's public IP.
3. Connect: `ssh -i ~/.ssh/<your key> ubuntu@tracker.example.com`.

## 2. Open ports 80 and 443 in Oracle's network

On Oracle Cloud a port is reachable only when **both** the subnet's security list and the
instance's own firewall (section 3) allow it. Ports 80 and 443 are closed in both by default.
(Based on [Opening up port 80 and 443 for Oracle Cloud servers](https://dev.to/armiedema/opening-up-port-80-and-443-for-oracle-cloud-servers-j35),
plus the stateless rules a tracker needs.)

In the console, open the instance → its **Virtual cloud network** (or Networking → Virtual
cloud networks → your VCN) → **Security** / **Security lists** → the subnet's default security
list.

**Ingress rules** (Add ingress rules), one per port, and tick **Stateless**:

| Stateless | Source CIDR | IP protocol | Source port | Destination port |
|---|---|---|---|---|
| Yes | 0.0.0.0/0 | TCP | All | 443 |
| Yes | 0.0.0.0/0 | TCP | All | 80 |

**Egress rules** (Add egress rules), needed because stateless traffic has no automatic return
path:

| Stateless | Destination CIDR | IP protocol | Source port | Destination port | Description |
|---|---|---|---|---|---|
| Yes | 0.0.0.0/0 | TCP | 443 | All | wss replies |
| Yes | 0.0.0.0/0 | TCP | 80 | All | HTTP and certbot replies |

Keep the default rules as they are: ingress 22 (SSH) and ICMP, and the stateful egress "All
protocols" rule (the server's own connections: apt, Let's Encrypt, git).

> **Why stateless:** a stateful rule makes Oracle track every connection in a table per
> instance. Tens of thousands of long-lived WebSocket connections fill it (we saw it at about
> 40k peers), and then Oracle silently drops **new** connections, SSH included, before they reach
> the instance. The server looks hung while it is healthy and its connected peers keep working.
> Stateless rules have no such table.

## 3. Open ports 80 and 443 in the instance firewall

Oracle's Ubuntu images ship an iptables `INPUT` chain that accepts SSH and rejects everything
else with a final `REJECT` rule; `ufw` is not used. Rules added *after* the `REJECT` never
match, so insert the new ones before it:

```bash
sudo iptables -L INPUT --line-numbers   # find the "REJECT ... icmp-host-prohibited" line
```

On a fresh image the `REJECT` rule is number 5 (after ESTABLISHED, icmp, lo and SSH):

```bash
sudo iptables -I INPUT 5 -p tcp -m state --state NEW -m tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -p tcp -m state --state NEW -m tcp --dport 443 -j ACCEPT
sudo iptables -S INPUT                  # 80 and 443 must come before "-A INPUT -j REJECT"
sudo netfilter-persistent save          # keep them after a reboot (/etc/iptables/rules.v4)
```

If iptables answers `Couldn't load match 'state'` (an nftables-based build), use the
equivalent `-m conntrack --ctstate NEW` instead of `-m state --state NEW`.

Check from your own machine (or any online port scanner) once something listens on the ports;
a closed port times out, an open one connects or is refused:

```bash
nc -vz -w 5 tracker.example.com 443
```

## 4. Base packages and certbot

```bash
sudo apt update
sudo apt dist-upgrade -y
sudo apt install -y certbot build-essential git
```

The apt package installs `certbot.timer`, which tries to renew twice a day (a certificate is
renewed within 30 days of expiry): `systemctl list-timers certbot.timer`.

## 5. Get the certificate

certbot's standalone mode answers the HTTP-01 challenge on port 80 itself, so nothing else may
listen on port 80 yet:

```bash
sudo certbot certonly --standalone -d tracker.example.com \
  --key-type ecdsa --agree-tos -m admin@example.com --no-eff-email
```

The files are in `/etc/letsencrypt/live/tracker.example.com/` (`fullchain.pem`, `privkey.pem`).

## 6. Build

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
git clone https://github.com/Novage/wt-tracker-rust.git ~/wt-tracker-rust
cd ~/wt-tracker-rust && ~/.cargo/bin/cargo build --release -p wt-server   # ~2 min on A1
```

The repository pins its Rust version (`rust-toolchain.toml`); rustup installs it on the first
build.

## 7. Install

### Binary and the certificate copy

The service runs as a dynamic, unprivileged user. It reads its own copy of the certificate,
readable by a dedicated group, which the renewal hook (section 8) refreshes:

```bash
sudo install -D -m 755 ~/wt-tracker-rust/target/release/wt-tracker /opt/wt-tracker-rust/wt-tracker
sudo groupadd --system wt-tracker-tls
sudo install -d -m 750 -g wt-tracker-tls /etc/wt-tracker-rust/tls
```

### Configuration: `/etc/wt-tracker-rust/config.json`

```bash
sudo tee /etc/wt-tracker-rust/config.json > /dev/null <<'EOF'
{
  "servers": [
    {
      "server": {
        "host": "0.0.0.0",
        "port": 443,
        "cert_file_name": "/etc/wt-tracker-rust/tls/fullchain.pem",
        "key_file_name": "/etc/wt-tracker-rust/tls/privkey.pem"
      },
      "websockets": {
        "path": "/*",
        "maxPayloadLength": 65536,
        "idleTimeout": 190,
        "compression": 1,
        "compressOutgoingMinSize": 256
      }
    }
  ],
  "tracker": { "maxOffers": 10, "announceInterval": 180 },
  "workers": 2,
  "reusePort": true,
  "shutdownTimeout": 5,
  "metrics": { "host": "127.0.0.1", "port": 9100 }
}
EOF
```

- `compressOutgoingMinSize: 256`, `maxOffers: 10`, `announceInterval: 180`: less outbound traffic
  per peer. The Always Free tier includes **10 TB of egress per month** (about 3.86 MB/s on
  average), which is the real limit: about 100k peers at ~38 B/s each.
- `metrics`: Prometheus `/metrics` and `/swarms` on localhost only. Remove it if not needed.
- Every setting: [README](../README.md#configuration) and [spec §13.1](SPEC.md#131-configuration-js-format).

### systemd unit: `/etc/systemd/system/wt-tracker-rust.service`

```bash
sudo tee /etc/systemd/system/wt-tracker-rust.service > /dev/null <<'EOF'
[Unit]
Description=wt-tracker-rust (WebTorrent tracker)
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/opt/wt-tracker-rust/wt-tracker /etc/wt-tracker-rust/config.json
# Reloads the TLS certificate; connections stay open.
ExecReload=/bin/kill -HUP $MAINPID
WorkingDirectory=/
DynamicUser=yes
SupplementaryGroups=wt-tracker-tls
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
LimitNOFILE=1048576
Restart=on-failure
RestartSec=5
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
EOF
sudo systemd-analyze verify /etc/systemd/system/wt-tracker-rust.service
```

## 8. Renewal without a restart

certbot runs deploy hooks after every successful renewal of **any** certificate on the host
(`$RENEWED_LINEAGE` is the renewed certificate's `live/` directory). This one ignores the other
certificates (copying another domain's certificate would make every wss client fail the
hostname check), copies the new files atomically and asks the tracker to reload them (SIGHUP through `ExecReload`); the tracker also notices changed files by itself within
`tlsReloadInterval` (60 s). A key that does not match its certificate is rejected and the old
certificate kept.

```bash
sudo tee /etc/letsencrypt/renewal-hooks/deploy/wt-tracker-rust.sh > /dev/null <<'EOF'
#!/bin/sh
# certbot deploy hook: copy the renewed certificate for wt-tracker-rust and reload it (no
# restart: connections stay open).
set -e
lineage=/etc/letsencrypt/live/tracker.example.com
# Deploy hooks run for every renewed certificate on the host: only the tracker's.
[ "${RENEWED_LINEAGE:-$lineage}" = "$lineage" ] || exit 0
src=$lineage
dst=/etc/wt-tracker-rust/tls
install -m 640 -g wt-tracker-tls "$src/privkey.pem" "$dst/.privkey.pem.new"
install -m 644 "$src/fullchain.pem" "$dst/.fullchain.pem.new"
mv "$dst/.privkey.pem.new" "$dst/privkey.pem"
mv "$dst/.fullchain.pem.new" "$dst/fullchain.pem"
systemctl try-reload-or-restart wt-tracker-rust.service
EOF
sudo chmod 755 /etc/letsencrypt/renewal-hooks/deploy/wt-tracker-rust.sh
```

Copy the current certificate once and start the tracker:

```bash
sudo /etc/letsencrypt/renewal-hooks/deploy/wt-tracker-rust.sh
sudo systemctl daemon-reload
sudo systemctl enable --now wt-tracker-rust.service
```

Test the renewal path (a dry run skips deploy hooks; a forced renewal runs them, and Let's
Encrypt allows only a few renewals per week for the same names):

```bash
sudo certbot renew --dry-run
sudo certbot renew --force-renewal
sudo journalctl -u wt-tracker-rust -n 5 -o cat   # event=tls_reloaded ... trigger=signal
```

## 9. Verify

```bash
systemctl is-active wt-tracker-rust.service
sudo journalctl -u wt-tracker-rust -n 10 -o cat   # event=listening, event=started
curl -s https://tracker.example.com/stats.json
curl -s http://127.0.0.1:9100/metrics | grep -E "^wt_(listener_connections|tls_)"
curl -s "http://127.0.0.1:9100/swarms?top=5"
```

From your machine:

```bash
echo | openssl s_client -connect tracker.example.com:443 -servername tracker.example.com 2>/dev/null \
  | openssl x509 -noout -subject -issuer -enddate
```

Then use `wss://tracker.example.com` as a tracker in P2P Media Loader.

## 10. Optional: plain `ws://` and HTTP on port 80

The tracker can also listen on a plain port (8080 here); iptables redirects port 80 to it. Port
80 must be free while certbot answers a challenge, so two hooks turn the redirect off and on
around each renewal.

1. Add a second listener to `config.json` (same `websockets` settings as on 443):

   ```json
   { "server": { "host": "0.0.0.0", "port": 8080 }, "websockets": { "path": "/*", "maxPayloadLength": 65536, "idleTimeout": 190, "compression": 1, "compressOutgoingMinSize": 256 } }
   ```

2. Redirect 80 to 8080 and accept 8080 only for the redirected traffic, then restart the tracker:

   ```bash
   sudo iptables -I INPUT 7 -p tcp --dport 8080 -m conntrack --ctstate DNAT -j ACCEPT
   sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080
   sudo netfilter-persistent save
   sudo systemctl restart wt-tracker-rust.service
   ```

3. certbot hooks:

   ```bash
   sudo tee /etc/letsencrypt/renewal-hooks/pre/80-to-8080-off.sh > /dev/null <<'EOF'
   #!/bin/sh
   # Free port 80 for the HTTP-01 challenge.
   iptables -t nat -D PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080 2>/dev/null || true
   EOF
   sudo tee /etc/letsencrypt/renewal-hooks/post/80-to-8080-on.sh > /dev/null <<'EOF'
   #!/bin/sh
   # Redirect port 80 to the tracker again.
   iptables -t nat -C PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080 2>/dev/null \
     || iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 8080
   EOF
   sudo chmod 755 /etc/letsencrypt/renewal-hooks/pre/*.sh /etc/letsencrypt/renewal-hooks/post/*.sh
   sudo certbot renew --dry-run && sudo iptables -t nat -S PREROUTING   # the redirect is back
   ```

An `index.html` for `/` can be set with `"indexHtml": "/etc/wt-tracker-rust/index.html"`.

## 11. Update and roll back

```bash
cd ~/wt-tracker-rust && git pull && ~/.cargo/bin/cargo build --release -p wt-server
sudo cp /opt/wt-tracker-rust/wt-tracker /opt/wt-tracker-rust/wt-tracker.prev
sudo install -m 755 target/release/wt-tracker /opt/wt-tracker-rust/wt-tracker
sudo systemctl restart wt-tracker-rust.service
```

A restart closes every connection with 1001 (Going Away) and takes a few seconds; clients
reconnect. Configuration changes also need a restart (certificates only a reload). To roll
back, copy `wt-tracker.prev` back and restart.

## 12. Troubleshooting

- **New connections and SSH time out, existing peers keep working:** Oracle's connection
  tracking is full; make the 80 / 443 rules stateless (section 2). Reboot from the console if
  SSH is unreachable (do not terminate the instance).
- **A port is closed from outside:** check both the security list (section 2) and
  `sudo iptables -S INPUT` (section 3: before the `REJECT` rule).
- **Renewal failed:** `sudo tail -50 /var/log/letsencrypt/letsencrypt.log`. With section 10, the
  redirect must be off during the challenge and back afterwards.
- **Certificate not reloaded:** `journalctl -u wt-tracker-rust | grep tls_` shows
  `tls_reloaded` or `tls_reload_failed` with the reason; `wt_tls_certificate_expiry_seconds` in
  `/metrics` is the served certificate's expiry.
- **Logs:** `journalctl -u wt-tracker-rust`. journald rotates by itself (at most 10% of the disk,
  4 GiB). Keep `logLevel` at `info` in production: `debug` logs every connection close.
- **Outbound traffic:** watch it in the Oracle console (instance → Metrics) or with
  `wt_socket_bytes_total{direction="out"}`; the Always Free tier includes 10 TB a month.
