# Public Deployment Template

This sanitized example shows how to run the public app from a single Linux host using:

- `spx-collector.service` for data collection
- `spx-backtest-prod.service` for the public UI on `127.0.0.1:8789`
- Caddy on ports `80/443`
- the same local SQLite file for both collector and UI

## Assumed Server Paths

- Repo: `/opt/spx-data-collector`
- Venv: `/opt/spx-data-collector/.venv`
- DB file: `/opt/spx-data-collector/spx_options.db`

The `.env` file should use an absolute SQLite path:

```env
DB_URL=sqlite:////opt/spx-data-collector/spx_options.db
```

## 1. DNS

Point the DNS `A` record for `your-domain.example` at the server's static IP.

If you want `www`, point it too and let Caddy redirect it to the root domain.

## 2. Install Caddy

```bash
sudo apt-get update
sudo apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt-get update
sudo apt-get install -y caddy
```

## 3. Install service files

```bash
sudo cp deploy/systemd/spx-backtest-prod.service /etc/systemd/system/
sudo cp deploy/systemd/spx-sqlite-backup.service /etc/systemd/system/
sudo cp deploy/systemd/spx-sqlite-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now spx-backtest-prod.service
sudo systemctl enable --now spx-sqlite-backup.timer
```

## 4. Install Caddy config

```bash
sudo cp deploy/caddy/public-site.example.Caddyfile /etc/caddy/Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl enable --now caddy
sudo systemctl reload caddy
```

Before using the checked-in Caddy example, replace placeholder hostnames with your real domain. If DNS is not pointed yet, Caddy will keep retrying certificate issuance until the domain resolves to the instance.

## 5. Firewall

Open only:

- `22`
- `80`
- `443`

Do not open `8789` publicly. The prod app should stay on loopback only.

## 6. Functional checks

Local:

```bash
curl http://127.0.0.1:8789/api/health
curl -I http://127.0.0.1:8789/
```

Public:

```bash
curl -I http://your-domain.example
curl -I https://your-domain.example
```

## 7. Logs

```bash
journalctl -u spx-collector -n 100 --no-pager
journalctl -u spx-backtest-prod -n 100 --no-pager
journalctl -u caddy -n 100 --no-pager
journalctl -u spx-sqlite-backup -n 100 --no-pager
```
