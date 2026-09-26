# Agents Notes

## Instruction Sources Used In This Repo
- Runtime/system instructions from Codex environment.
- Workspace-level instructions provided via `AGENTS.md` context in this session.
- Direct user requirements in the active task thread.

## UI Tabs Not Loading: Root Cause + Prevention

### What failed
- `Options Analyzer` did not load because inline JS in `_HTML` failed to parse.
- Browser showed: `Uncaught SyntaxError: Invalid or unexpected token (index):503`.
- The syntax error prevented tab init/bind code from running at all.

### Why it happened
- The UI is embedded in Python as a triple-quoted string (`_HTML` in `backtest_prod.py`).
- JS escape sequences like `"\n"` inside that Python string can become literal newlines in served JS if not escaped correctly for this embedding context.
- Some modern JS constructs can also cause compatibility/parser issues depending on browser/runtime.

### Fixes applied
- Replaced brittle JS usages in the inline script:
  - Removed nullish-coalescing (`??`) in key paths.
  - Replaced `replaceAll(...)` with `split(...).join(...)`.
- Corrected escaped newlines in JS string literals:
  - Use `"\\n"` in the Python-embedded JS where literal `\n` is intended.

### Operational gotcha
- Hard refresh is not enough after editing `_HTML` in Python.
- You must restart the running backtest process after editing `_HTML` so the updated page is served.
- Restart the local `backtest_prod.py` process after editing `_HTML`.

### Quick troubleshooting checklist
1. Open browser console and check first syntax error line in `(index)`.
2. Map that line to `_HTML` in `src/spx_collector/backtest_prod.py`.
3. Check for Python-string escape interactions in inline JS (`\n`, `\t`, etc.).
4. If parse error exists, assume tab code never initialized; fix parse error first.
5. Restart backend process after each `_HTML` edit before re-testing.
6. Ignore `favicon.ico 404`; it is unrelated.

## Security Hardening Learnings
- Common beginner mistakes for a public analytics site (and the concrete fix for this repo):
  - Expose raw SQL endpoints or script evaluation hooks (`/api/query`).
    - Fix: remove the route entirely and keep only curated read endpoints.
  - Leak internals in health/diagnostic responses (DB paths, internal flags, secret-bearing state).
    - Fix: return only the minimum success marker (`{"ok": true}`) and keep internals on backend.
  - Trust the public environment and file permissions of `.env`.
    - Fix: enforce strict owner-only file mode at startup and fail fast when too-broad permissions are found.
  - Leave insecure tabs/features visible in production that suggest direct data access.
    - Fix: disable UI actions that imply query execution and return safe disabled messaging.
  - Let static documentation claim removed/unsupported paths.
    - Fix: keep architecture docs in sync so public/developer guidance does not advertise retired attack surfaces.
- Query endpoint shutdown is now mandatory before public rollout: remove `/api/query` from handler routing and delete its SQL helper paths to reduce raw SQL attack surface.
- Avoid exposing internal server paths in health responses. `GET /api/health` should return only health status by default (no DB path or internal flags).
- Enforce strict `.env` permissions before loading settings. Insecure `.env` permissions are a local secret-leak vector even before code-level validation.
- Keep token-bearing files out of web root and logs; validate file modes at startup and fail fast when permissions are too broad.

## Lightsail Access Shortcuts
- If the user says anything like:
  - "log me into my server"
  - "ssh me into my lightsail server"
  - "ssh me into my server"
  - or equivalent phrasing to open their server workspace
- Open VS Code Remote-SSH in a new window using the locally configured host alias and project path:

```bash
code --new-window --remote ssh-remote+your-host-alias /path/to/project
```

- If the user explicitly asks for terminal-only SSH (for example: "ssh in terminal only", "plain ssh session"), run:

```bash
ssh -i /path/to/private-key user@your-server-ip
```

## Local App
- Run `PYTHONPATH=src python -m spx_collector.backtest_prod --host 127.0.0.1 --port 8789` for local review.
- The app requires `.env` permissions of `600`; use `chmod 600 .env`.
- Point `DB_URL` at a local SQLite database. The production service uses the same app module on its own host and database.

## Cross-Project Rules
- Do not hardcode time zone offsets for civil/business time. Use UTC for storage and named IANA zones for conversion.
- Keep `AGENTS.md` focused on durable rules and workflow. Put longer topic notes in clearly named docs and reference them here.
- Time zone guidance and DST pitfalls: `docs/TIMEZONE_NOTES.md`
- Architecture overview: `docs/architecture.md`

## Deployment Workflow
- Treat the local repo as the source of truth and Lightsail as a deploy target only.
- Do not edit app code directly on the Lightsail server except during emergency debugging.
- Do not push app changes directly to `main` during normal work.
- Standard flow:
  1. Create a local feature branch from updated `main`.
  2. Make and review changes locally in `backtest_prod.py` against a local database.
  3. Commit the branch, push it to GitHub, open a PR, and merge into `main`.
  4. On Lightsail, `git checkout main && git pull --ff-only origin main`, then restart the web service.
- Keep the server repo on `main` after deployment.
- Emergency direct pushes to `main` are allowed only when explicitly intended, and should be treated as exceptions.
