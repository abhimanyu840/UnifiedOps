# UnifiedOps v2 AI Coding Assistant Guidelines

## System Context (Windows)
- **OS**: Windows 11, Terminal: PowerShell (pwsh) only
- **Username**: abhim
- **Python command**: Use `python` not `python3`
- **Paths**: Always use backslashes in file paths
- **Never use Unix commands** — PowerShell equivalents only:
  - `New-Item` not `touch`
  - `Get-ChildItem` not `ls`
  - `Get-Content` not `cat`
  - `Remove-Item` not `rm`
  - `Copy-Item` not `cp`
  - `Move-Item` not `mv`
- **Virtual environments**: Always use `.venv`
  - Create:   `python -m venv .venv`
  - Activate: `.venv\Scripts\Activate.ps1`
  - Install:  `.venv\Scripts\pip.exe install ...`

## Language and Framework Preferences
- **Backend**: Python 3.9+, FastAPI, Uvicorn, HTTPX, WebSockets
- **Database**: InfluxDB v2 using `influxdb-client`
- **Frontend**: React 19, TypeScript, Vite, Zustand, `@tanstack/react-table`
- **Deployment target**: RHEL 9.4, airgapped (no internet on target servers)

## Folder Structure Rules
- `server/`   — FastAPI routers, services, server initialization logic
- `listener/` — Location-specific syslog listener scripts
  (`syslog_trap_listener_cdvl.py`, `_bcp.py`, `_sify.py`)
- `frontend/` — React components, Vite config, frontend assets
- `scripts/`  — Deployment, build, and utility PowerShell scripts
- `deploy/`   — Deployment instructions, TLS setup, InfluxDB setup
- `private/`  — Credentials, tokens, certs — NEVER committed to git
- `docs/`     — Vendor PDFs and manuals for RAG indexing
  (Hitachi VSP, NetApp ONTAP, Brocade FOS, Dell PowerStore)

When creating new files always ask: which folder does this belong in?
Never create files outside the project root.

## Coding Style Rules

### Python
- Follow PEP 8 strictly
- Always use `from __future__ import annotations` at top of every file
- Always use explicit typing from `typing`:
  `Dict`, `List`, `Optional`, `Tuple`, `Any` — never `dict`, `list` etc.
- Use `asyncio` for all FastAPI routes and WebSocket handlers
- Use `ThreadPoolExecutor` for all blocking InfluxDB calls
- Use `WsHub` class pattern for all WebSocket management

### React / TypeScript
- Modern React 19 functional components only — no class components
- Strict TypeScript types on all props, state, and function signatures
- No `any` type unless absolutely unavoidable
- Use Zustand stores for all shared/global state
- Use `@tanstack/react-table` for all data grids and tables
- Tailwind CSS only — no inline styles, no CSS modules

### Listeners
- Keep listener scripts flat and self-contained per location
- No imports that cross-contaminate `_cdvl`, `_bcp`, `_sify` logic
- Listeners must be readable as standalone scripts

## MCP Servers Available
These MCP servers are registered and active in Antigravity:

| Server | Trigger | Use for |
|---|---|---|
| ollama | @qwen @gemma @vision @screenshot | Local models, private work |
| nvidia-llm-router | @route | Auto-pick best NIM model |
| nvidia-aiq | @aiq | Deep research, vendor doc questions |
| nvidia-rag | @rag | Search indexed vendor PDFs in docs\ |

## NVIDIA NIM Usage Rules
- NVIDIA NIM is cloud infrastructure — same privacy rules as Antigravity
- Never send `private\` files, credentials, IPs, or `.env` to NIM
- Never send SAN configs, LUN mappings, or array credentials to NIM
- Use @route when unsure which NIM model to use — it auto-selects

## RAG / Vendor PDF Workflow
- Put all vendor PDFs in `docs\` folder
- Index once: `python nvidia_rag_mcp.py --index`
- Then use `@rag` for any vendor doc question in Antigravity chat
- For one-off large PDF: use nvidia-context (deepseek-v4-flash, 1M tokens)
- For repeated queries: always use @rag (faster, cited, no credit cost)

## What NOT to Auto-Run
- **Tests**: Do NOT run unit or integration tests automatically
- **Deployments**: Do NOT run installation scripts or systemd units
  without explicit permission
- **Database**: Do NOT run InfluxDB setup or any data operations
- **Packages**: Do NOT install new packages without asking — airgapped
  system requires offline RPM/wheel bundling
- **Bulk edits**: Always show a diff or file list before editing
  multiple files at once
- **Outside root**: Never create or modify files outside project root

## Git Commit Conventions
- `feat:`     — new features     (`feat: add BCP listener support`)
- `fix:`      — bug fixes        (`fix: resolve websocket connection drop`)
- `refactor:` — restructuring    (`refactor: extract influx pool to service`)
- `docs:`     — documentation    (`docs: update INSTALL.md with TLS matrix`)
- `chore:`    — maintenance      (`chore: update .gitignore for private dir`)

**Always commit after completing every task.**
**Never commit files from `private\` directory.**
**Verify `private\` and `.env` are in `.gitignore` before first commit.**