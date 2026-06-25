# UnifiedOps v2 AI Coding Assistant Guidelines

## System Context (Windows)
- **OS**: Windows 11, Terminal: PowerShell (pwsh) only
- **Username**: abhim
- **Python command**: `python` not `python3`
- **Paths**: Always use backslashes
- **Never use Unix commands**:
  - `New-Item` not `touch`
  - `Get-ChildItem` not `ls`
  - `Get-Content` not `cat`
  - `Remove-Item` not `rm`
  - `Copy-Item` not `cp`
  - `Move-Item` not `mv`
- **Virtual environments**: Always `.venv`
  - Create: `python -m venv .venv`
  - Activate: `.venv\Scripts\Activate.ps1`

## Language and Framework
- **Backend**: Python 3.9+, FastAPI, Uvicorn, HTTPX, WebSockets
- **Database**: InfluxDB v2, `influxdb-client`
- **Frontend**: React 19, TypeScript, Vite, Zustand, `@tanstack/react-table`
- **Target**: RHEL 9.4, airgapped

## Folder Structure
- `server/`   — FastAPI routers, services, server init
- `listener/` — Syslog listeners (`_cdvl.py`, `_bcp.py`, `_sify.py`)
- `frontend/` — React components, Vite config, assets
- `scripts/`  — PowerShell deployment and build scripts
- `deploy/`   — RHEL deployment instructions, TLS, InfluxDB setup
- `private/`  — Credentials, tokens, certs — NEVER committed
- `docs/`     — Vendor PDFs for RAG (Hitachi, NetApp, Brocade, Dell)

## Coding Style

### Python
- PEP 8, `from __future__ import annotations` every file
- Explicit typing: `Dict`, `List`, `Optional`, `Tuple`, `Any`
- `asyncio` for FastAPI/WebSocket, `ThreadPoolExecutor` for InfluxDB
- `WsHub` pattern for all WebSocket management

### React / TypeScript
- React 19 functional components, strict TypeScript
- No `any`, Zustand for state, `@tanstack/react-table` for grids
- Tailwind CSS only

### Listeners
- Self-contained per location, no cross-location imports

## MCP Servers Active

| Server | Trigger | Purpose | Auto-runs? |
|---|---|---|---|
| ollama | @qwen @gemma @vision @screenshot | Local models | No |
| nvidia-llm-router | @route | Auto model selection | No |
| nvidia-aiq | @aiq | Deep research agent | No |
| nvidia-rag | @rag | Vendor PDF search | No |
| nvidia-review | @review | Senior code review | NO — on-demand only |
| nvidia-docgen | @docgen | Doc generation | NO — on-demand only |

## ON-DEMAND ONLY — Critical Rules

### @review trigger
NEVER call automatically. Only when user explicitly requests a review.

Usage patterns:
- `@review [paste code here]` — standard code review
- `@review architecture` — full architecture audit
- `@review security [paste code]` — security audit
- `@review performance [paste function]` — perf review

Uses: Nemotron Super 120B as senior engineer persona
Covers: bugs, security, performance, readability, best practices

### @docgen trigger
NEVER call automatically. Only when user explicitly requests docs.

Usage patterns:
- `@docgen readme` — generate full README.md from codebase
- `@docgen api [paste routes]` — generate API documentation
- `@docgen runbook [paste deploy script]` — RHEL runbook
- `@docgen docstring [paste function]` — add Python docstrings
- `@docgen changelog [paste git log]` — generate CHANGELOG.md
- `@docgen adr [describe decision]` — Architecture Decision Record
- `@docgen component [paste React component]` — component docs

Uses: Kimi K2.6 (256K context, multimodal, long-horizon)

## New Models Available (kimi-k2.6 and minimax-m3)

### nvidia-kimi-new (moonshotai/kimi-k2.6)
- 1T MoE, 32B active, 256K context
- Supports 300 sequential tool calls — best for long agentic workflows
- Multimodal: text, image, video input
- Use for: complex multi-step tasks, doc generation, long-horizon coding

### nvidia-m3 (minimaxai/minimax-m3)
- 428B MoE, 1M context, native multimodal
- Long-horizon coding: 8+ hours autonomous sessions
- Use for: very long coding sessions, entire feature builds

## NVIDIA NIM Rules
- NIM is cloud — same privacy rules as Antigravity
- Never send `private\` files, credentials, IPs, `.env` to NIM
- Never send SAN configs or array credentials to NIM
- @review and @docgen are NIM-powered — never pass private files

## RAG Workflow
- Put vendor PDFs in `docs\` folder
- Index once: `python nvidia_rag_mcp.py --index`
- Use @rag for any vendor doc question
- For one-off large PDF: nvidia-context (1M tokens)

## What NOT to Auto-Run
- No tests, migrations, deploys without asking
- No new packages without asking (airgapped — needs offline bundling)
- No files outside project root
- No @review or @docgen without explicit user request
- Always show diff before bulk edits

## Git Conventions
- `feat:` `fix:` `refactor:` `docs:` `chore:`
- Commit after every completed task
- Never commit `private\` or `.env`