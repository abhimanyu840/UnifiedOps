# UnifiedOps v2 Agents Configuration

## Project Overview
UnifiedOps v2 (Hi-Track Alert v1.4.1) is an airgapped storage automation
and monitoring solution designed to run across a 4-server topology on
RHEL 9.4. The system ingests syslog UDP packets from storage arrays
across three pipeline VMs (CDVL, BCP, SIFY) using Python listener
scripts, and writes telemetry data to a local InfluxDB instance on each
node. A centralized UI VM hosts a Python FastAPI backend that proxies
requests to the pipeline VMs and serves a React/Vite frontend dashboard,
providing operators with real-time alerts pushed via WebSockets.

## CRITICAL — Model Priority Ladder (Credit Conservation)
Claude Sonnet/Opus quota drains fast with weekly hard caps. Opus costs
~8x Sonnet for the same task. ALWAYS try lower tiers first:

```
1. Local Ollama       (free, unlimited)  -> @qwen, @gemma, @vision, @screenshot
2. NVIDIA NIM          (free, generous)   -> nvidia-coder, nvidia-reason, etc.
3. Ollama Cloud        (free, 2nd cloud)  -> ollama-cloud-coder, ollama-cloud-reason
4. Free linters        (zero AI cost)     -> eslint MCP, python-analyzer MCP
5. Claude Sonnet 4.6   (LAST RESORT)      -> UI polish/design taste only
6. Claude Opus 4.6     (EMERGENCY ONLY)   -> genuine architecture calls only
```

Before any Claude call, confirm: "Did tiers 1-4 fail to produce
acceptable quality?" If unsure, try tier 1-3 first.

## Model Selection Rules

### Tier 1 — Local Models (Privacy-Safe / Offline / Unlimited / FREE)
- **Default coding**: `qwen3-coder:30b` via @qwen — backend, APIs,
  FastAPI, InfluxDB queries, WebSocket logic, listener scripts
- **Debugging / reasoning**: `deepseek-r1:14b` via @gemma — bug
  tracing, architecture decisions, data flow analysis
- **Fast edits**: `qwen2.5-coder:1.5b` — single line fixes, renaming
- **Quick screenshots**: `moondream2` via @vision — UI screenshots
- **Deep image analysis**: `gemma3:4b` via @screenshot — diagrams,
  SAN/network topology images

### Tier 2 — NVIDIA NIM (Free Tier, Generous Limits, Cloud)
Never send private files or credentials — it is cloud infrastructure.

**Coding and Agentic**
- nvidia-coder (qwen3-coder:480b) — frontier coding, try BEFORE Claude
- nvidia-agent (mistral/mistral-nemotron) — function calling, agents
- nvidia-gpt (gpt-oss-120b) — general coding, credit-efficient
- nvidia-glm (zhipuai/glm-5.1) — agentic coding, SOTA tool calling
- nvidia-qwen235 (qwen/qwen3-235b-a22b) — strong UI/reasoning blend,
  try BEFORE Claude Sonnet for component structure decisions

**Reasoning**
- nvidia-reason (deepseek-r1:671b) — full R1, try BEFORE Claude Opus
- nvidia-kimi-new (moonshotai/kimi-k2.6) — 256K context, 300
  sequential tool calls, multimodal, long-horizon agentic
- nvidia-qwq (qwen/qwq-32b) — fast chain-of-thought debugging
- nvidia-nemotron (nvidia/nemotron-3-super-120b-a12b) — 1M context
- nvidia-step (stepfun-ai/step-3.5-flash) — agentic planning

**General and Fast**
- nvidia-fast (minimax-m2.7) — try BEFORE Gemini Flash if rate limited
- nvidia-m3 (minimaxai/minimax-m3) — 1M context, 8+ hour coding sessions
- nvidia-llama (meta/llama-3.3-70b-instruct) — cheap reliable fallback
- nvidia-phi (microsoft/phi-4-mini-flash-reasoning) — quick lookups

**Long Context**
- nvidia-context (deepseek-v4-flash) — 1M context, large files, PDFs

**ON-DEMAND ONLY — never auto-run**
- nvidia-review (nemotron-3-super-120b) via @review — senior review
- nvidia-docgen (kimi-k2.6) via @docgen — documentation generator
- nvidia-starcoder (starcoder2-7b) — code-specific doc support

**Vision and Document Parsing**
- nvidia-vision (gemma4:e4b), nvidia-parse, nvidia-docparse

**Embeddings and RAG**
- nvidia-embed, nvidia-embedcode, nvidia-rerank

**Safety**
- nvidia-safety (nemotron-content-safety)

### Tier 3 — Ollama Cloud (Free Tier, Second Cloud Option)
Use when NIM is rate-limited, or to cross-check NIM results.
- **ollama-cloud-coder** (qwen3-coder:480b-cloud) — same model family
  as nvidia-coder, different infra. Use if NIM coding models are slow
  or rate-limited.
- **ollama-cloud-reason** (nemotron-3-ultra:cloud) — heavy reasoning,
  alternative to nvidia-reason when NIM is busy.
- Note: Ollama free cloud quota is limited — monitor usage, prefer
  level 1-2 models on Ollama cloud per Ollama's own rating system.

### Tier 4 — Free Skills (Zero AI Cost — Use First for These Tasks)
These run with NO model cost at all. Always run before asking any
AI model to review code style/quality.

| Skill | MCP Server | Trigger | Covers |
|---|---|---|---|
| ESLint | `eslint` | "lint this file" | JS/TS style, errors, framework rules |
| Ruff + Vulture | `python-analyzer` | "analyze this Python file" | Python lint, dead code, style |

Workflow: run free linter FIRST → fix mechanical issues yourself or
with @qwen → only escalate to @review (AI) for logic/architecture
issues the linter can't catch.

### Tier 5 — Antigravity Cloud Claude (LAST RESORT)
- **UI / Frontend**: Claude Sonnet 4.6 Thinking — ONLY after
  nvidia-coder and nvidia-qwen235 fail to produce acceptable quality
- **Complex architecture**: Claude Opus 4.6 Thinking — ONLY after
  nvidia-reason and nvidia-nemotron fail. Costs ~8x Sonnet.
- **Everyday tasks**: Gemini 3.5 Flash (High) — true default, separate
  quota pool from Claude, far less quota-intensive
- **Browser / deploy**: Gemini 3.1 Pro (High)

### NVIDIA Skills / MCP Servers

| Server | Trigger | Use for | Auto-runs? |
|---|---|---|---|
| ollama | @qwen @gemma @vision @screenshot | Local models | No |
| nvidia-llm-router | @route | Auto-pick best NIM model | No |
| nvidia-aiq | @aiq | Deep research, vendor docs | No |
| nvidia-rag | @rag | Search indexed vendor PDFs | No |
| nvidia-review | @review | Senior code review | NO — on-demand only |
| nvidia-docgen | @docgen | Documentation generator | NO — on-demand only |
| eslint | (auto via lint request) | JS/TS linting | Free, no AI cost |
| python-analyzer | (auto via lint request) | Python lint/dead code | Free, no AI cost |

## Model Transparency Rule
Always state which model answered: `[Model: <name>]`
If Claude was used, state why lower tiers were insufficient.

## Security Rules
- **NEVER use ANY cloud (Antigravity, NIM, or Ollama Cloud) for**:
  - Files inside `private\` directory
  - InfluxDB tokens, TLS certificates, API keys, `.env` files
  - Deployment scripts with server IPs or credentials
  - RHEL usernames, passwords, SSH keys
  - FC/SAN zone configs, LUN mappings, RAID group data
  - SNMP strings or array management credentials
- **Ollama Cloud is cloud infrastructure** — same rules as NIM/Antigravity
- **@review and @docgen are cloud** — never pass private files to them

## Project-Specific Agent Rules

### Backend (Python)
- Python 3.9 only, `from __future__ import annotations` every file
- Explicit typing: `Dict`, `List`, `Optional`, `Tuple` from `typing`
- `ThreadPoolExecutor` for InfluxDB, `WsHub` for WebSocket
- Run `python-analyzer` (Ruff) before any AI review of Python changes
- Never add dependencies without asking — airgapped system

### Frontend (React)
- React 19 functional components, TypeScript only
- Vite, Zustand, `@tanstack/react-table`, Tailwind CSS only
- Run `eslint` before any AI review of JS/TS changes
- UI priority: nvidia-coder → nvidia-qwen235 → Claude Sonnet (last resort)

### Syslog Listeners
- Self-contained per location: `_cdvl`, `_bcp`, `_sify` isolated
- @qwen only — never escalate listener edits to cloud unless reviewing
  logic (not the actual credentialed config)

### Deployment
- RHEL 9.4 offline — RPMs and wheels only
- @qwen only for deploy scripts — never any cloud tier

## Task-to-Model Quick Reference

| Task | Try 1st | Try 2nd | Try 3rd | Last Resort |
|---|---|---|---|---|
| FastAPI route | @qwen | nvidia-coder | ollama-cloud-coder | — |
| Bug trace | @gemma | nvidia-reason | nvidia-qwq | — |
| React component | nvidia-coder | nvidia-qwen235 | @screenshot | Claude Sonnet |
| Architecture | @gemma | nvidia-reason | nvidia-nemotron | Claude Opus |
| Lint JS/TS | eslint (free) | — | — | — |
| Lint Python | python-analyzer (free) | — | — | — |
| Long coding session | nvidia-m3 | nvidia-kimi-new | @qwen | — |
| Vendor PDF search | @rag | nvidia-context | @aiq | — |
| Code review | @review (on-demand) | — | — | — |
| Documentation | @docgen (on-demand) | — | — | — |
| Everyday/docs | Gemini Flash | nvidia-fast | @qwen | — |
| Private/credentials | @qwen or @gemma ONLY | — | — | never cloud |

## Git Rules
- `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`
- Commit after every completed task
- Never commit `private\` or `.env`