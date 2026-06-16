# UnifiedOps v2 Agents Configuration

## Project Overview
UnifiedOps v2 (Hi-Track Alert v1.4.1) is an airgapped storage automation
and monitoring solution designed to run across a 4-server topology on
RHEL 9.4. The system ingests syslog UDP packets from storage arrays across
three pipeline VMs (CDVL, BCP, SIFY) using Python listener scripts, and
writes the telemetry data to a local InfluxDB instance on each node. A
centralized UI VM hosts a Python FastAPI backend that proxies requests to
the pipeline VMs and serves a React/Vite frontend dashboard, providing
operators with real-time alerts pushed via WebSockets.

## Model Selection Rules

### Local Models (Privacy-Safe / Offline / Unlimited)
- **Default coding**: Use `qwen3-coder:30b` via @qwen for all general
  backend coding, API routes, FastAPI services, InfluxDB queries,
  WebSocket logic, and listener scripts.
- **Debugging / reasoning**: Use `deepseek-r1:14b` via @gemma for complex
  bug tracing, architectural decisions, data flow analysis across the
  4-server topology, and explaining why something is broken step by step.
- **Fast edits**: Use `qwen2.5-coder:1.5b` for single line fixes,
  renaming, formatting, minor syntax corrections.
- **Quick screenshots**: Use `moondream2` via @vision for fast UI
  screenshot analysis, reading dashboard images, 4GB VRAM safe.
- **Deep image analysis**: Use `gemma3:4b` via @screenshot for detailed
  UI feedback, complex diagrams, SAN/network topology images, 4GB VRAM safe.

### Antigravity Cloud Models (Primary Cloud Layer)
- **UI / Frontend**: Use Claude Sonnet 4.6 Thinking for all React
  components, TypeScript, Tailwind CSS, Zustand state, dashboard layout,
  and design system work.
- **Complex architecture**: Use Claude Opus 4.6 Thinking for large
  multi-file refactors, system design decisions. Use sparingly — slow.
- **Everyday tasks**: Use Gemini 3.5 Flash (High) for quick summaries,
  explanations, documentation, anything not requiring deep reasoning.
- **Browser / deploy**: Use Gemini 3.1 Pro (High) for browser subagent
  tasks, RHEL deployment validation, Google Cloud integrations.

### NVIDIA NIM Models (Secondary Cloud Layer / Antigravity Credit Fallback)
Use when Antigravity cloud credits run low. Never send private files or
credentials to NIM — it is cloud infrastructure same as Antigravity.

**Coding & Agentic**
- **nvidia-coder** (`qwen3-coder:480b`): Frontier coding fallback when
  Claude Sonnet or Gemini runs out.
- **nvidia-agent** (`mistral/mistral-nemotron`): Best function calling
  model on NIM. Use for agentic workflows, tool use, Antigravity agents.
- **nvidia-gpt** (`gpt-oss-120b`): General coding fallback, efficient
  MoE — good quality at lower credit cost.
- **nvidia-glm** (`zhipuai/glm-5.1`): Strong agentic coding with SOTA
  tool calling, 744B but credit-efficient.

**Reasoning**
- **nvidia-reason** (`deepseek-r1:671b`): Full R1 reasoning — not a
  distillation. Far stronger than local 14b for complex analysis.
- **nvidia-kimi** (`moonshotai/kimi-k2.5`): 1M context + strong
  reasoning. Alternative to deepseek-r1 for long analysis tasks.
- **nvidia-qwq** (`qwen/qwq-32b`): Chain-of-thought reasoning, smaller
  and faster than 671b for step-by-step debugging.

**General & Fast**
- **nvidia-fast** (`minimax-m2.7`): 230B sparse MoE, strong all-round.
  Use when Gemini Flash runs out.
- **nvidia-llama** (`meta/llama-3.3-70b-instruct`): Reliable fallback,
  lower credit cost than frontier models.
- **nvidia-phi** (`microsoft/phi-4-mini-flash-reasoning`): Tiny but
  capable, very fast, minimal credit usage for quick lookups.

**Long Context**
- **nvidia-context** (`deepseek-v4-flash`): 1M context window. Use for
  large codebase analysis, whole-repo tasks, 700+ page vendor PDFs.

**Vision & Document Parsing**
- **nvidia-vision** (`gemma4:e4b`): Cloud vision for complex diagrams,
  network topology images, SAN architecture drawings.
- **nvidia-parse** (`nvidia/nemotron-parse`): Extracts text and metadata
  from images/PDFs — built for document parsing, tables, charts.
- **nvidia-docparse** (`nvidia/nemoretriever-parse`): OCR + table
  extraction from scanned vendor documents.

**Embeddings & RAG**
- **nvidia-embed** (`nvidia/nv-embedqa-e5-v5`): General RAG embeddings
  for vendor manual search.
- **nvidia-embedcode** (`nvidia/nv-embedcode-7b`): Code-specific
  embeddings — better RAG over your Python/FastAPI codebase.
- **nvidia-rerank** (`nvidia/nv-rerankqa-mistral-4b-v3`): Reranks RAG
  results for significantly better accuracy.

**Safety**
- **nvidia-safety** (`nvidia/nemotron-content-safety`): Content
  moderation layer — screen prompts before cloud submission.

### NVIDIA Skills / MCP Servers
- **@route**: Auto-routes prompt to best NIM model by task type.
  Use instead of manually picking a model when unsure.
- **@aiq**: NVIDIA AI-Q deep research agent with storage/SAN context.
  Use for deep vendor doc questions, cited answers, multi-step analysis.
- **@rag**: RAG pipeline over indexed vendor PDFs (Hitachi, NetApp,
  Brocade, Dell manuals in docs\ folder).
  Use for: "find CLI command for X in the manual".

### Fallback Chain (try each level before moving to next)

| Need | Level 1 Primary | Level 2 NIM | Level 3 Local |
|---|---|---|---|
| UI / Frontend | Claude Sonnet 4.6 | nvidia-coder | @screenshot |
| Architecture | Claude Opus 4.6 | nvidia-reason | @gemma |
| General coding | @qwen (local) | nvidia-coder | nvidia-gpt |
| Everyday tasks | Gemini 3.5 Flash | nvidia-fast | @qwen |
| Agentic workflows | Gemini 3.1 Pro | nvidia-agent | @qwen |
| Large files / PDFs | nvidia-context | @rag (indexed) | @gemma |
| Vendor doc search | @rag | nvidia-context | @aiq |
| Vision / diagrams | nvidia-vision | @screenshot | @vision |
| Quick screenshots | @vision (local) | @screenshot | — |
| All cloud down | — | — | Full local only |
| Private / credentials | @qwen or @gemma only | Never NIM | — |

## Security Rules
- **NEVER use cloud models (Antigravity OR NVIDIA NIM) for**:
  - Files inside `private\` directory
  - InfluxDB tokens, self-signed TLS certificates, API keys
  - Any `.env` files or secrets configs
  - Deployment scripts containing server IPs or credentials
  - RHEL server usernames, passwords, or SSH keys
  - FC/SAN zone configs, LUN mappings, RAID group data
  - SNMP community strings or array management credentials
- **NVIDIA NIM is cloud** — treat identically to Antigravity cloud.
- **Always use local @qwen or @gemma for the above.**

## Project-Specific Agent Rules

### Backend (Python)
- Write Python 3.9 compatible code only
- Use `from __future__ import annotations` at top of every file
- Use explicit typing: `Dict`, `List`, `Optional`, `Tuple` — not `dict`,
  `list` etc.
- Respect existing FastAPI patterns: `ThreadPoolExecutor` for InfluxDB
  connections and `WsHub` for WebSocket management
- Never introduce new dependencies without asking first — airgapped system

### Frontend (React)
- Write React 19 functional components with TypeScript only
- Use Vite — no CRA or Next.js
- Use Zustand for global state, `@tanstack/react-table` for data grids
- Use Tailwind CSS only — no inline styles, no CSS modules
- Use Claude Sonnet 4.6 Thinking for all frontend work
- Fall back to nvidia-coder if Sonnet credits run out
- Fall back to @screenshot (gemma3:4b) if all cloud unavailable

### Syslog Listeners
- Each listener must remain fully self-contained per location
- No cross-location imports: `_cdvl`, `_bcp`, `_sify` are isolated
- Never create shared cross-location dependencies

### Deployment
- Target RHEL 9.4 offline installation — RPMs and Python wheels only
- Scripts written in PowerShell on Windows, execute on RHEL
- Always use @qwen for deployment scripts — never any cloud model

## Task-to-Model Quick Reference

| Task | Primary | Fallback 1 | Fallback 2 |
|---|---|---|---|
| FastAPI route / endpoint | @qwen (30b) | nvidia-coder (480b) | nvidia-gpt |
| InfluxDB query / schema | @qwen (30b) | nvidia-coder (480b) | — |
| WebSocket / WsHub logic | @qwen (30b) | nvidia-coder (480b) | — |
| Syslog listener changes | @qwen (local only) | — never cloud — | — |
| Bug trace / data flow | @gemma (14b) | nvidia-reason (671b) | nvidia-qwq |
| Architecture decision | @gemma (14b) | nvidia-reason (671b) | @aiq |
| React component / UI | Claude Sonnet 4.6 | nvidia-coder (480b) | @screenshot |
| Dashboard layout / CSS | Claude Sonnet 4.6 | nvidia-coder (480b) | @screenshot |
| Tailwind / Zustand | Claude Sonnet 4.6 | nvidia-coder (480b) | @screenshot |
| TanStack table / grid | Claude Sonnet 4.6 | nvidia-coder (480b) | @screenshot |
| Large multi-file refactor | Claude Opus 4.6 | nvidia-reason (671b) | @gemma |
| System design | Claude Opus 4.6 | nvidia-reason (671b) | @aiq |
| Agentic workflow | Gemini 3.1 Pro | nvidia-agent | @qwen |
| Whole repo analysis | nvidia-context (1M) | @gemma (14b) | — |
| Vendor PDF 700 pages | @rag (indexed) | nvidia-context (1M) | @aiq |
| SAN / network diagrams | nvidia-vision | @screenshot (4b) | — |
| Quick UI screenshots | @vision (moondream2) | @screenshot (4b) | — |
| Docs / summaries | Gemini 3.5 Flash | nvidia-fast (m2.7) | @qwen |
| Function calling / agents | nvidia-agent | nvidia-glm | @qwen |
| Code RAG / search | nvidia-embedcode | nvidia-embed | — |
| Auto model selection | @route | — | — |
| Deep vendor doc research | @aiq | @rag | nvidia-context |
| RHEL deploy scripts | @qwen (local only) | — never cloud — | — |
| Private / credentials | @qwen or @gemma only | — never cloud — | — |

## Git Rules
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`
- Commit after every completed task
- Never commit files from `private\` directory
- Add `private\` and `.env` to `.gitignore` if not already there