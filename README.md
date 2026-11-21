<div align="center">

<img src="pictures/banner.jpg" alt="FinCore-AI Architecture Banner" width="100%" />

<br />

<img src="https://readme-typing-svg.demolab.com?font=Inter&weight=700&size=24&pause=1400&color=E2A37E&center=true&vCenter=true&width=640&lines=FinCore-AI+%E2%80%94+Payment+Risk+%26+Fraud+ML+Engine;Open-Source+Core+Powering+PayShield+%F0%9F%9B%A1%EF%B8%8F;15+Handcrafted+Modules+%C2%B7+Zero+Boilerplate" alt="FinCore-AI Typing SVG" />

<p>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/🐍_Python-3.11+-161514?style=for-the-badge" alt="Python" />
  </a>
  <a href="https://fastapi.tiangolo.com/">
    <img src="https://img.shields.io/badge/⚡_FastAPI-REST_API-009688?style=for-the-badge" alt="FastAPI" />
  </a>
  <a href="https://scikit-learn.org/">
    <img src="https://img.shields.io/badge/🤖_scikit--learn-Fraud_ML-C87D55?style=for-the-badge" alt="scikit-learn" />
  </a>
  <a href="https://www.docker.com/">
    <img src="https://img.shields.io/badge/🐳_Docker-Ready-2496ED?style=for-the-badge" alt="Docker" />
  </a>
</p>

<p>
  🌐 <a href="https://radagastroigi.github.io/"><strong>Portfolio</strong></a> &nbsp;·&nbsp;
  💼 <a href="https://www.linkedin.com/in/shiva-rakesh-s/"><strong>LinkedIn</strong></a> &nbsp;·&nbsp;
  🎬 <a href="https://www.loom.com/share/41bde33a487442dfaca639692ed312cd"><strong>60s Video Intro</strong></a> &nbsp;·&nbsp;
  ✉️ <a href="mailto:monkthetics@gmail.com"><strong>Contact</strong></a>
</p>

</div>

---

### ⚙️ What It Does

Handcrafted from first principles to mirror production payment gateways, exchanges, and fraud engines—verified end-to-end with `pytest`.

| Pillar | Built-In Capabilities |
| :--- | :--- |
| 🔐 **Payment Integrity** | `O(1)` idempotency cache, ACID double-entry ledger with optimistic locking, transactional outbox |
| 🛡️ **Fraud & Risk ML** | Sliding-window velocity limiter, `scikit-learn` Gradient Boosting scorer (Precision/Recall/FPR), PSI drift alerts |
| ⚡ **Reliability & Scale** | 3-state circuit breaker, ASGI HTTP `429` rate limiter, HMAC-SHA256 webhook dispatcher with backoff & DLQ |
| 📊 **Matching & AI Audit** | Two-heap limit order book, atomic TTL flash-sale holds, prompt/SQL-injection firewall, Hybrid RAG reranker |

---

### 🚀 Quickstart

```bash
uv sync                                                   # Install dependencies
uv run python -m pytest -v                                # Run full test suite
uv run python -m uvicorn fincore_api.server:app --reload  # Start API (Swagger: /docs)
```

---

### 📦 Shipped Releases (`v1.01` – `v1.15`)

<details open>
<summary><strong>🔥 Latest Releases (v1.11 – v1.15)</strong></summary>

| Release | Module | Core Pattern |
| :--- | :--- | :--- |
| `v1.15` | **Fraud ML Scorer & PSI Drift Monitor** | `scikit-learn` Gradient Boosting (Precision/Recall/FPR) + 10-bin PSI drift alerts |
| `v1.14` | **Signed Webhook Dispatcher & DLQ** | Replay-safe HMAC-SHA256, exponential backoff (`2s → 4s → 8s`), Dead-Letter Queue |
| `v1.13` | **Limit Order Book Matching Engine** | Two-Heap (`Max-Heap` Bids / `Min-Heap` Asks) price-time priority matching |
| `v1.12` | **Flash-Sale Atomic Hold & TTL Rollback** | Lazy TTL reservation hold & auto-rollback (IRCTC / flash-sale pattern) |
| `v1.11` | **Min-Heap Priority Payment Scheduler** | `heapq` deterministic `(priority, seq, tx_id, amount)` queue |

</details>

<details>
<summary><strong>🧱 Core Foundation Releases (v1.01 – v1.10)</strong></summary>

| Release | Module | Core Pattern |
| :--- | :--- | :--- |
| `v1.10` | **Zero-Dependency Hybrid RAG Reranker** | Lexical keyword overlap + cosine vector similarity reranking |
| `v1.09` | **Safe NL-to-SQL Audit & Injection Firewall** | Pre-execution SQL & prompt-injection guard for read-only audits |
| `v1.08` | **HMAC-SHA256 JWT & RBAC Guard** | Constant-time `hmac.compare_digest` verification |
| `v1.07` | **3-State Circuit Breaker & Dockerfile** | `CLOSED → OPEN → HALF_OPEN` resilience guard + container build |
| `v1.06` | **ASGI HTTP Rate Limiter Middleware** | Sliding-window HTTP `429` enforcement + `pytest` harness |
| `v1.05` | **Unified FastAPI + SQLite Service** | Production REST API layer (`fincore_api/server.py`) |
| `v1.04` | **Transactional Outbox Pattern** | Atomic DB + `OutboxEvent` commit preventing dual-write loss |
| `v1.03` | **ACID Double-Entry Ledger** | Immutable Debit/Credit entries + Optimistic Concurrency Control |
| `v1.02` | **Sliding-Window Velocity Limiter** | `O(1)` amortized `collections.deque` rolling-window guard |
| `v1.01` | **Payment Idempotency Engine** | `O(1)` HashSet deduplication blocking duplicate charge replays |

</details>

---

### ☁️ Next Milestone: Hosted **PayShield** Demo
- 🌐 Deploy FastAPI API + clean merchant dashboard to a live public URL (Render / Hugging Face Spaces).
- 🔌 Add Razorpay & Stripe webhook adapters and 1-command `docker-compose up` self-hosting.
