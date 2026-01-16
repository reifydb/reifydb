<div align="center">

<picture>
  <img src="https://reifydb.com/img/logo.png" alt="ReifyDB Logo" width="512">
</picture>

<b>ReifyDB</b>  
<strong>Application State Database</strong>

A database designed to manage live application state with transactional guarantees, incremental derived state, and embedded state transitions.

<h3>
  <a href="https://reifydb.com">Homepage</a> |
  <a href="https://reifydb.com/#/documentation">Docs</a> |
  <a href="https://x.com/reifydb">X</a>
</h3>

[![GitHub Repo stars](https://img.shields.io/github/stars/reifydb/reifydb)](https://github.com/reifydb/reifydb/stargazers)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](https://github.com/reifydb/reifydb/blob/main/license.md)

<p align="center">
  <strong>IN DEVELOPMENT</strong><br>
  <em>Do not use in production yet. APIs and guarantees may change.</em>
</p>

---

</div>

## About ReifyDB

ReifyDB is a **database for application state**.

It stores, mutates, and derives live application state under a single transactional model.  
State is kept in memory for low latency, persisted asynchronously for durability, and extended with application-defined logic that runs next to the data.

ReifyDB is built for systems where correctness, freshness, and predictable performance matter more than synchronous durability on every write.

It is designed to be **application-owned**. The database is part of your application, not a shared SQL endpoint for untrusted users.

---

## What ReifyDB Is For

ReifyDB is designed to manage **live, mutable application state**, such as:

- User and session state
- Trading and financial state
- Game and simulation state
- Workflow and process state
- Counters, queues, buffers, and aggregates
- Derived state that must stay correct as data changes

ReifyDB is not designed for BI, analytics warehouses, ad-hoc reporting, or untrusted multi-tenant SQL access.

---

## Core Capabilities

- **Transactional Application State**  
  ACID transactions over live, mutable state with predictable low latency

- **Incremental Derived State**  
  Materialized views that update automatically as state changes, without polling or batch refresh

- **Programmable State Transitions**  
  Application-defined logic that runs inside the database under the same transactional guarantees

- **Multiple Native State Primitives**  
  Tables, views, counters, ring buffers, histograms, and other state structures in one engine

- **Asynchronous Durability**  
  State is persisted off the hot path with bounded durability latency and deterministic recovery

- **Embeddable or Server Mode**  
  Run ReifyDB embedded in your application or as a standalone process, similar to SQLite or DuckDB

- **Direct Client Access**  
  Applications and services can connect directly using WebSocket or HTTP without intermediary APIs

---

## Design Principles

- Application state is first class  
- All state changes happen through transactions  
- Derived state is maintained incrementally  
- Logic runs next to state, not around it  
- Durability is decoupled from commit latency  
- The database is owned by the application, not end users  

---
## How ReifyDB Defines Application State

In ReifyDB, **application state** is the live, mutable data that directly determines how an application behaves at any moment in time.

This is not historical data, analytical data, or reporting data.  
It is the state that your application reads, updates, and reasons about on every request.

### What Counts as Application State

Application state includes:

- User and session state  
- Balances, positions, and counters  
- Game world and simulation state  
- Workflow progress and task coordination  
- Queues, buffers, and rate limits  
- Feature flags and configuration state  
- Derived aggregates that must stay correct as data changes  

This state is **continuously evolving** and must be:

- Fast to read and write  
- Correct under concurrency  
- Immediately visible to application logic  
- Safe to mutate transactionally  

---

### What Does Not Count as Application State

ReifyDB deliberately does not optimize for:

- Long term historical analytics  
- BI dashboards and reporting  
- Ad-hoc exploratory queries  
- Cold archival data  
- Untrusted, user-facing SQL workloads  

Those workloads have very different tradeoffs and belong in different systems.

---

### State First, Queries Second

In ReifyDB, state is primary. Queries are secondary.

- Tables represent authoritative state  
- Counters, buffers, and other primitives represent specialized state  
- Views represent **derived state**, not reports  

Derived state is maintained incrementally as part of state changes, not recomputed later through batch jobs or polling.

---

### State Changes Are Transactions

All application state in ReifyDB is mutated through transactions.

This guarantees that:

- State transitions are atomic  
- Invariants are preserved  
- Concurrent updates are isolated  
- Derived state remains consistent  

Application logic, whether expressed as queries or programmable state transitions, always executes within this transactional boundary.

---

### Durability Is a Property of State, Not the Hot Path

ReifyDB treats durability as a property of application state, not as a requirement for every individual write to block on disk.

- State changes become visible immediately after commit  
- Persistence happens asynchronously with bounded latency  
- Recovery deterministically rebuilds state from durable storage  

This allows ReifyDB to prioritize low latency and predictable performance while still providing strong durability guarantees over time.

---

### Why This Matters

Most modern applications struggle because application state is fragmented across databases, caches, workers, and background jobs.

ReifyDB defines application state as a single, first class concept and manages it under one transactional engine.

This reduces complexity, eliminates glue code, and makes stateful systems easier to build, reason about, and maintain.

---

## What ReifyDB Replaces

Modern applications often manage state across multiple systems. Each layer adds complexity, operational overhead, and failure modes.

ReifyDB replaces these patterns by centralizing application state under a single transactional engine.

### Databases Plus Caches

Traditional stacks separate durable storage from fast access layers.

- PostgreSQL or MySQL for persistence  
- Redis or Memcached for speed  
- Manual cache invalidation and consistency logic  

ReifyDB combines durability and low-latency state access in one system. State is kept in memory for fast reads and writes and persisted asynchronously, removing the need for a separate cache layer.

---

### Batch Materialized Views and Polling

Many systems rely on background jobs to keep derived data up to date.

- Periodic refresh of materialized views  
- Polling-based read models  
- Cron jobs and scheduled workers  

ReifyDB maintains derived state incrementally as part of the write path. Views stay correct automatically as data changes, without polling, refresh jobs, or batch recomputation.

---

### Glue Code and Background Workers

Application logic is often scattered across services.

- Triggers in the database  
- Workers updating counters and aggregates  
- Custom in-memory state machines  

ReifyDB allows application-defined state transitions to run inside the database under transactional guarantees. Logic and state evolve together, reducing glue code and synchronization bugs.

---

### Fragmented State Primitives

Different state representations are often handled by different systems.

- Tables in databases  
- Counters and queues in Redis  
- Buffers and streams in custom services  

ReifyDB provides multiple native state primitives under one transactional model. Tables, counters, ring buffers, and derived views all participate in the same consistency guarantees.

---

## Installation
Coming soon...
For now, clone and build locally:
```bash
git clone https://github.com/reifydb/reifydb
cd reifydb
cargo build --release
```
---

## Development

For developer documentation, build instructions, testing strategies, and contributing guidelines, see:

→ **[developer.md](developer.md)** - Complete developer guide

Quick links:
- [Getting Started](developer.md#1-getting-started)
- [Testing Strategy](developer.md#3-testing-strategy)
- [Code Quality Standards](developer.md#4-code-quality-standards)
- [Contributing Guidelines](developer.md#8-contributing-guidelines)

---

## Contributing

ReifyDB is in early development. Feedback and contributions are welcome.
- Check out the [issues](https://github.com/reifydb/reifydb/issues)
- [Open](https://github.com/orgs/reifydb/discussions) a discussion on GitHub Discussions
- Star the project to help more people find it
---

<h2>License</h2>

<p>
ReifyDB is <strong>open-source under the <a href="https://github.com/reifydb/reifydb/blob/main/license.md">AGPL-3.0 license</a></strong>.
</p>

<p>You are free to use, modify, and self-host ReifyDB, including for commercial projects, as long as:</p>
<ul>
  <li>Your changes are also open-sourced under AGPL</li>
  <li>You do not offer ReifyDB as a hosted service without sharing modifications</li>
</ul>

<h3>Commercial License</h3>

<p>If you want to use ReifyDB without the AGPL's obligations, for example to:</p>

<ul>
  <li>Embed it into a proprietary application</li>
  <li>Offer it as part of a hosted service or SaaS</li>
  <li>Avoid open-sourcing your modifications</li>
</ul>

<p>
There is a <strong>commercial license</strong> for ReifyDB.<br>
This supports the development of ReifyDB and ensures fair use.
</p>

<p>
<strong>Contact:</strong> <a href="mailto:founder@reifydb.com">founder@reifydb.com</a>
</p>

<h3>Dual Licensing Model</h3>

<p>ReifyDB is built using a <strong>dual licensing</strong> model:</p>

<ul>
  <li><strong>AGPL-3.0</strong> for open-source users and contributors</li>
  <li><strong>Commercial license</strong> for closed-source or hosted use</li>
</ul>

<p>This model keeps ReifyDB open, fair, and sustainable while making it easy for teams to build with confidence.</p>


                                                                                                                                                                           
## AI-Assisted Development                                                                                                                                                 
                                                                                                                                                                           
Parts of this codebase were written with AI assistance for rapid prototyping. These sections are intended to be rewritten as the project matures.                          
                                                                                                                                                                           
---

---
## Commercial Support
ReifyDB is available as a managed service for enterprise users. If you're interested or need support, [contact](mailto:founder@reifydb.com) me for more information and deployment options.
