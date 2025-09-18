<div align="center">

<picture>
  <img src="https://reifydb.com/img/logo.png" alt="ReifyDB Logo" width="512">
</picture>

<b>ReifyDB</b>: is a versatile, embeddable relational database built to solve real problems for real developers. Designed for those who care more about building than configuring.

<h3>
  <a href="https://reifydb.com">Homepage</a> |
  <a href="https://reifydb.com/#/documentation">Docs</a> |
  <a href="https://discord.com/invite/vuBrm5kuuF">Discord</a> |
  <a href="https://x.com/reifydb">X</a>
</h3>

[![GitHub Repo stars](https://img.shields.io/github/stars/reifydb/reifydb)](https://github.com/reifydb/reifydb/stargazers)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](https://github.com/reifydb/reifydb/blob/main/license.md)

[![Workspace](https://img.shields.io/github/actions/workflow/status/reifydb/reifydb/workspace.yml?label=Workspace)](https://github.com/reifydb/reifydb/actions/workflows/workspace.yml)
[![TestSuite](https://img.shields.io/github/actions/workflow/status/reifydb/reifydb/test-suite.yml?label=TestSuite)](https://github.com/reifydb/reifydb/actions/workflows/test-suite.yml)
[![TypeScript](https://img.shields.io/github/actions/workflow/status/reifydb/reifydb/pkg-typescript.yml?label=TypeScript)](https://github.com/reifydb/reifydb/actions/workflows/pkg-typescript.yml)

<p align="center">
  <strong>⚠️ IN DEVELOPMENT</strong><br>
  <em>Do not use in production, yet. The API is unstable and may change at any time.</em>
</p>

---
You can think of ReifyDB as both a relational database and backend combined into one. 
Instead of deploying a web server that sits in between your frontend and your database, clients connect directly to the database and execute your logic inside the database itself.
ReifyDB takes stored procedures to the next level, allowing you to deploy your whole application directly into the database. It's like a smart contract... if smart contracts were fast, cheap, and easy to use.

---

## 📚 Examples

Learn ReifyDB through practical, working examples in [`pkg/rust/examples`](pkg/rust/examples):

### Quick Start

```bash
# Run all examples
cd pkg/rust/examples && make

# Run individual examples
cd pkg/rust/examples && make basic-hello-world
```


📖 See the [examples README](pkg/rust/examples/readme.md) for the complete list and detailed instructions.

---

</div>
<h2>🔧 What Makes ReifyDB Unique (Boiled Down)</h2>

<table>
  <thead>
    <tr>
      <th>Feature</th>
      <th>Why It Matters</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>🧠 Imperative query language</td>
      <td>Developers specify exactly what happens — no planner surprises, no magic</td>
    </tr>
    <tr>
      <td>⚡️ No ORM, no REST, no boilerplate</td>
      <td>You write logic directly on the data. The DB is the backend.</td>
    </tr>
    <tr>
      <td>🔒 Frontend can talk to DB directly</td>
      <td>No injection risk — app users = DB users</td>
    </tr>
    <tr>
      <td>🧩 Embeddable or server</td>
      <td>Works like SQLite or DuckDB — use in apps, scripts, or as a daemon</td>
    </tr>
    <tr>
      <td>🔄 Multi-statement transactions</td>
      <td>One request = one atomic block, reducing race conditions</td>
    </tr>
    <tr>
      <td>🔍 Optimized for reads + reactive views</td>
      <td>Great for dashboards, analytics, and apps that read more than write</td>
    </tr>
    <tr>
      <td>🧪 Testable, deterministic, inspectable</td>
      <td>Write fast, reliable integration tests — the DB is predictable and local</td>
    </tr>
  </tbody>
</table>

## 📦 Installation
Coming soon...
For now, clone and build locally:
```bash
git clone https://github.com/reifydb/reifydb
cd reifydb
cargo build --release
```
---

## 🤝 Contributing
ReifyDB is still in early development — feedback and contributions are welcome!
- Check out the [issues](https://github.com/reifydb/reifydb/issues)
- [Open](https://github.com/orgs/reifydb/discussions) a discussion on GitHub Discussions
- Star ⭐️ the project to help more people find it!
---

<h2>🧾 License</h2>

<p>
ReifyDB is <strong>open-source under the <a href="https://github.com/reifydb/reifydb/blob/main/license.md">AGPL-3.0 license</a></strong>.
</p>

<p>This means you are free to use, modify, and self-host ReifyDB — even for commercial projects — as long as:</p>
<ul>
  <li>Your changes are also open-sourced under AGPL</li>
  <li>You do not offer ReifyDB as a hosted service without sharing modifications</li>
</ul>

<h3>💼 Commercial License</h3>

<p>If you want to use ReifyDB without the AGPL’s obligations — for example to:</p>

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
👉 <strong>Contact:</strong> <a href="mailto:founder@reifydb.com">founder@reifydb.com</a>
</p>

<h3>⚖️ Dual Licensing Model</h3>

<p>ReifyDB is built using a <strong>dual licensing</strong> model:</p>

<ul>
  <li><strong>AGPL-3.0</strong> for open-source users and contributors</li>
  <li><strong>Commercial license</strong> for closed-source or hosted use</li>
</ul>

<p>This model helps to stay open, fair, and sustainable — while making it easy for serious teams to build with confidence.</p>

---
## Commercial Support
ReifyDB is available as a managed service for enterprise users. If you're interested or need support, [contact](mailto:founder@reifydb.com) me for more information and deployment options.
