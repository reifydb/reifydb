<div align="center">

<picture>
  <img src="https://reifydb.com/logo.png" alt="ReifyDB Logo" width="512">
</picture>

<b>ReifyDB</b>: is a versatile, embeddable relational database built to solve real problems for real developers. Designed for those who care more about building than configuring.

<h3>
  <a href="https://reifydb.com">Homepage</a> |
  <a href="https://reifydb.com/#/documentation">Docs</a> |
  <a href="https://discord.com/invite/vuBrm5kuuF">Discord</a> |
  <a href="https://x.com/reifydb">X</a>
</h3>

[![GitHub Repo stars](https://img.shields.io/github/stars/reifydb/reifydb)](https://github.com/reifydb/reifydb/stargazers)
[![License](https://img.shields.io/github/license/reifydb/reifydb)](https://github.com/reifydb/reifydb/blob/main/license.md)
[![CI](https://img.shields.io/github/actions/workflow/status/reifydb/reifydb/ci.yml?label=CI)](https://github.com/reifydb/reifydb/actions/workflows/ci.yml)
[![TestSuite](https://img.shields.io/github/actions/workflow/status/reifydb/reifydb/testsuite.yml?label=TestSuite)](https://github.com/reifydb/reifydb/actions/workflows/testsuite.yml)

<p align="center">
  <strong>⚠️ IN DEVELOPMENT</strong><br>
  <em>Do not use in production, yet. The API is unstable and may change at any time.</em>
</p>

---
You can think of ReifyDB as both a relational database and backend combined into one. 
Instead of deploying a web server that sits in between your frontend and your database, clients connect directly to the database and execute your logic inside the database itself.
ReifyDB takes stored procedures to the next level, allowing you to deploy your whole application directly into the database. It's like a smart contract... if smart contracts were fast, cheap, and easy to use.

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

## License
ReifyDB is licensed under the The AGPL-3.0 or later (Affero General Public License).

---
## Commercial Support
ReifyDB is available as a managed service for selected users, if you're interested or need support, [contact](mailto:founder@reifydb.com) me for more information and deployment options.
