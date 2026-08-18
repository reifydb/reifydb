<div align="center">

<picture>
  <img src="https://reifydb.com/assets/img/logo.png" alt="ReifyDB Logo" width="512">
</picture>

<b>ReifyDB</b>  
<strong>The database that runs your backend logic.</strong>

One database instead of Postgres + Redis + a queue + a cron job.

<h3>
  <a href="https://reifydb.com">Homepage</a> |
  <a href="https://reifydb.com/docs">Docs</a> |
  <a href="https://reifydb.com/manifesto">Manifesto</a> |
  <a href="https://x.com/reifydb">X</a>
</h3>

[![GitHub Repo stars](https://img.shields.io/github/stars/reifydb/reifydb)](https://github.com/reifydb/reifydb/stargazers)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/reifydb/reifydb/blob/main/license.md)

<p align="center">
  <strong>IN DEVELOPMENT</strong><br>
  <em>Do not use in production yet. APIs and guarantees may change.</em>
</p>

---

</div>

## You have built this

You have a database. It holds the truth. Then the product needed to be fast, so the hot rows got copied into Redis. Then a dashboard needed a total, so a cron job started recomputing it every five minutes. Then a rule had to run when an order changed, so it moved into a worker behind a queue. Then something had to know which cache key to delete when the row changed, so you wrote that too.

And all of it connects to the database as one account, with one password, so the code in front decides on behalf of every user what they may see.

None of these were mistakes. Each one was the reasonable next step. The sum is five systems holding one application's state, held together by code whose only job is keeping them from disagreeing.

You did not architect that. You accumulated it.

```
+---------------+
|   POSTGRES    |
+---------------+
    ~ glue ~
+---------------+
|     REDIS     |
+---------------+
    ~ glue ~
+---------------+
|     CRON      |
+---------------+
    ~ glue ~
+---------------+
|     QUEUE     |
+---------------+
    ~ glue ~
+---------------+
|    WORKERS    |
+---------------+
five systems, one state

        |
        v

+---------------+
|    REIFYDB    |
|               |
|  tables       |
|  views        |
|  transitions  |
|  primitives   |
+---------------+
one system, one transaction
```

## Every box is an apology

- **Redis, the hot copy.** The database could not serve these rows fast enough, so now there are two of them. One is right.
- **Cron, the refresh.** The database could not keep a derived number current, so it is recomputed on a timer. Between runs it is wrong, and it looks fresh.
- **Queue and workers, the rule, later.** The database could not run your logic when the data changed, so the logic runs afterwards, elsewhere, and hopes the data has not moved.
- **Service account, the one password.** The database could not tell your users apart, so everything connects as one privileged account and the code in front of it decides who may do what. Every rule lives twice, one connection can do anything, and queries get built from user input on the way through.
- **Glue, the code that knows.** None of the above knows about the others, so you wrote the code that does. It is the most fragile code you own, and it ships no feature.

Databases became systems of record. Applications need systems of live state.

## What ReifyDB is built on

1. **Derived state is the database's job.** If a number can be computed from your data, you should never maintain it by hand. The write that changes the data is the thing that updates the number.
2. **A rule enforced in a service is a rule enforced sometimes.** Put the check on the data, inside the write that changes it, and there is no way around it.
3. **One write, one truth.** If a change and its consequences cannot commit together, you have two systems and a race between them.
4. **Counters, queues, and buffers are state, not cache.** They deserve the same transaction as the row next to them.
5. **The network is the speed limit.** The hot path should not have a network in it.
6. **The application user is the database user.** Every client authenticates to the database as itself, and policies decide, per user, what may be read and written. No shared service account, no privileged connection to hijack: a hostile query runs as the user, with the user's permissions, and can do nothing the user could not do anyway.

## What ReifyDB is

One database for that state.

- **Tables** hold the rows. State lives in memory and is persisted asynchronously, off the hot path.
- **Views** hold the derived numbers, and the write keeps them current. Nothing to refresh, nothing to poll.
- **Transitions** run your rules inside the transaction that changes the data. They are procedures and handlers: code you version and test inside the database, not a trigger someone forgot.
- **Primitives** are built in: counters, queues, ring buffers, histograms, all under the same transaction.
- **Embedded or server.** Run it inside your process like SQLite, or as a standalone server.
- **It knows who is asking.** Clients authenticate to the database as themselves, over WebSocket or HTTP, and policies gate every read and write per user. There is nothing in front of it holding the one password or re-checking permissions: clients talk to ReifyDB, and the rules about who may do what live with the data, like every other rule.

One feature, drawn twice:

```
TODAY                                 WITH REIFYDB

client                                client
  | POST /orders     as alice           | place_order(...)     as alice
  v                                     v
api server   check alice may,         reifydb
             build the query            policy     alice may place orders
  v                                     procedure  check balance, insert,
postgres     runs it as "app",                     debit: one transaction
             the one account            view       revenue updated by
  +-> redis   drop cached balance                  that same write
  +-> queue   worker: totals            v
  +-> cron    revenue, later          alice sees   balance, revenue
  v                                                (current, pushed)
client polls balance, revenue
             (stale in between)
```

Not for BI, warehouses, or ad-hoc reporting over cold history. Those belong in different systems.

The full argument: [reifydb.com/manifesto](https://reifydb.com/manifesto).

## Status

Version 0.9. Not production ready. APIs and guarantees will change, and every page says so. What will not change is the list above.

## Build

```bash
git clone https://github.com/reifydb/reifydb
cd reifydb
cargo build --release
```

Then read the [docs](https://reifydb.com/docs) or try it in the browser at the [playground](https://reifydb.com/playground).

## Contributing

Code contributions are not yet being accepted. See [contributing.md](contributing.md) for how to get involved.

- Report bugs or suggest features via [GitHub Issues](https://github.com/reifydb/reifydb/issues)
- Join the conversation on [GitHub Discussions](https://github.com/orgs/reifydb/discussions)
- Star the project so more people find it

## License

ReifyDB is open source under the [Apache-2.0 license](license.md).

## AI-Assisted Development

Parts of this codebase were written with AI assistance for rapid prototyping. These sections are intended to be rewritten as the project matures.
