// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL statement parsing (CREATE, ALTER, DROP).
//!
//! RQL DDL syntax uses curly braces for column definitions:
//! - `CREATE TABLE namespace.table { col: Type, ... }`
//! - `CREATE VIEW namespace.view AS { query }`
//! - `CREATE DEFERRED VIEW namespace.view { col: Type } [AS { query }]`
//! - `CREATE TRANSACTIONAL VIEW namespace.view { col: Type } [AS { query }]`
//! - `CREATE NAMESPACE name [IF NOT EXISTS]`
//! - `CREATE INDEX idx ON namespace.table { col1, col2 }`
//! - `CREATE UNIQUE INDEX idx ON namespace.table { col }`
//! - `ALTER TABLE namespace.table { CREATE PRIMARY KEY { cols } }`
//! - `DROP FLOW [IF EXISTS] name`

mod alter;
mod common;
mod dictionary_create;
mod drop;
mod flow_create;
mod index_create;
mod namespace_create;
mod ringbuffer_create;
mod series_create;
mod subscription_create;
mod table_create;
mod view_create;
