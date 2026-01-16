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

pub mod alter;
pub mod common;
pub mod dictionary_create;
pub mod drop;
pub mod flow_create;
pub mod index_create;
pub mod namespace_create;
pub mod ringbuffer_create;
pub mod series_create;
pub mod subscription_create;
pub mod table_create;
pub mod view_create;
