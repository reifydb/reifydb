// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML statement parsing (INSERT, UPDATE, DELETE).
//!
//! The actual implementations are in the `parse::dml` module:
//! - `parse::dml::table_insert` - INSERT parsing
//! - `parse::dml::table_update` - UPDATE parsing
//! - `parse::dml::table_delete` - DELETE parsing
//!
//! This file is kept for backwards compatibility with module structure.
