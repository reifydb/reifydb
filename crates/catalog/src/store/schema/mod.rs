// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema storage operations.
//!
//! Schemas are stored as:
//! - Schema header (fingerprint, field count, row size) under SchemaKey
//! - Individual fields under SchemaFieldKey for each field

mod create;
mod get;
mod layout;

pub use create::create_schema;
pub use get::{find_schema_by_fingerprint, load_all_schemas};
