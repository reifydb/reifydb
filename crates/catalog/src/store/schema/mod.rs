// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema storage operations.
//!
//! Schemas are stored as:
//! - Schema header (fingerprint, field count, row size) under SchemaKey
//! - Individual fields under SchemaFieldKey for each field

pub(crate) mod create;
pub(crate) mod find;
pub(crate) mod schema;
