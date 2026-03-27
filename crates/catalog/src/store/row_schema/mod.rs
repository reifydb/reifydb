// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowSchema storage operations.
//!
//! Schemas are stored as:
//! - RowSchema header (fingerprint, field count, row size) under RowSchemaKey
//! - Individual fields under RowSchemaFieldKey for each field

pub(crate) mod create;
pub(crate) mod find;
pub(crate) mod schema;
