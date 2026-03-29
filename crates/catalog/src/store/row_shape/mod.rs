// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape storage operations.
//!
//! Shapes are stored as:
//! - RowShape header (fingerprint, field count, row size) under RowShapeKey
//! - Individual fields under RowShapeFieldKey for each field

pub(crate) mod create;
pub(crate) mod find;
pub(crate) mod shape;
