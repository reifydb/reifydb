// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for the slice of catalog state an FFI extension can inspect: namespaces, tables, columns, and
//! primary keys. Limited intentionally - extensions do not need the entire catalog object hierarchy, only enough
//! to know what shape they are reading from or writing into.

pub mod column;
pub mod namespace;
pub mod primary_key;
pub mod table;
