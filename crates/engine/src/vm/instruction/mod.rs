// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Instruction handlers the VM dispatches to. Split into DDL (catalog mutations: create/alter/drop, plus
//! migrations) and DML (data manipulation: insert/update/delete, plus the read-side scans, joins, projections).
//! Each handler owns the validation and policy interactions specific to its operation.

pub(crate) mod ddl;
pub(crate) mod dml;
