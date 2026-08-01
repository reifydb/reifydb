// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Instruction handlers the VM dispatches to, split into DDL (catalog mutations) and DML (data manipulation and
//! the read side). Each handler owns the validation and policy interactions specific to its operation.

pub(crate) mod ddl;
pub mod dml;
