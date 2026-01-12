// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML statement parsing (INSERT, UPDATE, DELETE).
//!
//! RQL DML syntax differs from SQL:
//! - `INSERT [namespace.]table` - Just the target, data comes from pipeline
//! - `UPDATE [namespace.]table` - Target optional, modifications come from pipeline
//! - `DELETE [namespace.]table` - Target optional, filter comes from pipeline

mod delete;
mod insert;
mod update;
