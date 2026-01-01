// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! AST to Plan compilation.

mod catalog;
mod control;
mod core;
mod ddl;
mod dml;
mod expr;
mod projection;
mod query;
mod scope;
mod statement;

// Public API - preserves existing external interface
pub use core::{PlanError, PlanErrorKind, Result, plan};
