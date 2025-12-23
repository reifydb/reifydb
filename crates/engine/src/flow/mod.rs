// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Flow compilation module - compiles RQL plans into Flows
//!
//! This module contains the flow compiler that was moved from reifydb-rql to avoid
//! lifetime issues with async recursion and generic CommandTransaction types.

mod compiler;

pub use compiler::compile_flow;
