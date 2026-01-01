// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB Query Language v2 (RQL) parser, AST, and plan
//!
//! This crate provides:
//! - Bump-allocated tokenization via the [`token`] module
//! - Unified AST for queries and scripting via the [`ast`] module
//! - Unified execution plan via the [`plan`] module

pub mod ast;
pub mod plan;
pub mod token;
