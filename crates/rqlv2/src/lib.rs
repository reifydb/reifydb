// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB Query Language v2 (RQL) parser, AST, plan, and bytecode
//!
//! This crate provides:
//! - Bump-allocated tokenization via the [`token`] module
//! - Unified AST for queries and scripting via the [`ast`] module
//! - Unified execution plan via the [`plan`] module
//! - Bytecode compilation and encoding via the [`bytecode`] module
//! - Compiled expressions via the [`expression`] module

pub mod ast;
pub mod bytecode;
pub mod expression;
pub mod plan;
pub mod token;
