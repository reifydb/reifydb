// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Renderers for EXPLAIN. Each pipeline stage - tokenisation, AST, logical plan, physical plan - has a renderer
//! here so a user can ask the engine to describe what it sees at each level. Output is human-readable text suited
//! to a terminal or log; structured EXPLAIN consumers should walk the AST or plan directly rather than parsing
//! these strings.

pub mod ast;
pub mod logical;
pub mod physical;
pub mod tokenize;
