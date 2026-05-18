// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Engine-side compiler that turns a CREATE FLOW statement into the dataflow definition the `sub-flow` runtime
//! consumes. This module is the bridge between RQL's flow AST and the operator graph the streaming runtime
//! actually evaluates.

pub mod compiler;
