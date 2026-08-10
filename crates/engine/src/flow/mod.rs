// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The bridge between RQL's flow AST and the operator graph the streaming runtime evaluates: turns a CREATE
//! FLOW statement into the dataflow definition `sub-flow` consumes.

pub mod aggregate;
pub mod compiler;
pub mod time_domain;
