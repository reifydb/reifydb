// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM runtime components.
//!
//! This module contains the core runtime for executing bytecode:
//! - `state`: VM state and execution context
//! - `dispatch`: Opcode dispatch table
//! - `interpreter`: Bytecode interpretation logic
//! - `operand`: Operand stack values
//! - `stack`: Call stack management
//! - `scope`: Variable scoping
//! - `context`: VM configuration
//! - `builtin`: Built-in functions
//! - `script`: Script function support

pub mod builtin;
pub mod context;
pub mod dispatch;
pub mod interpreter;
pub mod operand;
pub mod scope;
pub mod script;
pub mod stack;
pub mod state;
