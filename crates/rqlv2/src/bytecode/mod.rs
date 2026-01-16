// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode definitions, encoding/decoding, and compilation.
//!
//! This module provides:
//! - [`Opcode`] - Bytecode opcodes for the VM
//! - [`BytecodeReader`] / [`BytecodeWriter`] - Instruction encoding/decoding
//! - [`CompiledProgram`] - Compiled bytecode ready for execution
//! - [`SourceMap`] - Maps bytecode offsets to source spans for error reporting
//! - [`PlanCompiler`] - Compiles Plan to bytecode
//! - [`ProgramFormatter`] - Pretty-prints compiled programs for debugging
//! - [`explain_bytecode`] - Explains compiled programs (for testing/debugging)

pub mod compile;
pub mod display;
pub mod explain;
pub mod instruction;
pub mod opcode;
pub mod program;
