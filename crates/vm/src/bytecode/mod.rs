// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode definitions and encoding/decoding.
//!
//! # Deprecation Notice
//!
//! **This module is deprecated.** The VM now uses RQLv2's bytecode format directly.
//!
//! Use [`reifydb_rqlv2::bytecode`] instead:
//! - [`reifydb_rqlv2::bytecode::Opcode`] - Bytecode opcodes
//! - [`reifydb_rqlv2::bytecode::OperatorKind`] - Operator types
//! - [`reifydb_rqlv2::bytecode::CompiledProgram`] - Compiled program structure
//! - [`reifydb_rqlv2::bytecode::BytecodeReader`] - Bytecode decoder
//! - [`reifydb_rqlv2::bytecode::BytecodeWriter`] - Bytecode encoder
//!
//! This module will be removed in a future release.

#[deprecated(
	since = "0.2.0",
	note = "Use reifydb_rqlv2::bytecode instead. This module will be removed in a future release."
)]
pub mod instruction;
#[deprecated(
	since = "0.2.0",
	note = "Use reifydb_rqlv2::bytecode instead. This module will be removed in a future release."
)]
pub mod opcode;
#[deprecated(
	since = "0.2.0",
	note = "Use reifydb_rqlv2::bytecode instead. This module will be removed in a future release."
)]
pub mod program;

#[allow(deprecated)]
pub use instruction::{BytecodeReader, BytecodeWriter};
#[allow(deprecated)]
pub use opcode::{Opcode, OperatorKind};
#[allow(deprecated)]
pub use program::{FunctionDef, ParameterDef, Program, SourceDef};
