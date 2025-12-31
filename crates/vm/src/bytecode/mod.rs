// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode definitions and encoding/decoding.

pub mod instruction;
pub mod opcode;
pub mod program;

pub use instruction::{BytecodeReader, BytecodeWriter};
pub use opcode::{Opcode, OperatorKind};
pub use program::{FunctionDef, ParameterDef, Program, SourceDef};
