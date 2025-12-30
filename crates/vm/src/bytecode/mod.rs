// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Bytecode definitions and encoding/decoding.

pub mod instruction;
pub mod opcode;
pub mod program;

pub use instruction::{BytecodeReader, BytecodeWriter};
pub use opcode::{Opcode, OperatorKind};
pub use program::{FunctionDef, ParameterDef, Program, SourceDef};
