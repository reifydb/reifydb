// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Human-readable explanation of bytecode programs.
//!
//! This module provides a simple wrapper around the display formatter
//! for consistency with other explain modules (ast, plan, token).

use super::{display::ProgramFormatter, program::CompiledProgram};

/// Explain a compiled bytecode program in human-readable format.
///
/// This uses the IDA-style disassembly formatter from the display module
/// to show the complete bytecode program including:
/// - Program metadata (entry point, size, pool counts)
/// - Constants pool
/// - Source definitions
/// - Column lists and other metadata
/// - Disassembled bytecode with hex bytes and resolved references
pub fn explain_bytecode(program: &CompiledProgram) -> String {
	ProgramFormatter::new(program).format()
}
