// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![forbid(unsafe_code)]

pub use function::*;
pub use global::*;
pub use memory::*;
pub use module::*;
pub use table::*;
pub use types::*;
pub use value::*;

pub mod function;
pub mod global;
pub mod memory;
pub mod module;
pub mod table;
pub mod types;
pub mod value;

// ---------------------------------------------------------------------------
// Index type aliases
// ---------------------------------------------------------------------------

pub type BranchingDepth = usize;
pub type FunctionIndex = usize;
pub type FunctionTypeIndex = usize;
pub type GlobalIndex = usize;
pub type LocalIndex = usize;
pub type MemoryIndex = usize;
pub type TableIndex = usize;
pub type TableElementIndex = usize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const PAGE_SIZE: u32 = 65536; // 64KiB

// ---------------------------------------------------------------------------
// Error / Trap types
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum Error {
	Trap(Trap),
}

#[derive(Debug, PartialEq)]
pub enum Trap {
	CallDepthExceeded,
	Conversion,
	DivisionByZero(TrapDivisionByZero),

	NotFound(TrapNotFound),
	NotImplemented(TrapNotImplemented),

	OutOfFuel,
	OutOfRange(TrapOutOfRange),
	Overflow(TrapOverflow),

	Type(TrapType),
	Underflow(TrapUnderflow),
	Unreachable,
	UninitializedElement,
	UndefinedElement,
	UnresolvedHostFunction(String, String),
}

impl std::fmt::Display for Trap {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Trap::CallDepthExceeded => write!(f, "call depth exceeded"),
			Trap::Conversion => write!(f, "invalid conversion to integer"),
			Trap::DivisionByZero(t) => write!(f, "{}", t),
			Trap::NotFound(t) => write!(f, "{}", t),
			Trap::NotImplemented(t) => write!(f, "{}", t),
			Trap::OutOfFuel => write!(f, "out of fuel"),
			Trap::OutOfRange(t) => write!(f, "{}", t),
			Trap::Overflow(t) => write!(f, "{}", t),
			Trap::Type(t) => write!(f, "{}", t),
			Trap::Underflow(t) => write!(f, "{}", t),
			Trap::Unreachable => write!(f, "unreachable"),
			Trap::UninitializedElement => write!(f, "uninitialized element"),
			Trap::UndefinedElement => write!(f, "undefined element"),
			Trap::UnresolvedHostFunction(module, name) => {
				write!(f, "unresolved host function: {}::{}", module, name)
			}
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapDivisionByZero {
	Integer,
}

impl std::fmt::Display for TrapDivisionByZero {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapDivisionByZero::Integer => write!(f, "integer divide by zero"),
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapNotFound {
	ExportedFunction(String),
	Function(String),
	FunctionLocal(FunctionIndex),
	FunctionType(FunctionTypeIndex),
	Memory(MemoryIndex),
	Module(String),
	ReturnValue,
	Table(TableIndex),
	TableElement(TableIndex, TableElementIndex),
}

impl std::fmt::Display for TrapNotFound {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapNotFound::ExportedFunction(name) => {
				write!(f, "unknown function: exported function '{}' not found", name)
			}
			TrapNotFound::Function(name) => write!(f, "unknown function '{}'", name),
			TrapNotFound::FunctionLocal(idx) => write!(f, "unknown function {}", idx),
			TrapNotFound::FunctionType(idx) => write!(f, "unknown type {}", idx),
			TrapNotFound::Memory(idx) => write!(f, "unknown memory {}", idx),
			TrapNotFound::Module(name) => write!(f, "unknown module '{}'", name),
			TrapNotFound::ReturnValue => write!(f, "missing return value"),
			TrapNotFound::Table(idx) => write!(f, "unknown table {}", idx),
			TrapNotFound::TableElement(_, _) => write!(f, "undefined element"),
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapNotImplemented {
	Instruction(Instruction),
}

impl std::fmt::Display for TrapNotImplemented {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapNotImplemented::Instruction(i) => {
				write!(f, "instruction not implemented {:?}", i)
			}
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapOutOfRange {
	Memory(MemoryIndex),
	Table(TableIndex),
}

impl std::fmt::Display for TrapOutOfRange {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapOutOfRange::Memory(_) => write!(f, "out of bounds memory access"),
			TrapOutOfRange::Table(_) => write!(f, "out of bounds table access"),
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapOverflow {
	Integer,
	Stack,
}

impl std::fmt::Display for TrapOverflow {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapOverflow::Integer => write!(f, "integer overflow"),
			TrapOverflow::Stack => write!(f, "stack overflow"),
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapType {
	MismatchValueType(ValueType, ValueType),
	MismatchIndirectCallType(FunctionType, FunctionType),
}

impl std::fmt::Display for TrapType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapType::MismatchValueType(expected, actual) => {
				write!(f, "expected type {:?}, got {:?}", expected, actual)
			}
			TrapType::MismatchIndirectCallType(_expected, _actual) => {
				write!(f, "indirect call type mismatch")
			}
		}
	}
}

#[derive(Debug, PartialEq)]
pub enum TrapUnderflow {
	Stack,
}

impl std::fmt::Display for TrapUnderflow {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TrapUnderflow::Stack => write!(f, "stack underflow"),
		}
	}
}
