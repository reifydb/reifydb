// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use core::fmt;

use reifydb_rqlv2::expression::types::EvalError;
use reifydb_type::value::r#type::Type;

#[derive(Debug, Clone)]
pub enum VmError {
	// Expression evaluation errors
	TypeMismatch {
		expected: Type,
		found: Type,
		context: String,
	},

	ColumnNotFound {
		name: String,
	},

	ColumnIndexOutOfBounds {
		index: usize,
		count: usize,
	},

	DivisionByZero,

	UnexpectedNull,

	// Operator errors
	EmptyPipeline,

	RowCountMismatch {
		expected: usize,
		actual: usize,
	},

	// Storage errors
	Storage(String),

	// Internal errors
	Internal(String),

	// ─────────────────────────────────────────────────────────────
	// Bytecode VM Errors
	// ─────────────────────────────────────────────────────────────

	// Bytecode errors
	InvalidBytecode {
		position: usize,
	},

	UnexpectedEndOfBytecode,

	UnknownOpcode {
		opcode: u8,
	},

	UnknownOperatorKind {
		kind: u8,
	},

	// Stack errors
	StackOverflow {
		stack: String,
	},

	StackUnderflow {
		stack: String,
	},

	// Index errors
	InvalidConstantIndex {
		index: u16,
	},

	InvalidExpressionIndex {
		index: u16,
	},

	InvalidSourceIndex {
		index: u16,
	},

	InvalidFunctionIndex {
		index: u16,
	},

	InvalidColumnListIndex {
		index: u16,
	},

	InvalidSortSpecIndex {
		index: u16,
	},

	InvalidExtSpecIndex {
		index: u16,
	},

	// Variable errors
	UndefinedVariable {
		name: String,
	},

	// Table errors
	TableNotFound {
		name: String,
	},

	NamespaceNotFound {
		name: String,
	},

	CatalogError {
		message: String,
	},

	InvalidDdlDefIndex {
		index: u16,
	},

	InvalidDmlTargetIndex {
		index: u16,
	},

	TransactionRequired,

	UnexpectedDdlType {
		expected: String,
		found: String,
	},

	// Function errors
	ReturnOutsideFunction,

	// Type errors
	ExpectedString {
		index: u16,
	},

	ExpectedBoolean,

	ExpectedExpression,

	ExpectedColumnList,

	ExpectedInteger,

	ExpectedSortSpec,

	ExpectedExtensionSpec,

	ExpectedPipeline,

	ExpectedFrame,

	ExpectedRecord,

	InvalidPipelineHandle,

	// Operation errors
	UnsupportedOperation {
		operation: String,
	},

	FieldNotFound {
		field: String,
		record: String,
	},

	// ─────────────────────────────────────────────────────────────
	// Compile Errors
	// ─────────────────────────────────────────────────────────────
	UndefinedFunction {
		name: String,
	},

	WrongArgumentCount {
		name: String,
		expected: usize,
		got: usize,
	},

	DuplicateFunction {
		name: String,
	},

	CompileError {
		message: String,
	},

	// ─────────────────────────────────────────────────────────────
	// Subquery Errors
	// ─────────────────────────────────────────────────────────────
	SubqueryExecutorNotAvailable,

	InvalidSubqueryIndex {
		index: u16,
	},

	SubqueryNoColumns,

	SubqueryWrongColumnCount {
		expected: usize,
		got: usize,
	},

	ScalarSubqueryTooManyRows {
		count: usize,
	},

	SubqueryMultipleRows {
		expected: usize,
		found: usize,
	},

	NoTransactionAvailable,

	TypeMismatchStr {
		expected: String,
		found: String,
	},
}

impl fmt::Display for VmError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::TypeMismatch {
				expected,
				found,
				context,
			} => {
				write!(f, "type mismatch: expected {expected}, found {found} in {context}")
			}
			Self::ColumnNotFound {
				name,
			} => write!(f, "column not found: {name}"),
			Self::ColumnIndexOutOfBounds {
				index,
				count,
			} => {
				write!(f, "column index out of bounds: {index} (have {count} columns)")
			}
			Self::DivisionByZero => write!(f, "division by zero"),
			Self::UnexpectedNull => write!(f, "null value in non-nullable context"),
			Self::EmptyPipeline => write!(f, "empty pipeline"),
			Self::RowCountMismatch {
				expected,
				actual,
			} => {
				write!(f, "row count mismatch: expected {expected}, got {actual}")
			}
			Self::Storage(msg) => write!(f, "storage error: {msg}"),
			Self::Internal(msg) => write!(f, "internal error: {msg}"),
			Self::InvalidBytecode {
				position,
			} => {
				write!(f, "invalid bytecode at position {position}")
			}
			Self::UnexpectedEndOfBytecode => write!(f, "unexpected end of bytecode"),
			Self::UnknownOpcode {
				opcode,
			} => write!(f, "unknown opcode: 0x{opcode:02X}"),
			Self::UnknownOperatorKind {
				kind,
			} => write!(f, "unknown operator kind: {kind}"),
			Self::StackOverflow {
				stack,
			} => write!(f, "{stack} stack overflow"),
			Self::StackUnderflow {
				stack,
			} => write!(f, "{stack} stack underflow"),
			Self::InvalidConstantIndex {
				index,
			} => write!(f, "invalid constant index: {index}"),
			Self::InvalidExpressionIndex {
				index,
			} => {
				write!(f, "invalid expression index: {index}")
			}
			Self::InvalidSourceIndex {
				index,
			} => write!(f, "invalid source index: {index}"),
			Self::InvalidFunctionIndex {
				index,
			} => write!(f, "invalid function index: {index}"),
			Self::InvalidColumnListIndex {
				index,
			} => {
				write!(f, "invalid column list index: {index}")
			}
			Self::InvalidSortSpecIndex {
				index,
			} => write!(f, "invalid sort spec index: {index}"),
			Self::InvalidExtSpecIndex {
				index,
			} => {
				write!(f, "invalid extension spec index: {index}")
			}
			Self::UndefinedVariable {
				name,
			} => write!(f, "undefined variable: {name}"),
			Self::TableNotFound {
				name,
			} => write!(f, "table not found: {name}"),
			Self::NamespaceNotFound {
				name,
			} => write!(f, "namespace not found: {name}"),
			Self::CatalogError {
				message,
			} => write!(f, "catalog error: {message}"),
			Self::InvalidDdlDefIndex {
				index,
			} => {
				write!(f, "invalid DDL definition index: {index}")
			}
			Self::InvalidDmlTargetIndex {
				index,
			} => {
				write!(f, "invalid DML target index: {index}")
			}
			Self::TransactionRequired => write!(f, "transaction required for this operation"),
			Self::UnexpectedDdlType {
				expected,
				found,
			} => {
				write!(f, "unexpected DDL type: expected {expected}, found {found}")
			}
			Self::ReturnOutsideFunction => write!(f, "return outside of function"),
			Self::ExpectedString {
				index,
			} => {
				write!(f, "expected string at constant index {index}")
			}
			Self::ExpectedBoolean => write!(f, "expected boolean value"),
			Self::ExpectedExpression => write!(f, "expected expression reference"),
			Self::ExpectedColumnList => write!(f, "expected column list"),
			Self::ExpectedInteger => write!(f, "expected integer value"),
			Self::ExpectedSortSpec => write!(f, "expected sort specification"),
			Self::ExpectedExtensionSpec => write!(f, "expected extension specification"),
			Self::ExpectedPipeline => write!(f, "expected pipeline"),
			Self::ExpectedFrame => write!(f, "expected frame"),
			Self::ExpectedRecord => write!(f, "expected record"),
			Self::InvalidPipelineHandle => write!(f, "invalid pipeline handle"),
			Self::UnsupportedOperation {
				operation,
			} => {
				write!(f, "unsupported operation: {operation}")
			}
			Self::FieldNotFound {
				field,
				record,
			} => {
				write!(f, "field '{field}' not found in record '{record}'")
			}
			Self::UndefinedFunction {
				name,
			} => write!(f, "undefined function: {name}"),
			Self::WrongArgumentCount {
				name,
				expected,
				got,
			} => {
				write!(f, "wrong number of arguments for '{name}': expected {expected}, got {got}")
			}
			Self::DuplicateFunction {
				name,
			} => write!(f, "duplicate function definition: {name}"),
			Self::CompileError {
				message,
			} => write!(f, "compile error: {message}"),
			Self::SubqueryExecutorNotAvailable => write!(f, "subquery executor not available"),
			Self::InvalidSubqueryIndex {
				index,
			} => write!(f, "invalid subquery index: {index}"),
			Self::SubqueryNoColumns => write!(f, "subquery returned no columns"),
			Self::SubqueryWrongColumnCount {
				expected,
				got,
			} => {
				write!(f, "subquery returned {got} columns, expected {expected}")
			}
			Self::ScalarSubqueryTooManyRows {
				count,
			} => {
				write!(f, "scalar subquery returned {count} rows (expected 0 or 1)")
			}
			Self::SubqueryMultipleRows {
				expected,
				found,
			} => {
				write!(f, "subquery returned {found} rows, expected at most {expected}")
			}
			Self::NoTransactionAvailable => {
				write!(f, "no transaction available for subquery execution")
			}
			Self::TypeMismatchStr {
				expected,
				found,
			} => {
				write!(f, "type mismatch: expected {expected}, found {found}")
			}
		}
	}
}

impl std::error::Error for VmError {}

pub type Result<T> = std::result::Result<T, VmError>;

// Conversion from RQLv2's EvalError to VmError
impl From<EvalError> for VmError {
	fn from(err: EvalError) -> Self {
		match err {
			EvalError::ColumnNotFound {
				name,
			} => VmError::ColumnNotFound {
				name,
			},
			EvalError::VariableNotFound {
				id,
			} => VmError::UndefinedVariable {
				name: format!("variable ID {}", id),
			},
			EvalError::TypeMismatch {
				expected,
				found,
				context,
			} => VmError::CompileError {
				message: format!(
					"type mismatch in {}: expected {}, found {}",
					context, expected, found
				),
			},
			EvalError::DivisionByZero => VmError::DivisionByZero,
			EvalError::RowCountMismatch {
				expected,
				actual,
			} => VmError::RowCountMismatch {
				expected,
				actual,
			},
			EvalError::UnsupportedOperation {
				operation,
			} => VmError::UnsupportedOperation {
				operation,
			},
			EvalError::SubqueryError {
				message,
			} => VmError::Internal(message),
		}
	}
}
