// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_rqlv2::expression::EvalError;
use reifydb_type::Type;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum VmError {
	// Expression evaluation errors
	#[error("type mismatch: expected {expected}, found {found} in {context}")]
	TypeMismatch {
		expected: Type,
		found: Type,
		context: String,
	},

	#[error("column not found: {name}")]
	ColumnNotFound {
		name: String,
	},

	#[error("column index out of bounds: {index} (have {count} columns)")]
	ColumnIndexOutOfBounds {
		index: usize,
		count: usize,
	},

	#[error("division by zero")]
	DivisionByZero,

	#[error("null value in non-nullable context")]
	UnexpectedNull,

	// Operator errors
	#[error("empty pipeline")]
	EmptyPipeline,

	#[error("row count mismatch: expected {expected}, got {actual}")]
	RowCountMismatch {
		expected: usize,
		actual: usize,
	},

	// Storage errors
	#[error("storage error: {0}")]
	Storage(String),

	// Internal errors
	#[error("internal error: {0}")]
	Internal(String),

	// ─────────────────────────────────────────────────────────────
	// Bytecode VM Errors
	// ─────────────────────────────────────────────────────────────

	// Bytecode errors
	#[error("invalid bytecode at position {position}")]
	InvalidBytecode {
		position: usize,
	},

	#[error("unexpected end of bytecode")]
	UnexpectedEndOfBytecode,

	#[error("unknown opcode: 0x{opcode:02X}")]
	UnknownOpcode {
		opcode: u8,
	},

	#[error("unknown operator kind: {kind}")]
	UnknownOperatorKind {
		kind: u8,
	},

	// Stack errors
	#[error("{stack} stack overflow")]
	StackOverflow {
		stack: String,
	},

	#[error("{stack} stack underflow")]
	StackUnderflow {
		stack: String,
	},

	// Index errors
	#[error("invalid constant index: {index}")]
	InvalidConstantIndex {
		index: u16,
	},

	#[error("invalid expression index: {index}")]
	InvalidExpressionIndex {
		index: u16,
	},

	#[error("invalid source index: {index}")]
	InvalidSourceIndex {
		index: u16,
	},

	#[error("invalid function index: {index}")]
	InvalidFunctionIndex {
		index: u16,
	},

	#[error("invalid column list index: {index}")]
	InvalidColumnListIndex {
		index: u16,
	},

	#[error("invalid sort spec index: {index}")]
	InvalidSortSpecIndex {
		index: u16,
	},

	#[error("invalid extension spec index: {index}")]
	InvalidExtSpecIndex {
		index: u16,
	},

	// Variable errors
	#[error("undefined variable: {name}")]
	UndefinedVariable {
		name: String,
	},

	// Table errors
	#[error("table not found: {name}")]
	TableNotFound {
		name: String,
	},

	#[error("namespace not found: {name}")]
	NamespaceNotFound {
		name: String,
	},

	#[error("catalog error: {message}")]
	CatalogError {
		message: String,
	},

	#[error("invalid DDL definition index: {index}")]
	InvalidDdlDefIndex {
		index: u16,
	},

	#[error("invalid DML target index: {index}")]
	InvalidDmlTargetIndex {
		index: u16,
	},

	#[error("transaction required for this operation")]
	TransactionRequired,

	#[error("unexpected DDL type: expected {expected}, found {found}")]
	UnexpectedDdlType {
		expected: String,
		found: String,
	},

	// Function errors
	#[error("return outside of function")]
	ReturnOutsideFunction,

	// Type errors
	#[error("expected string at constant index {index}")]
	ExpectedString {
		index: u16,
	},

	#[error("expected boolean value")]
	ExpectedBoolean,

	#[error("expected expression reference")]
	ExpectedExpression,

	#[error("expected column list")]
	ExpectedColumnList,

	#[error("expected integer value")]
	ExpectedInteger,

	#[error("expected sort specification")]
	ExpectedSortSpec,

	#[error("expected extension specification")]
	ExpectedExtensionSpec,

	#[error("expected pipeline")]
	ExpectedPipeline,

	#[error("expected frame")]
	ExpectedFrame,

	#[error("expected record")]
	ExpectedRecord,

	#[error("invalid pipeline handle")]
	InvalidPipelineHandle,

	// Operation errors
	#[error("unsupported operation: {operation}")]
	UnsupportedOperation {
		operation: String,
	},

	#[error("field '{field}' not found in record '{record}'")]
	FieldNotFound {
		field: String,
		record: String,
	},

	// ─────────────────────────────────────────────────────────────
	// Compile Errors
	// ─────────────────────────────────────────────────────────────
	#[error("undefined function: {name}")]
	UndefinedFunction {
		name: String,
	},

	#[error("wrong number of arguments for '{name}': expected {expected}, got {got}")]
	WrongArgumentCount {
		name: String,
		expected: usize,
		got: usize,
	},

	#[error("duplicate function definition: {name}")]
	DuplicateFunction {
		name: String,
	},

	#[error("compile error: {message}")]
	CompileError {
		message: String,
	},

	// ─────────────────────────────────────────────────────────────
	// Subquery Errors
	// ─────────────────────────────────────────────────────────────
	#[error("subquery executor not available")]
	SubqueryExecutorNotAvailable,

	#[error("invalid subquery index: {index}")]
	InvalidSubqueryIndex {
		index: u16,
	},

	#[error("subquery returned no columns")]
	SubqueryNoColumns,

	#[error("subquery returned {got} columns, expected {expected}")]
	SubqueryWrongColumnCount {
		expected: usize,
		got: usize,
	},

	#[error("scalar subquery returned {count} rows (expected 0 or 1)")]
	ScalarSubqueryTooManyRows {
		count: usize,
	},

	#[error("subquery returned {found} rows, expected at most {expected}")]
	SubqueryMultipleRows {
		expected: usize,
		found: usize,
	},

	#[error("no transaction available for subquery execution")]
	NoTransactionAvailable,

	#[error("type mismatch: expected {expected}, found {found}")]
	TypeMismatchStr {
		expected: String,
		found: String,
	},
}

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
