// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{Display, Formatter},
	ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize, de, ser};

mod diagnostic;
pub mod r#macro;
pub mod render;
pub mod util;

use render::DefaultRenderer;

use crate::{fragment::Fragment, value::r#type::Type};

/// Entry in the operator call chain for flow operator errors
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorChainEntry {
	pub node_id: u64,
	pub operator_name: String,
	pub operator_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
	pub code: String,
	pub statement: Option<String>,
	pub message: String,
	pub column: Option<DiagnosticColumn>,
	pub fragment: Fragment,
	pub label: Option<String>,
	pub help: Option<String>,
	pub notes: Vec<String>,
	pub cause: Option<Box<Diagnostic>>,
	/// Operator call chain when error occurred (for flow operator errors)
	pub operator_chain: Option<Vec<OperatorChainEntry>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticColumn {
	pub name: String,
	pub r#type: Type,
}

impl Default for Diagnostic {
	fn default() -> Self {
		Self {
			code: String::new(),
			statement: None,
			message: String::new(),
			column: None,
			fragment: Fragment::None,
			label: None,
			help: None,
			notes: Vec::new(),
			cause: None,
			operator_chain: None,
		}
	}
}

impl Display for Diagnostic {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.write_fmt(format_args!("{}", self.code))
	}
}

impl Diagnostic {
	/// Set the statement for this diagnostic and all nested diagnostics
	/// recursively
	pub fn with_statement(&mut self, statement: String) {
		self.statement = Some(statement.clone());

		// Recursively set statement for all nested diagnostics
		if let Some(ref mut cause) = self.cause {
			let mut updated_cause = std::mem::replace(cause.as_mut(), Diagnostic::default());
			updated_cause.with_statement(statement);
			*cause = Box::new(updated_cause);
		}
	}

	/// Set or update the fragment for this diagnostic and all nested
	/// diagnostics recursively
	pub fn with_fragment(&mut self, new_fragment: Fragment) {
		// Always update the fragment, not just when it's None
		// This is needed for cast errors that need to update the
		// fragment
		self.fragment = new_fragment;

		if let Some(ref mut cause) = self.cause {
			cause.with_fragment(self.fragment.clone());
		}
	}

	/// Get the fragment if this is a Statement fragment (for backward
	/// compatibility)
	pub fn fragment(&self) -> Option<Fragment> {
		match &self.fragment {
			Fragment::Statement {
				..
			} => Some(self.fragment.clone()),
			_ => None,
		}
	}
}

/// Trait for converting error types into Diagnostic.
///
/// Implement this trait to provide rich diagnostic information for custom error types.
/// The trait consumes the error (takes `self` by value) to allow moving owned data
/// into the diagnostic.
pub trait IntoDiagnostic {
	/// Convert self into a Diagnostic with error code, message, fragment, and other metadata.
	fn into_diagnostic(self) -> Diagnostic;
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
	Not,
}

impl Display for UnaryOp {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			UnaryOp::Not => f.write_str("NOT"),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
	Add,
	Sub,
	Mul,
	Div,
	Rem,
	Equal,
	NotEqual,
	LessThan,
	LessThanEqual,
	GreaterThan,
	GreaterThanEqual,
	Between,
}

impl BinaryOp {
	pub fn symbol(&self) -> &'static str {
		match self {
			BinaryOp::Add => "+",
			BinaryOp::Sub => "-",
			BinaryOp::Mul => "*",
			BinaryOp::Div => "/",
			BinaryOp::Rem => "%",
			BinaryOp::Equal => "==",
			BinaryOp::NotEqual => "!=",
			BinaryOp::LessThan => "<",
			BinaryOp::LessThanEqual => "<=",
			BinaryOp::GreaterThan => ">",
			BinaryOp::GreaterThanEqual => ">=",
			BinaryOp::Between => "BETWEEN",
		}
	}
}

impl Display for BinaryOp {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.write_str(self.symbol())
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalOp {
	Not,
	And,
	Or,
	Xor,
}

impl Display for LogicalOp {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			LogicalOp::Not => f.write_str("NOT"),
			LogicalOp::And => f.write_str("AND"),
			LogicalOp::Or => f.write_str("OR"),
			LogicalOp::Xor => f.write_str("XOR"),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperandCategory {
	Number,
	Text,
	Temporal,
	Uuid,
}

impl Display for OperandCategory {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			OperandCategory::Number => f.write_str("number"),
			OperandCategory::Text => f.write_str("text"),
			OperandCategory::Temporal => f.write_str("temporal value"),
			OperandCategory::Uuid => f.write_str("UUID"),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintKind {
	Utf8MaxBytes {
		actual: usize,
		max: usize,
	},
	BlobMaxBytes {
		actual: usize,
		max: usize,
	},
	IntMaxBytes {
		actual: usize,
		max: usize,
	},
	UintMaxBytes {
		actual: usize,
		max: usize,
	},
	DecimalPrecision {
		actual: u8,
		max: u8,
	},
	DecimalScale {
		actual: u8,
		max: u8,
	},
	NoneNotAllowed {
		column_type: Type,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum TemporalKind {
	InvalidDateFormat,
	InvalidDateTimeFormat,
	InvalidTimeFormat,
	InvalidDurationFormat,
	InvalidYear,
	InvalidTimeComponentFormat {
		component: String,
	},
	InvalidMonth,
	InvalidDay,
	InvalidHour,
	InvalidMinute,
	InvalidSecond,
	InvalidFractionalSeconds,
	InvalidDateValues,
	InvalidTimeValues,
	InvalidDurationCharacter,
	IncompleteDurationSpecification,
	InvalidUnitInContext {
		unit: char,
		in_time_part: bool,
	},
	InvalidDurationComponentValue {
		unit: char,
	},
	UnrecognizedTemporalPattern,
	EmptyDateComponent,
	EmptyTimeComponent,
	DuplicateDurationComponent {
		component: char,
	},
	OutOfOrderDurationComponent {
		component: char,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlobEncodingKind {
	InvalidHex,
	InvalidBase64,
	InvalidBase64Url,
	InvalidBase58,
	InvalidUtf8Sequence {
		error: String,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstErrorKind {
	TokenizeError {
		message: String,
	},
	UnexpectedEof,
	ExpectedIdentifier,
	InvalidColumnProperty,
	InvalidPolicy,
	UnexpectedToken {
		expected: String,
	},
	UnsupportedToken,
	MultipleExpressionsWithoutBraces,
	UnrecognizedType,
	UnsupportedAstNode {
		node_type: String,
	},
	EmptyPipeline,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureErrorKind {
	UndefinedProcedure {
		name: String,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeErrorKind {
	VariableNotFound {
		name: String,
	},
	VariableIsDataframe {
		name: String,
	},
	VariableIsImmutable {
		name: String,
	},
	BreakOutsideLoop,
	ContinueOutsideLoop,
	MaxIterationsExceeded {
		limit: usize,
	},
	UndefinedFunction {
		name: String,
	},
	FieldNotFound {
		variable: String,
		field: String,
		available: Vec<String>,
	},
	AppendTargetNotFrame {
		name: String,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum NetworkErrorKind {
	Connection {
		message: String,
	},
	Engine {
		message: String,
	},
	Transport {
		message: String,
	},
	Status {
		message: String,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthErrorKind {
	AuthenticationFailed {
		reason: String,
	},
	AuthorizationDenied {
		resource: String,
	},
	TokenExpired,
	InvalidToken,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionErrorKind {
	UnknownFunction,
	ArityMismatch {
		expected: usize,
		actual: usize,
	},
	TooManyArguments {
		max_args: usize,
		actual: usize,
	},
	InvalidArgumentType {
		index: usize,
		expected: Vec<Type>,
		actual: Type,
	},
	UndefinedArgument {
		index: usize,
	},
	MissingInput,
	ExecutionFailed {
		reason: String,
	},
	InternalError {
		details: String,
	},
	GeneratorNotFound,
}

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
	#[error("Cannot apply {operator} operator to {operand_category}")]
	LogicalOperatorNotApplicable {
		operator: LogicalOp,
		operand_category: OperandCategory,
		fragment: Fragment,
	},

	#[error("Cannot apply '{operator}' operator to {left} and {right}")]
	BinaryOperatorNotApplicable {
		operator: BinaryOp,
		left: Type,
		right: Type,
		fragment: Fragment,
	},

	#[error("unsupported cast from {from} to {to}")]
	UnsupportedCast {
		from: Type,
		to: Type,
		fragment: Fragment,
	},

	#[error("failed to cast to {target}")]
	CastToNumberFailed {
		target: Type,
		fragment: Fragment,
		cause: Box<TypeError>,
	},

	#[error("failed to cast to {target}")]
	CastToTemporalFailed {
		target: Type,
		fragment: Fragment,
		cause: Box<TypeError>,
	},

	#[error("failed to cast to bool")]
	CastToBooleanFailed {
		fragment: Fragment,
		cause: Box<TypeError>,
	},

	#[error("failed to cast to {target}")]
	CastToUuidFailed {
		target: Type,
		fragment: Fragment,
		cause: Box<TypeError>,
	},

	#[error("failed to cast BLOB to UTF8")]
	CastBlobToUtf8Failed {
		fragment: Fragment,
		cause: Box<TypeError>,
	},

	#[error("{message}")]
	ConstraintViolation {
		kind: ConstraintKind,
		message: String,
		fragment: Fragment,
	},

	#[error("invalid number format")]
	InvalidNumberFormat {
		target: Type,
		fragment: Fragment,
	},

	#[error("number out of range")]
	NumberOutOfRange {
		target: Type,
		fragment: Fragment,
		descriptor: Option<NumberOutOfRangeDescriptor>,
	},

	#[error("NaN not allowed")]
	NanNotAllowed,

	#[error("too large for precise float conversion")]
	IntegerPrecisionLoss {
		source_type: Type,
		target: Type,
		fragment: Fragment,
	},

	#[error("decimal scale exceeds precision")]
	DecimalScaleExceedsPrecision {
		scale: u8,
		precision: u8,
		fragment: Fragment,
	},

	#[error("invalid decimal precision")]
	DecimalPrecisionInvalid {
		precision: u8,
	},

	#[error("invalid boolean format")]
	InvalidBooleanFormat {
		fragment: Fragment,
	},

	#[error("empty boolean value")]
	EmptyBooleanValue {
		fragment: Fragment,
	},

	#[error("invalid boolean")]
	InvalidNumberBoolean {
		fragment: Fragment,
	},

	#[error("{message}")]
	Temporal {
		kind: TemporalKind,
		message: String,
		fragment: Fragment,
	},

	#[error("invalid UUID v4 format")]
	InvalidUuid4Format {
		fragment: Fragment,
	},

	#[error("invalid UUID v7 format")]
	InvalidUuid7Format {
		fragment: Fragment,
	},

	#[error("{message}")]
	BlobEncoding {
		kind: BlobEncodingKind,
		message: String,
		fragment: Fragment,
	},

	#[error("Serde deserialization error: {message}")]
	SerdeDeserialize {
		message: String,
	},

	#[error("Serde serialization error: {message}")]
	SerdeSerialize {
		message: String,
	},

	#[error("Keycode serialization error: {message}")]
	SerdeKeycode {
		message: String,
	},

	#[error("Array conversion error: {message}")]
	ArrayConversion {
		message: String,
	},

	#[error("UTF-8 conversion error: {message}")]
	Utf8Conversion {
		message: String,
	},

	#[error("Integer conversion error: {message}")]
	IntegerConversion {
		message: String,
	},

	#[error("{message}")]
	Network {
		kind: NetworkErrorKind,
		message: String,
	},

	#[error("{message}")]
	Auth {
		kind: AuthErrorKind,
		message: String,
	},

	#[error("dictionary entry ID {value} exceeds maximum {max_value} for type {id_type}")]
	DictionaryCapacityExceeded {
		id_type: Type,
		value: u128,
		max_value: u128,
	},

	#[error("{message}")]
	AssertionFailed {
		fragment: Fragment,
		message: String,
		expression: Option<String>,
	},

	#[error("{message}")]
	Function {
		kind: FunctionErrorKind,
		message: String,
		fragment: Fragment,
	},

	#[error("{message}")]
	Ast {
		kind: AstErrorKind,
		message: String,
		fragment: Fragment,
	},

	#[error("{message}")]
	Runtime {
		kind: RuntimeErrorKind,
		message: String,
	},

	#[error("{message}")]
	Procedure {
		kind: ProcedureErrorKind,
		message: String,
		fragment: Fragment,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumberOutOfRangeDescriptor {
	pub namespace: Option<String>,
	pub table: Option<String>,
	pub column: Option<String>,
	pub column_type: Option<Type>,
}

impl NumberOutOfRangeDescriptor {
	pub fn location_string(&self) -> String {
		match (self.namespace.as_deref(), self.table.as_deref(), self.column.as_deref()) {
			(Some(s), Some(t), Some(c)) => format!("{}::{}.{}", s, t, c),
			(Some(s), Some(t), None) => format!("{}::{}", s, t),
			(None, Some(t), Some(c)) => format!("{}.{}", t, c),
			(Some(s), None, Some(c)) => format!("{}::{}", s, c),
			(Some(s), None, None) => s.to_string(),
			(None, Some(t), None) => t.to_string(),
			(None, None, Some(c)) => c.to_string(),
			(None, None, None) => "unknown location".to_string(),
		}
	}
}

#[derive(Debug, PartialEq)]
pub struct Error(pub Diagnostic);

impl Deref for Error {
	type Target = Diagnostic;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Error {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl Display for Error {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let out = DefaultRenderer::render_string(&self.0);
		f.write_str(out.as_str())
	}
}

impl Error {
	pub fn diagnostic(self) -> Diagnostic {
		self.0
	}
}

impl std::error::Error for Error {}

impl de::Error for Error {
	fn custom<T: Display>(msg: T) -> Self {
		TypeError::SerdeDeserialize {
			message: msg.to_string(),
		}
		.into()
	}
}

impl ser::Error for Error {
	fn custom<T: Display>(msg: T) -> Self {
		TypeError::SerdeSerialize {
			message: msg.to_string(),
		}
		.into()
	}
}

impl From<std::num::TryFromIntError> for Error {
	fn from(err: std::num::TryFromIntError) -> Self {
		TypeError::IntegerConversion {
			message: err.to_string(),
		}
		.into()
	}
}

impl From<std::array::TryFromSliceError> for Error {
	fn from(err: std::array::TryFromSliceError) -> Self {
		TypeError::ArrayConversion {
			message: err.to_string(),
		}
		.into()
	}
}

impl From<std::string::FromUtf8Error> for Error {
	fn from(err: std::string::FromUtf8Error) -> Self {
		TypeError::Utf8Conversion {
			message: err.to_string(),
		}
		.into()
	}
}

impl From<TypeError> for Error {
	fn from(err: TypeError) -> Self {
		Error(err.into_diagnostic())
	}
}

impl From<std::num::TryFromIntError> for TypeError {
	fn from(err: std::num::TryFromIntError) -> Self {
		TypeError::IntegerConversion {
			message: err.to_string(),
		}
	}
}

impl From<std::array::TryFromSliceError> for TypeError {
	fn from(err: std::array::TryFromSliceError) -> Self {
		TypeError::ArrayConversion {
			message: err.to_string(),
		}
	}
}

impl From<std::string::FromUtf8Error> for TypeError {
	fn from(err: std::string::FromUtf8Error) -> Self {
		TypeError::Utf8Conversion {
			message: err.to_string(),
		}
	}
}

impl de::Error for TypeError {
	fn custom<T: Display>(msg: T) -> Self {
		TypeError::SerdeDeserialize {
			message: msg.to_string(),
		}
	}
}

impl ser::Error for TypeError {
	fn custom<T: Display>(msg: T) -> Self {
		TypeError::SerdeSerialize {
			message: msg.to_string(),
		}
	}
}
