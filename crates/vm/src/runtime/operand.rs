// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Operand stack value types.

use reifydb_core::value::column::{Column, columns::Columns};
use reifydb_type::value::Value;

/// A record is a single row with named fields.
#[derive(Debug, Clone)]
pub struct Record {
	/// Field name -> value pairs.
	pub fields: Vec<(String, Value)>,
}

impl Record {
	/// Create a new record from field name-value pairs.
	pub fn new(fields: Vec<(String, Value)>) -> Self {
		Self {
			fields,
		}
	}

	/// Get a field value by name.
	pub fn get(&self, name: &str) -> Option<&Value> {
		self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
	}
}

/// Handle to a pipeline (can be cloned, represents shared ownership).
#[derive(Debug, Clone)]
pub struct PipelineHandle {
	/// Unique identifier for lookup in pipeline registry.
	pub id: u64,
}

/// Values that can live on the operand stack.
#[derive(Debug, Clone)]
pub enum OperandValue {
	/// Scalar value (literals, computed results).
	Scalar(Value),

	/// Single column (vectorized value for columnar operations).
	Column(Column),

	/// Reference to an expression in the program.
	ExprRef(u16),

	/// Column reference by name.
	ColRef(String),

	/// List of column names (for select).
	ColList(Vec<String>),

	/// Materialized frame (collected pipeline result).
	Frame(Columns),

	/// Reference to a user-defined function.
	FunctionRef(u16),

	/// Pipeline reference (for storing pipelines in variables).
	PipelineRef(PipelineHandle),

	/// Sort specification reference.
	SortSpecRef(u16),

	/// Extension specification reference.
	ExtSpecRef(u16),

	/// Record (single row with named fields).
	Record(Record),
}

impl OperandValue {
	/// Check if this is a scalar value.
	pub fn is_scalar(&self) -> bool {
		matches!(self, OperandValue::Scalar(_))
	}

	/// Check if this is a column value.
	pub fn is_column(&self) -> bool {
		matches!(self, OperandValue::Column(_))
	}

	/// Try to get as a scalar value.
	pub fn as_scalar(&self) -> Option<&Value> {
		match self {
			OperandValue::Scalar(v) => Some(v),
			_ => None,
		}
	}

	/// Try to get as a column.
	pub fn as_column(&self) -> Option<&Column> {
		match self {
			OperandValue::Column(c) => Some(c),
			_ => None,
		}
	}

	/// Try to take as a column (consumes self).
	pub fn into_column(self) -> Option<Column> {
		match self {
			OperandValue::Column(c) => Some(c),
			_ => None,
		}
	}

	/// Try to get as an integer.
	pub fn as_int(&self) -> Option<i64> {
		match self {
			OperandValue::Scalar(Value::Int8(n)) => Some(*n),
			_ => None,
		}
	}

	/// Try to get as a boolean.
	pub fn as_bool(&self) -> Option<bool> {
		match self {
			OperandValue::Scalar(Value::Boolean(b)) => Some(*b),
			_ => None,
		}
	}

	/// Try to get as a string.
	pub fn as_string(&self) -> Option<&str> {
		match self {
			OperandValue::Scalar(Value::Utf8(s)) => Some(s),
			_ => None,
		}
	}
}
