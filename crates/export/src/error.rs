// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ExportError {
	#[error(
		"text value in {shape}.{column} contains both single and double quotes and cannot be represented as an RQL literal"
	)]
	UnrepresentableText {
		shape: String,
		column: String,
	},

	#[error("non-finite float value in {shape}.{column} cannot be represented as an RQL literal")]
	NonFiniteFloat {
		shape: String,
		column: String,
	},

	#[error("value of type {value_type} in {shape}.{column} cannot be exported")]
	UnsupportedValue {
		shape: String,
		column: String,
		value_type: String,
	},

	#[error("column type {value_type} in {shape} cannot be exported")]
	UnsupportedType {
		shape: String,
		value_type: String,
	},

	#[error("unresolved {kind} reference with id {id} while rendering {shape}")]
	UnresolvedReference {
		kind: &'static str,
		id: u64,
		shape: String,
	},
}

#[derive(Debug, PartialEq)]
pub enum RenderError {
	UnrepresentableText,
	NonFiniteFloat,
	Unsupported(&'static str),
}
