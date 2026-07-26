// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ExportError {
	#[error(
		"text value in {object}.{column} contains both single and double quotes and cannot be represented as an RQL literal"
	)]
	UnrepresentableText {
		object: String,
		column: String,
	},

	#[error("non-finite float value in {object}.{column} cannot be represented as an RQL literal")]
	NonFiniteFloat {
		object: String,
		column: String,
	},

	#[error("value of type {value_type} in {object}.{column} cannot be exported")]
	UnsupportedValue {
		object: String,
		column: String,
		value_type: String,
	},

	#[error("column type {value_type} in {object} cannot be exported")]
	UnsupportedType {
		object: String,
		value_type: String,
	},

	#[error("unresolved {kind} reference with id {id} while rendering {object}")]
	UnresolvedReference {
		kind: &'static str,
		id: u64,
		object: String,
	},
}

#[derive(Debug, PartialEq)]
pub enum RenderError {
	UnrepresentableText,
	NonFiniteFloat,
	Unsupported(&'static str),
}
