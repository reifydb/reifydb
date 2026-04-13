// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;
use serde::{Deserialize, Serialize};

/// A response frame containing query/command results, in the JSON wire shape
/// consumed by both server responses and client deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame {
	#[serde(default)]
	pub row_numbers: Vec<u64>,
	#[serde(default)]
	pub created_at: Vec<String>,
	#[serde(default)]
	pub updated_at: Vec<String>,
	pub columns: Vec<ResponseColumn>,
}

/// A column in a response frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub name: String,
	#[serde(rename = "type")]
	pub r#type: Type,
	pub payload: Vec<String>,
}
