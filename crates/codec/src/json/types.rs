// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::json::wire_type::WireValueType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub op: Option<u8>,
	#[serde(default)]
	pub row_numbers: Vec<u64>,
	#[serde(default)]
	pub created_at: Vec<String>,
	#[serde(default)]
	pub updated_at: Vec<String>,
	#[serde(default)]
	pub time: Vec<String>,
	pub columns: Vec<ResponseColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub name: String,
	#[serde(rename = "type")]
	pub r#type: WireValueType,
	pub payload: Vec<String>,
}
