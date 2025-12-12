// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! WebSocket response types.

use reifydb_type::Type;
use serde::{Deserialize, Serialize};

/// A response frame containing query/command results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<ResponseColumn>,
}

/// A column in a response frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub namespace: Option<String>,
	pub store: Option<String>,
	pub name: String,
	#[serde(rename = "type")]
	pub r#type: Type,
	pub data: Vec<String>,
}
