// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::Diagnostic;
use crate::{OwnedFragment, Type};

/// DICT_001: Dictionary entry ID exceeds maximum capacity for the configured type
pub fn dictionary_entry_id_capacity_exceeded(id_type: Type, value: u128, max_value: u128) -> Diagnostic {
	Diagnostic {
		code: "DICT_001".to_string(),
		statement: None,
		message: format!("dictionary entry ID {} exceeds maximum {} for type {}", value, max_value, id_type),
		column: None,
		fragment: OwnedFragment::None,
		label: Some(format!("{} capacity exceeded", id_type)),
		help: Some(
			"use a larger ID type (e.g., Uint2 instead of Uint1) when creating the dictionary".to_string()
		),
		notes: vec![],
		cause: None,
	}
}
