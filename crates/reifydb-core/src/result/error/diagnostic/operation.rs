// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interface::fragment::IntoFragment,
	result::error::diagnostic::Diagnostic,
};

pub fn take_negative_value<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	let value = fragment.value();
	Diagnostic {
		code: "TAKE_001".to_string(),
		statement: None,
		message: format!("TAKE operator requires non-negative value, got {}", value),
		column: None,
		fragment,
		label: Some("negative value not allowed".to_string()),
		help: Some("Use a positive number to limit results, or 0 to return no rows".to_string()),
		notes: vec![
			"TAKE operator limits the number of rows returned from a query".to_string(),
			"Negative values are not meaningful in this context".to_string(),
			"Valid examples: TAKE 10, TAKE 0, TAKE 100".to_string(),
		],
		cause: None,
	}
}
