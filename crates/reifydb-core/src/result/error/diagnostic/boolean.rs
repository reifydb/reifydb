// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{IntoOwnedSpan, result::error::diagnostic::Diagnostic};

pub fn invalid_boolean_format(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"expected 'true' or 'false', found '{}'",
		owned_span.fragment
	));
	Diagnostic {
		code: "BOOLEAN_001".to_string(),
		statement: None,
		message: "invalid boolean format".to_string(),
		span: Some(owned_span),
		label,
		help: Some("use 'true' or 'false'".to_string()),
		notes: vec![
			"valid: true, TRUE".to_string(),
			"valid: false, FALSE".to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn empty_boolean_value(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some("boolean value cannot be empty".to_string());
	Diagnostic {
		code: "BOOLEAN_002".to_string(),
		statement: None,
		message: "empty boolean value".to_string(),
		span: Some(owned_span),
		label,
		help: Some("provide either 'true' or 'false'".to_string()),
		notes: vec![
			"valid: true".to_string(),
			"valid: false".to_string(),
		],
		column: None,
		cause: None,
	}
}

pub fn invalid_number_boolean(span: impl IntoOwnedSpan) -> Diagnostic {
	let owned_span = span.into_span();
	let label = Some(format!(
		"number '{}' cannot be cast to boolean, only 1 or 0 are allowed",
		owned_span.fragment
	));
	Diagnostic {
		code: "BOOLEAN_003".to_string(),
		statement: None,
		message: "invalid boolean".to_string(),
		span: Some(owned_span),
		label,
		help: Some("use 1 for true or 0 for false".to_string()),
		notes: vec![
			"valid: 1 → true".to_string(),
			"valid: 0 → false".to_string(),
			"invalid: any other number".to_string(),
		],
		column: None,
		cause: None,
	}
}
