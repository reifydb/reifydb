// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{error::diagnostic::Diagnostic, fragment::Fragment};

pub fn assertion_failed(fragment: Fragment, message: Option<String>, expression: Option<String>) -> Diagnostic {
	let base_msg = match (&message, &expression) {
		(Some(msg), _) => msg.clone(),
		(None, Some(expr)) => format!("assertion failed: {}", expr),
		(None, None) => "assertion failed".to_string(),
	};

	let label = expression
		.as_ref()
		.map(|expr| format!("this expression is false: {}", expr))
		.or_else(|| Some("assertion failed".to_string()));

	Diagnostic {
		code: "ASSERT".to_string(),
		statement: None,
		message: base_msg,
		fragment,
		label,
		help: None,
		notes: vec![],
		column: None,
		cause: None,
		operator_chain: None,
	}
}
