// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::Diagnostic, fragment::Fragment};

pub fn single_statement_required(message: &str) -> Diagnostic {
	Diagnostic {
		code: "SUBS_001".to_string(),
		rql: None,
		message: message.to_string(),
		fragment: Fragment::None,
		label: Some("expected exactly one statement".to_string()),
		help: Some(
			"send exactly one CREATE SUBSCRIPTION or DROP SUBSCRIPTION statement per request".to_string()
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn invalid_statement(message: &str) -> Diagnostic {
	Diagnostic {
		code: "SUBS_002".to_string(),
		rql: None,
		message: message.to_string(),
		fragment: Fragment::None,
		label: Some("unsupported statement type".to_string()),
		help: Some("use CREATE SUBSCRIPTION or DROP SUBSCRIPTION".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn subscription_missing_as_clause(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "SUBS_003".to_string(),
		rql: None,
		message: "CREATE SUBSCRIPTION requires an AS clause".to_string(),
		fragment,
		label: Some("missing AS clause".to_string()),
		help: Some("provide a query with AS { SELECT ... }".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
