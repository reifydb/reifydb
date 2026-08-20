// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment};

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

pub fn subscription_operation_unsupported(operation: &str) -> Diagnostic {
	Diagnostic {
		code: "SUBS_004".to_string(),
		rql: None,
		message: format!("operator `{}` is not supported in a subscription", operation),
		fragment: Fragment::None,
		label: Some("unsupported operator in subscription".to_string()),
		help: Some("subscriptions support only filter, gate, map, extend, take, and distinct over a source"
			.to_string()),
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

pub fn hydration_row_cap_exceeded(cap: u64, advice: &str) -> Diagnostic {
	Diagnostic {
		code: "SUBS_006".to_string(),
		rql: None,
		message: format!("subscription hydration exceeds max_rows={}", cap),
		fragment: Fragment::None,
		label: Some("hydration row cap exceeded".to_string()),
		help: Some(advice.to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn subscription_lagged(id: u64, capacity: usize, overrun: u16) -> Diagnostic {
	Diagnostic {
		code: "SUBS_005".to_string(),
		rql: None,
		message: format!(
			"subscription {} overran its {} batch capacity by {} batches and was closed",
			id, capacity, overrun
		),
		fragment: Fragment::None,
		label: Some("the subscriber did not consume changes fast enough".to_string()),
		help: Some(format!(
			"resubscribe to receive a fresh snapshot; the change stream cannot be resumed because the missed batches are gone, and a subscriber that overran by {} needs either a faster consumer or a capacity above {}",
			overrun, capacity
		)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
