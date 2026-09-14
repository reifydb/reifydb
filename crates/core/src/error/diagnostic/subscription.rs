// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment, value::duration::Duration};

pub fn single_statement_required(message: &str) -> Diagnostic {
	Diagnostic {
		code: "SUBS_001".to_string(),
		rql: None,
		message: message.to_string(),
		fragment: Fragment::None,
		label: Some("expected exactly one statement".to_string()),
		help: Some("send exactly one query per subscribe request".to_string()),
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

pub fn hydration_max_rows_zero() -> Diagnostic {
	Diagnostic {
		code: "SUBS_007".to_string(),
		rql: None,
		message: "hydration.max_rows must be greater than zero (use enabled: false to disable)".to_string(),
		fragment: Fragment::None,
		label: Some("expected a positive integer".to_string()),
		help: Some("set hydration.max_rows above zero, or set hydration.enabled to false".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn negative_throttle(throttle: Duration) -> Diagnostic {
	Diagnostic {
		code: "SUBS_008".to_string(),
		rql: None,
		message: format!("expected a non-negative duration for 'throttle', found `{}`", throttle),
		fragment: Fragment::None,
		label: Some("expected a non-negative duration".to_string()),
		help: Some("set throttle to zero or a positive duration".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn negative_linger(linger: Duration) -> Diagnostic {
	Diagnostic {
		code: "SUBS_009".to_string(),
		rql: None,
		message: format!("expected a non-negative duration for 'linger', found `{}`", linger),
		fragment: Fragment::None,
		label: Some("expected a non-negative duration".to_string()),
		help: Some("set linger to zero or a positive duration".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn single_query_required() -> Diagnostic {
	Diagnostic {
		code: "SUBS_010".to_string(),
		rql: None,
		message: "a subscription must be a single query".to_string(),
		fragment: Fragment::None,
		label: Some("expected a query expression here".to_string()),
		help: Some("subscribe to one query expression (FROM, MAP, FILTER, ...); DDL, DML and statements such as LET are not allowed"
			.to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
