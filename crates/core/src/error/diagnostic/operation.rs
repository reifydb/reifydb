// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment, value::value_type::ValueType};

pub fn aggregate_group_by_not_column(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_007".to_string(),
		rql: None,
		message: "AGGREGATE BY only accepts column references".to_string(),
		column: None,
		fragment,
		label: Some("not a column".to_string()),
		help: Some("Compute the key first with EXTEND, then group by that column, e.g., 'EXTEND {k: g + 1} | AGGREGATE {count(id)} BY {k}'".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_group_by_unkeyable(fragment: Fragment, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_008".to_string(),
		rql: None,
		message: format!("AGGREGATE BY cannot group by a column of type {}", ty),
		column: None,
		fragment,
		label: Some("column cannot be a group key".to_string()),
		help: Some("Group by a scalar column instead".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_map_without_aggregate(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_009".to_string(),
		rql: None,
		message: "AGGREGATE map entries must be built from aggregate function calls".to_string(),
		column: None,
		fragment,
		label: Some("no aggregate function here".to_string()),
		help: Some(
			"Wrap the value in an aggregate function, e.g., 'AGGREGATE {total: math::sum(a) + 1} BY {g}'"
				.to_string(),
		),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn distinct_key_unkeyable(fragment: Fragment, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "DISTINCT_001".to_string(),
		rql: None,
		message: format!("DISTINCT cannot compare rows by a column of type {}", ty),
		column: None,
		fragment,
		label: Some("column cannot be a distinct key".to_string()),
		help: Some("Remove the column from DISTINCT, or apply DISTINCT before the column is added".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn join_key_unkeyable(fragment: Fragment, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "JOIN_001".to_string(),
		rql: None,
		message: format!("JOIN cannot match rows by a column of type {}", ty),
		column: None,
		fragment,
		label: Some("column cannot be a join key".to_string()),
		help: Some("Join on a scalar column instead".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn natural_join_no_shared_column(fragment: Fragment, left: &str, right: &str) -> Diagnostic {
	Diagnostic {
		code: "JOIN_002".to_string(),
		rql: None,
		message: format!("NATURAL JOIN of {} with {} has no shared column", left, right),
		column: None,
		fragment,
		label: Some("no column name appears on both sides".to_string()),
		help: Some("Rename a column so both sides share it, or join with USING".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn sort_key_not_orderable(fragment: Fragment, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "SORT_002".to_string(),
		rql: None,
		message: format!("SORT cannot order by a column of type {}", ty),
		column: None,
		fragment,
		label: Some("column is not orderable".to_string()),
		help: Some("Sort by a scalar column instead".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}
