// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{byte_size::ByteSize, error::Diagnostic, fragment::Fragment};

pub fn memory_limit_exceeded(used: ByteSize, limit: ByteSize) -> Diagnostic {
	Diagnostic {
		code: "QUERY_006".to_string(),
		rql: None,
		message: format!("query exceeded its memory limit of {} (needs at least {})", limit, used),
		fragment: Fragment::None,
		label: Some("this query buffers more data in memory than the per-query limit allows".to_string()),
		help: Some(
			"reduce the amount of data the query materializes (narrow the FILTER, project fewer columns, or add a LIMIT), or raise the query memory limit"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn column_not_found(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "QUERY_001".to_string(),
		rql: None,
		message: "column not found".to_string(),
		fragment,
		label: Some("this column does not exist in the current context".to_string()),
		help: Some("check for typos or ensure the column is defined in the input".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn extend_duplicate_column(fragment: Fragment, column_name: &str) -> Diagnostic {
	Diagnostic {
		code: "EXTEND_002".to_string(),
		rql: None,
		message: format!("Cannot extend with duplicate column name '{}'", column_name),
		fragment,
		label: Some("column already exists in the current frame".to_string()),
		help: Some("Use a different column name or remove the existing column first".to_string()),
		column: None,
		notes: vec![
			"EXTEND operation cannot add columns that already exist in the frame".to_string(),
			"Each column name must be unique within the result frame".to_string(),
			"Consider using MAP if you want to replace existing columns".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn unsupported_source_qualification(fragment: Fragment, name: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_002".to_string(),
		rql: None,
		message: format!("Source qualification '{}' is not supported in RQL expressions", name),
		fragment,
		label: Some("source qualification is only allowed for join aliases in ON clauses".to_string()),
		help: Some("Remove the qualification or use it only with a join alias in the ON clause".to_string()),
		column: None,
		notes: vec![
			"RQL uses a dataframe-centered approach where each operation produces a new dataframe"
				.to_string(),
			"Source qualifications are not needed as columns are unambiguous in the dataframe".to_string(),
			"Only join aliases can be used for qualification within ON clauses to disambiguate columns"
				.to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn system_column_read_only(fragment: Fragment) -> Diagnostic {
	let name = fragment.text();
	Diagnostic {
		code: "QUERY_004".to_string(),
		rql: None,
		message: format!("system column '{}' is read-only", name),
		fragment,
		label: Some("system columns are managed automatically and cannot be set".to_string()),
		help: Some("remove this field from the INSERT/UPDATE values".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn duplicate_field(fragment: Fragment, name: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_009".to_string(),
		rql: None,
		message: format!("field '{}' is given more than once", name),
		fragment,
		label: Some("a row, constructor, MAP or UPDATE may give each field only once".to_string()),
		help: Some("remove one of the values for this field".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn as_clause_not_query(fragment: Fragment, kind: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_005".to_string(),
		rql: None,
		message: format!("AS clause body must be a query, found {}", kind),
		fragment,
		label: Some("expected a query expression here".to_string()),
		help: Some(
			"AS { ... } accepts only query expressions (FROM, MAP, FILTER, JOIN, ...); nested DDL statements such as CREATE/DROP/ALTER are not allowed"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn not_a_query_input(fragment: Fragment, kind: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_010".to_string(),
		rql: None,
		message: format!("{} cannot be the input of a query step", kind),
		fragment,
		label: Some("a query was expected here".to_string()),
		help: Some(format!(
			"a step such as MAP or FILTER reads from a query; run the {} as its own statement",
			kind
		)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn join_column_alias_error(fragment: Fragment, message: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_003".to_string(),
		rql: None,
		message: format!("Join column alias error: {}", message),
		fragment,
		label: Some("invalid column qualification in using clause".to_string()),
		help: Some("In each pair, exactly one expression should reference the join alias".to_string()),
		column: None,
		notes: vec![
			"Example: using (id, orders.user_id) where 'orders' is the join alias".to_string(),
			"Unqualified columns refer to the current dataframe".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn window_requires_deferred_view(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "QUERY_007".to_string(),
		rql: None,
		message: "window runs only in deferred views, not in a batch query".to_string(),
		fragment,
		label: Some("a batch query cannot run a window".to_string()),
		help: Some(
			"define the window in CREATE DEFERRED VIEW ns::name AS { ... } and read the view with FROM ns::name"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn append_requires_deferred_view(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "QUERY_011".to_string(),
		rql: None,
		message: "append runs only in deferred views, not in a batch query".to_string(),
		fragment,
		label: Some("a batch query cannot run an append".to_string()),
		help: Some(
			"define the append in CREATE DEFERRED VIEW ns::name AS { ... } and read the view with FROM ns::name"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn no_column_snapshot(fragment: Fragment, kind: &str, name: &str) -> Diagnostic {
	let help = match kind {
		"series" => format!(
			"wait for the bucket to seal, or read the {} with a regular query",
			kind
		),
		_ => format!(
			"wait for the column store to materialize the {}, or read it with a regular query",
			kind
		),
	};
	Diagnostic {
		code: "QUERY_012".to_string(),
		rql: None,
		message: format!("no column snapshot for {} '{}'", kind, name),
		fragment,
		label: Some(format!("this {} has not been materialized into the column store yet", kind)),
		help: Some(help),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn unsupported_in_column_layout(what: &str) -> Diagnostic {
	Diagnostic {
		code: "QUERY_013".to_string(),
		rql: None,
		message: format!("{} cannot be read from the column store", what),
		fragment: Fragment::None,
		label: Some("a column query reads only materialized table snapshots".to_string()),
		help: Some("run this query with a regular query instead".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn unknown_apply_operator(fragment: Fragment) -> Diagnostic {
	let name = fragment.text().to_string();
	Diagnostic {
		code: "QUERY_008".to_string(),
		rql: None,
		message: format!("unknown operator '{}'", name),
		fragment,
		label: Some("no operator with this name is registered".to_string()),
		help: Some("check the operator name for typos, or register the operator before applying it".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn memory_limit_exceeded_has_stable_code_and_fields() {
		let d = memory_limit_exceeded(ByteSize::from_kib(5), ByteSize::from_kib(4));
		assert_eq!(d.code, "QUERY_006");
		assert!(matches!(d.fragment, Fragment::None));
		assert!(d.message.contains("exceeded its memory limit of 4 KiB"), "unexpected message: {}", d.message);
		assert!(d.message.contains("needs at least 5 KiB"), "unexpected message: {}", d.message);
		assert!(d.label.unwrap().contains("buffers more data in memory"));
		assert!(d.help.unwrap().contains("raise the query memory limit"));
	}
}
