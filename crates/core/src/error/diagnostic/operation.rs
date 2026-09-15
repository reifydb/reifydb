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

pub fn aggregate_argument_not_literal(fragment: Fragment, function: &str, position: usize) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_010".to_string(),
		rql: None,
		message: format!("argument {} of aggregate function {} must be a literal", position, function),
		column: None,
		fragment,
		label: Some("not a literal".to_string()),
		help: Some("Write the value directly, e.g., '0.01', instead of a column or an expression".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_accuracy_out_of_range(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_011".to_string(),
		rql: None,
		message: "accuracy must be between 0.001 and 0.1".to_string(),
		column: None,
		fragment,
		label: Some("accuracy out of range".to_string()),
		help: Some("Write an accuracy from 0.001 to 0.1, e.g., '0.01'".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_accuracy_not_whole_ppm(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_012".to_string(),
		rql: None,
		message: "accuracy must be a whole number of parts per million".to_string(),
		column: None,
		fragment,
		label: Some("accuracy has too many decimal places".to_string()),
		help: Some("Write the accuracy with at most six decimal places, e.g., '0.0125'".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_accuracy_not_a_number(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_013".to_string(),
		rql: None,
		message: "accuracy must be a number".to_string(),
		column: None,
		fragment,
		label: Some("not a number".to_string()),
		help: Some("Write the accuracy as a number literal, e.g., '0.01'".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_accuracy_required(fragment: Fragment, function: &str) -> Diagnostic {
	let example = match function {
		"stats::approx_percentile" => "stats::approx_percentile(latency, 0.99, 0.01)",
		_ => "stats::digest(latency, 0.01)",
	};
	Diagnostic {
		code: "AGGREGATE_014".to_string(),
		rql: None,
		message: "accuracy is required for a non-digest input".to_string(),
		column: None,
		fragment,
		label: Some("missing accuracy".to_string()),
		help: Some(format!("Add an accuracy argument, e.g., '{}'", example)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_accuracy_from_digest_type(fragment: Fragment, function: &str) -> Diagnostic {
	let example = match function {
		"stats::approx_percentile" => "stats::approx_percentile(lat, 0.99)",
		_ => "stats::digest(lat)",
	};
	Diagnostic {
		code: "AGGREGATE_015".to_string(),
		rql: None,
		message: "accuracy comes from the digest type, remove the argument".to_string(),
		column: None,
		fragment,
		label: Some("accuracy given for a digest input".to_string()),
		help: Some(format!("Merge digests without an accuracy, e.g., '{}'", example)),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_digest_unsupported_input(fragment: Fragment, function: &str, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_016".to_string(),
		rql: None,
		message: format!("{} not supported for {}", function, ty),
		column: None,
		fragment,
		label: Some("unsupported input type".to_string()),
		help: Some("Use an int, float or duration input; other types have no natural zero or no buckets yet"
			.to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_digest_merge_mismatch(fragment: Fragment, left: ValueType, right: ValueType) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_017".to_string(),
		rql: None,
		message: format!("cannot merge {} with {}", left, right),
		column: None,
		fragment,
		label: Some("digest types differ".to_string()),
		help: Some("Merge only digests built with the same input type and accuracy".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_digest_input_mismatch(fragment: Fragment, expected: ValueType, actual: ValueType) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_018".to_string(),
		rql: None,
		message: format!("digest of {} cannot take a {} value", expected, actual),
		column: None,
		fragment,
		label: Some("input type changed".to_string()),
		help: Some("Cast the input to one type before building a digest".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_digest_month_part(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_019".to_string(),
		rql: None,
		message: "digest input cannot have a month part".to_string(),
		column: None,
		fragment,
		label: Some("duration has a month part".to_string()),
		help: Some("Express the duration in days instead of months".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_percentile_out_of_range(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_020".to_string(),
		rql: None,
		message: "p must be between 0 and 1".to_string(),
		column: None,
		fragment,
		label: Some("p out of range".to_string()),
		help: Some("Write p from 0 to 1, e.g., '0.99' for the 99th percentile".to_string()),
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn aggregate_percentile_not_a_number(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "AGGREGATE_021".to_string(),
		rql: None,
		message: "p must be a number".to_string(),
		column: None,
		fragment,
		label: Some("not a number".to_string()),
		help: Some("Write p as a number literal, e.g., '0.99'".to_string()),
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

pub fn join_pick_column_not_found(fragment: Fragment, right: &str) -> Diagnostic {
	Diagnostic {
		code: "JOIN_003".to_string(),
		rql: None,
		message: format!("JOIN pick orders by column {}, which {} does not carry", fragment.text(), right),
		column: None,
		fragment,
		label: Some("column is not on the right side".to_string()),
		help: Some("Pick by a column the right side carries".to_string()),
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
