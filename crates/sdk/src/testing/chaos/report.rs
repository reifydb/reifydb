// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Write,
};

use reifydb_type::value::Value;

use super::oracle::{MaterializedRow, MaterializedTable, OutputKey};

#[derive(Debug, Clone, Default)]
pub struct Tolerances(pub HashMap<String, f64>);

impl Tolerances {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with(mut self, column: impl Into<String>, tol: f64) -> Self {
		self.0.insert(column.into(), tol);
		self
	}

	pub fn get(&self, column: &str) -> Option<f64> {
		self.0.get(column).copied()
	}
}

#[derive(Debug, Clone)]
pub struct ColumnDiff {
	pub column: String,
	pub oracle_value: Option<Value>,
	pub operator_value: Option<Value>,

	pub tolerance: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DivergentRow {
	pub key: OutputKey,
	pub column_diffs: Vec<ColumnDiff>,
}

#[derive(Debug, Clone, Default)]
pub struct ComparisonResult {
	pub only_in_oracle: Vec<OutputKey>,
	pub only_in_operator: Vec<OutputKey>,
	pub divergent: Vec<DivergentRow>,
}

impl ComparisonResult {
	pub fn is_match(&self) -> bool {
		self.only_in_oracle.is_empty() && self.only_in_operator.is_empty() && self.divergent.is_empty()
	}

	pub fn format_failure(&self, header_lines: &[String], max_divergent_shown: usize) -> String {
		let mut out = String::new();
		for line in header_lines {
			let _ = writeln!(out, "{line}");
		}
		let _ = writeln!(out, "ChaosOutcome divergence summary:");
		let _ = writeln!(out, "    only_in_oracle:    {}", self.only_in_oracle.len());
		let _ = writeln!(out, "    only_in_operator:  {}", self.only_in_operator.len());
		let _ = writeln!(
			out,
			"    divergent:         {} (showing first {})",
			self.divergent.len(),
			max_divergent_shown
		);

		for (idx, dr) in self.divergent.iter().take(max_divergent_shown).enumerate() {
			let _ = writeln!(out);
			let _ = writeln!(
				out,
				"  --- divergence #{} -------------------------------------------------------",
				idx + 1
			);
			let _ = writeln!(out, "  key: {}", format_output_key(&dr.key));
			out.push_str(&render_divergence_table(&dr.column_diffs));
		}

		if !self.only_in_oracle.is_empty() {
			let _ = writeln!(out);
			let _ = writeln!(out, "  --- keys present in oracle but missing in operator (first 10) ---");
			for k in self.only_in_oracle.iter().take(10) {
				let _ = writeln!(out, "    {}", format_output_key(k));
			}
		}
		if !self.only_in_operator.is_empty() {
			let _ = writeln!(out);
			let _ = writeln!(out, "  --- keys present in operator but missing in oracle (first 10) ---");
			for k in self.only_in_operator.iter().take(10) {
				let _ = writeln!(out, "    {}", format_output_key(k));
			}
		}
		out
	}
}

const COL_NAME_CAP: usize = 24;
const VALUE_CAP: usize = 30;
const TOL_CAP: usize = 12;

fn render_divergence_table(diffs: &[ColumnDiff]) -> String {
	let headers: [&str; 5] = ["column", "oracle", "operator", "tol", "diff"];
	let caps: [usize; 5] = [COL_NAME_CAP, VALUE_CAP, VALUE_CAP, TOL_CAP, VALUE_CAP];

	let rows: Vec<[String; 5]> = diffs
		.iter()
		.map(|cd| {
			[
				truncate_to(cd.column.clone(), caps[0]),
				truncate_to(format_value_opt(&cd.oracle_value), caps[1]),
				truncate_to(format_value_opt(&cd.operator_value), caps[2]),
				truncate_to(format_tolerance(cd.tolerance), caps[3]),
				truncate_to(format_diff(&cd.oracle_value, &cd.operator_value), caps[4]),
			]
		})
		.collect();

	let widths = compute_widths(&headers, &rows);
	let separator = build_separator(&widths);
	let header_row = build_row(&headers.map(|s| s.to_string()), &widths);

	let mut out = String::new();
	let _ = writeln!(out, "{separator}");
	let _ = writeln!(out, "{header_row}");
	let _ = writeln!(out, "{separator}");
	for r in &rows {
		let _ = writeln!(out, "{}", build_row(r, &widths));
	}
	let _ = writeln!(out, "{separator}");
	out
}

fn compute_widths(headers: &[&str; 5], rows: &[[String; 5]]) -> [usize; 5] {
	let mut widths = [0usize; 5];
	for (i, h) in headers.iter().enumerate() {
		widths[i] = widths[i].max(h.chars().count());
	}
	for r in rows {
		for (i, cell) in r.iter().enumerate() {
			widths[i] = widths[i].max(cell.chars().count());
		}
	}
	widths
}

fn build_separator(widths: &[usize; 5]) -> String {
	let mut s = String::from("+");
	for w in widths {
		s.push_str(&"-".repeat(w + 2));
		s.push('+');
	}
	s
}

fn build_row(cells: &[String; 5], widths: &[usize; 5]) -> String {
	let mut s = String::from("|");
	for (i, cell) in cells.iter().enumerate() {
		s.push(' ');
		s.push_str(cell);
		let pad = widths[i].saturating_sub(cell.chars().count());
		s.push_str(&" ".repeat(pad));
		s.push(' ');
		s.push('|');
	}
	s
}

fn truncate_to(s: String, max: usize) -> String {
	if s.chars().count() <= max {
		s
	} else if max <= 1 {
		".".to_string()
	} else {
		let mut t: String = s.chars().take(max - 1).collect();
		t.push('.');
		t
	}
}

fn format_value_opt(v: &Option<Value>) -> String {
	match v {
		None => "<absent>".to_string(),
		Some(v) => format_value(v),
	}
}

fn format_value(v: &Value) -> String {
	match v {
		Value::None {
			..
		} => "<none>".into(),
		Value::Boolean(b) => b.to_string(),
		Value::Int1(x) => x.to_string(),
		Value::Int2(x) => x.to_string(),
		Value::Int4(x) => x.to_string(),
		Value::Int8(x) => x.to_string(),
		Value::Int16(x) => x.to_string(),
		Value::Uint1(x) => x.to_string(),
		Value::Uint2(x) => x.to_string(),
		Value::Uint4(x) => x.to_string(),
		Value::Uint8(x) => x.to_string(),
		Value::Uint16(x) => x.to_string(),
		Value::Float4(f) => f.value().to_string(),
		Value::Float8(f) => f.value().to_string(),
		Value::Utf8(s) => s.clone(),
		other => format!("{other:?}"),
	}
}

fn format_output_key(key: &OutputKey) -> String {
	let parts: Vec<String> = key.as_slice().iter().map(format_value).collect();
	parts.join(", ")
}

fn format_tolerance(tol: Option<f64>) -> String {
	match tol {
		Some(t) => format!("{t:e}"),
		None => "exact".to_string(),
	}
}

fn format_diff(oracle: &Option<Value>, operator: &Option<Value>) -> String {
	let (Some(o), Some(p)) = (oracle, operator) else {
		return "--".to_string();
	};
	let (oo, pp) = (value_to_f64(o), value_to_f64(p));
	let (Some(of), Some(pf)) = (oo, pp) else {
		return if o == p {
			"(equal)".into()
		} else {
			"(differs)".into()
		};
	};
	let diff = pf - of;
	if is_integer_value(o) && is_integer_value(p) && diff.fract() == 0.0 && diff.abs() < (i64::MAX as f64) {
		format!("{:+}", diff as i64)
	} else {
		format!("{diff:+}")
	}
}

fn value_to_f64(v: &Value) -> Option<f64> {
	match v {
		Value::Float8(f) => Some(f.value()),
		Value::Float4(f) => Some(f.value() as f64),
		Value::Int1(x) => Some(*x as f64),
		Value::Int2(x) => Some(*x as f64),
		Value::Int4(x) => Some(*x as f64),
		Value::Int8(x) => Some(*x as f64),
		Value::Uint1(x) => Some(*x as f64),
		Value::Uint2(x) => Some(*x as f64),
		Value::Uint4(x) => Some(*x as f64),
		Value::Uint8(x) => Some(*x as f64),
		_ => None,
	}
}

fn is_integer_value(v: &Value) -> bool {
	matches!(
		v,
		Value::Int1(_)
			| Value::Int2(_) | Value::Int4(_)
			| Value::Int8(_) | Value::Uint1(_)
			| Value::Uint2(_) | Value::Uint4(_)
			| Value::Uint8(_)
	)
}

pub fn compare(operator: &MaterializedTable, oracle: &MaterializedTable, tolerances: &Tolerances) -> ComparisonResult {
	let oracle_keys: BTreeSet<&OutputKey> = oracle.rows.keys().collect();
	let operator_keys: BTreeSet<&OutputKey> = operator.rows.keys().collect();

	let only_in_oracle: Vec<OutputKey> = oracle_keys.difference(&operator_keys).map(|k| (*k).clone()).collect();
	let only_in_operator: Vec<OutputKey> = operator_keys.difference(&oracle_keys).map(|k| (*k).clone()).collect();
	let common: Vec<&&OutputKey> = oracle_keys.intersection(&operator_keys).collect();

	let mut divergent = Vec::new();
	for k in common {
		let oracle_row = oracle.rows.get(*k).unwrap();
		let operator_row = operator.rows.get(*k).unwrap();
		let column_diffs = diff_rows(oracle_row, operator_row, tolerances);
		if !column_diffs.is_empty() {
			divergent.push(DivergentRow {
				key: (*k).clone(),
				column_diffs,
			});
		}
	}

	ComparisonResult {
		only_in_oracle,
		only_in_operator,
		divergent,
	}
}

fn diff_rows(oracle: &MaterializedRow, operator: &MaterializedRow, tolerances: &Tolerances) -> Vec<ColumnDiff> {
	let mut all_columns: BTreeMap<&str, ()> = BTreeMap::new();
	for k in oracle.columns.keys() {
		all_columns.insert(k.as_str(), ());
	}
	for k in operator.columns.keys() {
		all_columns.insert(k.as_str(), ());
	}

	let mut diffs = Vec::new();
	for col in all_columns.keys() {
		let ov = oracle.columns.get(*col);
		let pv = operator.columns.get(*col);
		let tol = tolerances.get(col);
		if !values_match(ov, pv, tol) {
			diffs.push(ColumnDiff {
				column: (*col).to_string(),
				oracle_value: ov.cloned(),
				operator_value: pv.cloned(),
				tolerance: tol,
			});
		}
	}
	diffs
}

fn values_match(a: Option<&Value>, b: Option<&Value>, tol: Option<f64>) -> bool {
	match (a, b) {
		(None, None) => true,
		(Some(a), Some(b)) => match (a, b) {
			(Value::Float8(x), Value::Float8(y)) => match tol {
				Some(t) => (x.value() - y.value()).abs() <= t,
				None => x == y,
			},
			(Value::Float4(x), Value::Float4(y)) => match tol {
				Some(t) => ((x.value() as f64) - (y.value() as f64)).abs() <= t,
				None => x == y,
			},

			_ => a == b,
		},
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(values: Vec<Value>) -> OutputKey {
		OutputKey::new(values)
	}

	fn row(pairs: &[(&str, Value)]) -> MaterializedRow {
		MaterializedRow::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())))
	}

	#[test]
	fn empty_tables_match() {
		let result = compare(&MaterializedTable::empty(), &MaterializedTable::empty(), &Tolerances::new());
		assert!(result.is_match());
	}

	#[test]
	fn identical_tables_match() {
		let mut t1 = MaterializedTable::empty();
		t1.insert(
			key(vec![Value::uint8(1u64)]),
			row(&[("v", Value::float8(1.0_f64)), ("k", Value::uint8(1u64))]),
		);
		let mut t2 = MaterializedTable::empty();
		t2.insert(
			key(vec![Value::uint8(1u64)]),
			row(&[("v", Value::float8(1.0_f64)), ("k", Value::uint8(1u64))]),
		);
		assert!(compare(&t1, &t2, &Tolerances::new()).is_match());
	}

	#[test]
	fn missing_in_operator_is_only_in_oracle() {
		let mut oracle = MaterializedTable::empty();
		oracle.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64))]));
		let result = compare(&MaterializedTable::empty(), &oracle, &Tolerances::new());
		assert!(!result.is_match());
		assert_eq!(result.only_in_oracle.len(), 1);
		assert_eq!(result.only_in_operator.len(), 0);
		assert_eq!(result.divergent.len(), 0);
	}

	#[test]
	fn missing_in_oracle_is_only_in_operator() {
		let mut op = MaterializedTable::empty();
		op.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64))]));
		let result = compare(&op, &MaterializedTable::empty(), &Tolerances::new());
		assert_eq!(result.only_in_oracle.len(), 0);
		assert_eq!(result.only_in_operator.len(), 1);
	}

	#[test]
	fn float_diff_under_tolerance_is_match() {
		let mut o = MaterializedTable::empty();
		o.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64))]));
		let mut p = MaterializedTable::empty();
		p.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64 + 1e-12))]));
		// Exact comparison: differs.
		let strict = compare(&p, &o, &Tolerances::new());
		assert!(!strict.is_match());
		// With tolerance: matches.
		let lenient = compare(&p, &o, &Tolerances::new().with("v", 1e-9));
		assert!(lenient.is_match(), "{:?}", lenient);
	}

	#[test]
	fn float_diff_above_tolerance_is_divergence() {
		let mut o = MaterializedTable::empty();
		o.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64))]));
		let mut p = MaterializedTable::empty();
		p.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(2.0_f64))]));
		let result = compare(&p, &o, &Tolerances::new().with("v", 1e-9));
		assert!(!result.is_match());
		assert_eq!(result.divergent.len(), 1);
		assert_eq!(result.divergent[0].column_diffs.len(), 1);
		assert_eq!(result.divergent[0].column_diffs[0].column, "v");
	}

	#[test]
	fn integer_columns_are_always_exact() {
		let mut o = MaterializedTable::empty();
		o.insert(key(vec![Value::uint8(1u64)]), row(&[("count", Value::uint8(5u64))]));
		let mut p = MaterializedTable::empty();
		p.insert(key(vec![Value::uint8(1u64)]), row(&[("count", Value::uint8(6u64))]));
		// Tolerance on an integer column does not apply; this should
		// still be a divergence.
		let result = compare(&p, &o, &Tolerances::new().with("count", 1.0));
		assert!(!result.is_match());
	}

	#[test]
	fn missing_column_on_one_side_is_a_divergence() {
		let mut o = MaterializedTable::empty();
		o.insert(
			key(vec![Value::uint8(1u64)]),
			row(&[("a", Value::float8(1.0_f64)), ("b", Value::float8(2.0_f64))]),
		);
		let mut p = MaterializedTable::empty();
		p.insert(key(vec![Value::uint8(1u64)]), row(&[("a", Value::float8(1.0_f64))]));
		let result = compare(&p, &o, &Tolerances::new());
		assert_eq!(result.divergent.len(), 1);
		let cd = &result.divergent[0].column_diffs[0];
		assert_eq!(cd.column, "b");
		assert!(cd.operator_value.is_none());
	}

	#[test]
	fn format_failure_includes_seed_and_diffs() {
		let mut o = MaterializedTable::empty();
		o.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(1.0_f64))]));
		let mut p = MaterializedTable::empty();
		p.insert(key(vec![Value::uint8(1u64)]), row(&[("v", Value::float8(2.0_f64))]));
		let result = compare(&p, &o, &Tolerances::new().with("v", 1e-9));
		let report = result.format_failure(&["seed: 12345".to_string()], 5);
		assert!(report.contains("seed: 12345"));
		assert!(report.contains("divergent:         1"));
		assert!(report.contains("oracle"));
		assert!(report.contains("operator"));
		// Column name appears as a row in the table.
		assert!(report.contains("| v "), "missing v row in table: {report}");
	}

	#[test]
	fn format_failure_table_has_5_columns_with_diff_and_tol() {
		// One float divergence (with registered tolerance), one int
		// divergence (no tolerance).
		let mut o = MaterializedTable::empty();
		o.insert(
			key(vec![Value::utf8("TOKA"), Value::uint8(0u64)]),
			row(&[("base_volume", Value::float8(100.0_f64)), ("data_points", Value::uint4(10u32))]),
		);
		let mut p = MaterializedTable::empty();
		p.insert(
			key(vec![Value::utf8("TOKA"), Value::uint8(0u64)]),
			row(&[("base_volume", Value::float8(101.5_f64)), ("data_points", Value::uint4(11u32))]),
		);
		let result = compare(&p, &o, &Tolerances::new().with("base_volume", 1e-9));
		let report = result.format_failure(&["seed: 42".to_string()], 5);

		// Header row contains all 5 column labels.
		assert!(report.contains("column"), "{report}");
		assert!(report.contains("oracle"), "{report}");
		assert!(report.contains("operator"), "{report}");
		assert!(report.contains("tol"), "{report}");
		assert!(report.contains("diff"), "{report}");

		// Float diff with leading sign.
		assert!(report.contains("+1.5"), "missing +1.5 diff: {report}");
		// Float column has the registered tolerance, not "exact".
		assert!(report.contains("1e-9"), "missing 1e-9 tol: {report}");

		// Int diff with leading sign.
		assert!(report.contains("+1"), "missing +1 int diff: {report}");
		// Int column with no registered tolerance shows "exact".
		assert!(report.contains("exact"), "missing exact tol: {report}");

		// Output key without type wrappers.
		assert!(report.contains("TOKA, 0"), "key not stripped: {report}");
		assert!(!report.contains("Utf8(\"TOKA\")"), "Utf8 wrapper leaked: {report}");
		assert!(!report.contains("Uint8(0)"), "Uint8 wrapper leaked: {report}");

		// No Float8/OrderedF64 wrappers in body.
		assert!(!report.contains("Float8("), "Float8 wrapper leaked: {report}");
		assert!(!report.contains("OrderedF64"), "OrderedF64 wrapper leaked: {report}");
	}

	#[test]
	fn format_diff_handles_absent_and_non_numeric() {
		// Absent on one side -> "--".
		assert_eq!(format_diff(&None, &Some(Value::float8(1.0_f64))), "--");
		assert_eq!(format_diff(&Some(Value::float8(1.0_f64)), &None), "--");
		// Numeric subtraction.
		assert_eq!(format_diff(&Some(Value::float8(10.0_f64)), &Some(Value::float8(7.5_f64))), "-2.5");
		assert_eq!(format_diff(&Some(Value::uint4(5u32)), &Some(Value::uint4(8u32))), "+3");
		// Non-numeric differing.
		assert_eq!(format_diff(&Some(Value::utf8("a")), &Some(Value::utf8("b"))), "(differs)");
	}

	#[test]
	fn format_value_strips_type_wrappers() {
		assert_eq!(format_value(&Value::float8(1.5_f64)), "1.5");
		assert_eq!(format_value(&Value::uint8(42u64)), "42");
		assert_eq!(format_value(&Value::int8(-7_i64)), "-7");
		assert_eq!(format_value(&Value::utf8("hello")), "hello");
		assert_eq!(format_value(&Value::Boolean(true)), "true");
	}

	#[test]
	fn format_output_key_renders_comma_separated() {
		let k = OutputKey::new(vec![Value::utf8("TOKA"), Value::utf8("SOL"), Value::uint8(0u64)]);
		assert_eq!(format_output_key(&k), "TOKA, SOL, 0");
	}
}
