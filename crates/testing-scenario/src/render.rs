// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

pub fn literal(value: &Value) -> String {
	match value {
		Value::None {
			..
		} => "none".to_string(),
		Value::Utf8(text) => quote(text),
		Value::Boolean(_)
		| Value::Float4(_)
		| Value::Float8(_)
		| Value::Int1(_)
		| Value::Int2(_)
		| Value::Int4(_)
		| Value::Int8(_)
		| Value::Int16(_)
		| Value::Uint1(_)
		| Value::Uint2(_)
		| Value::Uint4(_)
		| Value::Uint8(_)
		| Value::Uint16(_)
		| Value::Int(_)
		| Value::Uint(_)
		| Value::Decimal(_) => value.to_string(),
		other => panic!("scenario rows cannot render a {:?} literal", other.get_type()),
	}
}

fn quote(text: &str) -> String {
	let mut out = String::with_capacity(text.len() + 2);
	out.push('"');
	for character in text.chars() {
		match character {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			_ => out.push(character),
		}
	}
	out.push('"');
	out
}

pub fn row(columns: &[&'static str], values: &[Value]) -> String {
	assert_eq!(
		columns.len(),
		values.len(),
		"row generator produced {} values for {} columns",
		values.len(),
		columns.len()
	);

	let fields: Vec<String> =
		columns.iter().zip(values).map(|(column, value)| format!("{}: {}", column, literal(value))).collect();

	format!("{{ {} }}", fields.join(", "))
}

pub fn insert_statements(
	table: &str,
	columns: &[&'static str],
	rows: impl Iterator<Item = Vec<Value>>,
	batch_size: usize,
) -> Vec<String> {
	assert!(batch_size > 0, "insert batch size must be positive");

	let mut statements = Vec::new();
	let mut batch: Vec<String> = Vec::with_capacity(batch_size);

	for values in rows {
		batch.push(row(columns, &values));
		if batch.len() == batch_size {
			statements.push(format!("INSERT {} [{}]", table, batch.join(", ")));
			batch.clear();
		}
	}

	if !batch.is_empty() {
		statements.push(format!("INSERT {} [{}]", table, batch.join(", ")));
	}

	statements
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::render::{insert_statements, literal, row};

	#[test]
	fn utf8_literal_is_quoted() {
		// Unquoted text would parse as an identifier, so a seeded row would either fail to
		// insert or silently bind the wrong thing. Display alone does not quote.
		assert_eq!(literal(&Value::Utf8("user_1".to_string())), "\"user_1\"");
	}

	#[test]
	fn utf8_literal_escapes_embedded_quotes_and_backslashes() {
		assert_eq!(literal(&Value::Utf8("a\"b".to_string())), "\"a\\\"b\"");
		assert_eq!(literal(&Value::Utf8("a\\b".to_string())), "\"a\\\\b\"");
	}

	#[test]
	fn missing_value_renders_as_none_not_null() {
		assert_eq!(
			literal(&Value::None {
				inner: ValueType::Int8,
			}),
			"none"
		);
	}

	#[test]
	fn numeric_and_boolean_literals_are_bare() {
		assert_eq!(literal(&Value::Int8(42)), "42");
		assert_eq!(literal(&Value::Int8(-7)), "-7");
		assert_eq!(literal(&Value::Boolean(true)), "true");
	}

	#[test]
	fn row_renders_column_value_pairs() {
		let rendered = row(&["id", "name"], &[Value::Int8(1), Value::Utf8("a".to_string())]);
		assert_eq!(rendered, "{ id: 1, name: \"a\" }");
	}

	#[test]
	#[should_panic(expected = "row generator produced")]
	fn row_rejects_arity_mismatch() {
		row(&["id", "name"], &[Value::Int8(1)]);
	}

	#[test]
	fn insert_statements_split_on_batch_size() {
		let rows = (0..5u64).map(|i| vec![Value::Int8(i as i64)]);
		let statements = insert_statements("bench::t", &["id"], rows, 2);

		assert_eq!(statements.len(), 3);
		assert_eq!(statements[0], "INSERT bench::t [{ id: 0 }, { id: 1 }]");
		assert_eq!(statements[2], "INSERT bench::t [{ id: 4 }]");
	}

	#[test]
	fn insert_statements_emit_nothing_for_an_empty_dataset() {
		let statements = insert_statements("bench::t", &["id"], Vec::new().into_iter(), 100);
		assert!(statements.is_empty());
	}
}
