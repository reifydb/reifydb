// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_value::value::Value;

use crate::{
	error::{ExportError, RenderError},
	model::ObjectRows,
	render::{
		layout::{EnumColumn, LayoutColumn},
		value::render_value,
	},
};

pub fn render_inserts(
	qualified_object: &str,
	rows: &ObjectRows,
	batch_size: usize,
	layout: &[LayoutColumn],
) -> Result<String, ExportError> {
	if rows.rows.is_empty() {
		return Ok(String::new());
	}

	let index: HashMap<&str, usize> = rows.columns.iter().enumerate().map(|(i, name)| (name.as_str(), i)).collect();

	let batch = batch_size.max(1);
	let mut out = String::new();

	for chunk in rows.rows.chunks(batch) {
		out.push_str(&format!("INSERT {} [\n", qualified_object));
		for (i, row) in chunk.iter().enumerate() {
			out.push_str("  { ");
			for (j, column) in layout.iter().enumerate() {
				if j > 0 {
					out.push_str(", ");
				}
				match column {
					LayoutColumn::Plain(c) => {
						let rendered = match cell(&index, row, &c.name) {
							Some(value) => render_value(value).map_err(|e| {
								map_value_error(e, qualified_object, &c.name)
							})?,
							None => "none".to_string(),
						};
						out.push_str(&format!("{}: {}", c.name, rendered));
					}
					LayoutColumn::Enum(e) => {
						out.push_str(&render_enum_value(e, &index, row, qualified_object)?);
					}
				}
			}
			out.push_str(" }");
			if i + 1 < chunk.len() {
				out.push(',');
			}
			out.push('\n');
		}
		out.push_str("];\n");
	}

	Ok(out)
}

fn cell<'a>(index: &HashMap<&str, usize>, row: &'a [Value], name: &str) -> Option<&'a Value> {
	index.get(name).and_then(|&i| row.get(i))
}

fn render_enum_value(
	column: &EnumColumn,
	index: &HashMap<&str, usize>,
	row: &[Value],
	object: &str,
) -> Result<String, ExportError> {
	let variant = match cell(index, row, &column.logical_name).or_else(|| cell(index, row, &column.tag_column)) {
		None
		| Some(Value::None {
			..
		}) => return Ok(format!("{}: none", column.logical_name)),
		Some(Value::Utf8(name)) => column.variants.iter().find(|v| v.name == *name).ok_or_else(|| {
			ExportError::UnsupportedValue {
				object: object.to_string(),
				column: column.logical_name.clone(),
				value_type: format!("enum variant name {name} outside its variants"),
			}
		})?,
		Some(Value::Uint1(tag)) => column.variants.iter().find(|v| v.tag == *tag).ok_or_else(|| {
			ExportError::UnresolvedReference {
				kind: "sumtype variant",
				id: *tag as u64,
				object: object.to_string(),
			}
		})?,
		Some(_) => {
			return Err(ExportError::UnsupportedValue {
				object: object.to_string(),
				column: column.logical_name.clone(),
				value_type: "enum (expected a variant name or a uint1 tag)".to_string(),
			});
		}
	};

	let name = render_value(&Value::Utf8(variant.name.clone()))
		.map_err(|e| map_value_error(e, object, &column.logical_name))?;
	let mut parts = vec![format!("{}: {}", column.logical_name, name)];
	for (field_name, physical) in &variant.fields {
		let rendered = match cell(index, row, physical) {
			Some(value) => render_value(value).map_err(|e| map_value_error(e, object, field_name))?,
			None => "none".to_string(),
		};
		parts.push(format!("{}: {}", physical, rendered));
	}
	Ok(parts.join(", "))
}

fn map_value_error(error: RenderError, object: &str, column: &str) -> ExportError {
	match error {
		RenderError::UnrepresentableText => ExportError::UnrepresentableText {
			object: object.to_string(),
			column: column.to_string(),
		},
		RenderError::NonFiniteFloat => ExportError::NonFiniteFloat {
			object: object.to_string(),
			column: column.to_string(),
		},
		RenderError::Unsupported(value_type) => ExportError::UnsupportedValue {
			object: object.to_string(),
			column: column.to_string(),
			value_type: value_type.to_string(),
		},
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::ColumnId,
	};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use super::*;
	use crate::render::layout::EnumVariant;

	fn rows() -> ObjectRows {
		ObjectRows {
			columns: vec!["id".to_string(), "name".to_string()],
			rows: vec![
				vec![Value::Int4(1), Value::Utf8("Alice".to_string())],
				vec![Value::Int4(2), Value::Utf8("Bob".to_string())],
			],
		}
	}

	fn plain(names: &[&str]) -> Vec<Column> {
		names.iter()
			.enumerate()
			.map(|(i, n)| Column {
				id: ColumnId(i as u64),
				name: n.to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				index: ColumnIndex(i as u8),
				auto_increment: false,
				dictionary_id: None,
			})
			.collect()
	}

	fn layout(columns: &[Column]) -> Vec<LayoutColumn<'_>> {
		columns.iter().map(LayoutColumn::Plain).collect()
	}

	#[test]
	fn renders_batched_record_inserts() {
		let cols = plain(&["id", "name"]);
		let out = render_inserts("test::users", &rows(), 500, &layout(&cols)).unwrap();
		assert_eq!(out, "INSERT test::users [\n  { id: 1, name: 'Alice' },\n  { id: 2, name: 'Bob' }\n];\n");
	}

	#[test]
	fn splits_into_batches() {
		let cols = plain(&["id", "name"]);
		let out = render_inserts("test::users", &rows(), 1, &layout(&cols)).unwrap();
		let inserts = out.matches("INSERT test::users").count();
		assert_eq!(inserts, 2);
	}

	#[test]
	fn empty_rows_produce_nothing() {
		let empty = ObjectRows {
			columns: vec!["id".to_string()],
			rows: vec![],
		};
		let cols = plain(&["id"]);
		assert_eq!(render_inserts("test::users", &empty, 500, &layout(&cols)).unwrap(), "");
	}

	#[test]
	fn unrepresentable_value_fails_loud_with_location() {
		let bad = ObjectRows {
			columns: vec!["note".to_string()],
			rows: vec![vec![Value::Utf8("both ' and \"".to_string())]],
		};
		let cols = plain(&["note"]);
		assert_eq!(
			render_inserts("test::notes", &bad, 500, &layout(&cols)),
			Err(ExportError::UnrepresentableText {
				object: "test::notes".to_string(),
				column: "note".to_string()
			})
		);
	}

	#[test]
	fn reconstructs_unit_enum_value_from_tag() {
		let id_cols = plain(&["id"]);
		let enum_col = EnumColumn {
			logical_name: "state".to_string(),
			sumtype_id: 9,
			tag_column: "state_tag".to_string(),
			variants: vec![
				EnumVariant {
					tag: 0,
					name: "active".to_string(),
					fields: vec![],
				},
				EnumVariant {
					tag: 1,
					name: "inactive".to_string(),
					fields: vec![],
				},
			],
		};
		let layout = vec![LayoutColumn::Plain(&id_cols[0]), LayoutColumn::Enum(enum_col)];
		let rows = ObjectRows {
			columns: vec!["id".to_string(), "state_tag".to_string()],
			rows: vec![vec![Value::Int4(1), Value::Uint1(0)], vec![Value::Int4(2), Value::Uint1(1)]],
		};
		let out = render_inserts("shop::items", &rows, 500, &layout).unwrap();
		assert_eq!(
			out,
			"INSERT shop::items [\n  { id: 1, state: 'active' },\n  { id: 2, state: 'inactive' }\n];\n"
		);
	}

	#[test]
	fn reconstructs_structured_enum_value_from_tag_and_fields() {
		let id_cols = plain(&["id"]);
		let enum_col = EnumColumn {
			logical_name: "shape".to_string(),
			sumtype_id: 9,
			tag_column: "shape_tag".to_string(),
			variants: vec![
				EnumVariant {
					tag: 0,
					name: "circle".to_string(),
					fields: vec![("radius".to_string(), "shape_circle_radius".to_string())],
				},
				EnumVariant {
					tag: 1,
					name: "rectangle".to_string(),
					fields: vec![
						("width".to_string(), "shape_rectangle_width".to_string()),
						("height".to_string(), "shape_rectangle_height".to_string()),
					],
				},
			],
		};
		let layout = vec![LayoutColumn::Plain(&id_cols[0]), LayoutColumn::Enum(enum_col)];
		let rows = ObjectRows {
			columns: vec![
				"id".to_string(),
				"shape_tag".to_string(),
				"shape_circle_radius".to_string(),
				"shape_rectangle_width".to_string(),
				"shape_rectangle_height".to_string(),
			],
			rows: vec![
				vec![Value::Int4(1), Value::Uint1(0), Value::float8(5.0), Value::none(), Value::none()],
				vec![
					Value::Int4(2),
					Value::Uint1(1),
					Value::none(),
					Value::float8(3.0),
					Value::float8(4.0),
				],
			],
		};
		let out = render_inserts("shop::shapes", &rows, 500, &layout).unwrap();
		assert_eq!(
			out,
			"INSERT shop::shapes [\n  { id: 1, shape: 'circle', shape_circle_radius: 5 },\n  { id: 2, shape: 'rectangle', shape_rectangle_width: 3, shape_rectangle_height: 4 }\n];\n"
		);
	}
}
