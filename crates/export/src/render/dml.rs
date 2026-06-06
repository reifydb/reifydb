// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_value::value::Value;

use crate::{
	error::{ExportError, RenderError},
	model::{NameResolver, ShapeRows},
	render::{
		layout::{EnumColumn, LayoutColumn},
		value::render_value,
	},
};

pub fn render_inserts(
	qualified_shape: &str,
	rows: &ShapeRows,
	batch_size: usize,
	layout: &[LayoutColumn],
	resolver: &NameResolver,
) -> Result<String, ExportError> {
	if rows.rows.is_empty() {
		return Ok(String::new());
	}

	let index: HashMap<&str, usize> = rows.columns.iter().enumerate().map(|(i, name)| (name.as_str(), i)).collect();

	let batch = batch_size.max(1);
	let mut out = String::new();

	for chunk in rows.rows.chunks(batch) {
		out.push_str(&format!("INSERT {} [\n", qualified_shape));
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
								map_value_error(e, qualified_shape, &c.name)
							})?,
							None => "none".to_string(),
						};
						out.push_str(&format!("{}: {}", c.name, rendered));
					}
					LayoutColumn::Enum(e) => {
						let rendered =
							render_enum_value(e, resolver, &index, row, qualified_shape)?;
						out.push_str(&format!("{}: {}", e.logical_name, rendered));
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
	resolver: &NameResolver,
	index: &HashMap<&str, usize>,
	row: &[Value],
	shape: &str,
) -> Result<String, ExportError> {
	let qualified = &resolver
		.sumtype(column.sumtype_id)
		.ok_or_else(|| ExportError::UnresolvedReference {
			kind: "sumtype",
			id: column.sumtype_id,
			shape: shape.to_string(),
		})?
		.qualified_name;

	let tag = match cell(index, row, &column.tag_column) {
		None
		| Some(Value::None {
			..
		}) => return Ok("none".to_string()),
		Some(Value::Uint1(t)) => *t,
		Some(_) => {
			return Err(ExportError::UnsupportedValue {
				shape: shape.to_string(),
				column: column.logical_name.clone(),
				value_type: "enum tag (expected uint1)".to_string(),
			});
		}
	};

	let variant =
		column.variants.iter().find(|v| v.tag == tag).ok_or_else(|| ExportError::UnresolvedReference {
			kind: "sumtype variant",
			id: tag as u64,
			shape: shape.to_string(),
		})?;

	if variant.fields.is_empty() {
		return Ok(format!("{}::{}", qualified, variant.name));
	}

	let mut parts = Vec::with_capacity(variant.fields.len());
	for (field_name, physical) in &variant.fields {
		let rendered = match cell(index, row, physical) {
			Some(value) => render_value(value).map_err(|e| map_value_error(e, shape, field_name))?,
			None => "none".to_string(),
		};
		parts.push(format!("{}: {}", field_name, rendered));
	}
	Ok(format!("{}::{} {{ {} }}", qualified, variant.name, parts.join(", ")))
}

fn map_value_error(error: RenderError, shape: &str, column: &str) -> ExportError {
	match error {
		RenderError::UnrepresentableText => ExportError::UnrepresentableText {
			shape: shape.to_string(),
			column: column.to_string(),
		},
		RenderError::NonFiniteFloat => ExportError::NonFiniteFloat {
			shape: shape.to_string(),
			column: column.to_string(),
		},
		RenderError::Unsupported(value_type) => ExportError::UnsupportedValue {
			shape: shape.to_string(),
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
	use crate::{model::ResolvedSumType, render::layout::EnumVariant};

	fn rows() -> ShapeRows {
		ShapeRows {
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
		let out = render_inserts("test::users", &rows(), 500, &layout(&cols), &NameResolver::empty()).unwrap();
		assert_eq!(out, "INSERT test::users [\n  { id: 1, name: 'Alice' },\n  { id: 2, name: 'Bob' }\n];\n");
	}

	#[test]
	fn splits_into_batches() {
		let cols = plain(&["id", "name"]);
		let out = render_inserts("test::users", &rows(), 1, &layout(&cols), &NameResolver::empty()).unwrap();
		let inserts = out.matches("INSERT test::users").count();
		assert_eq!(inserts, 2);
	}

	#[test]
	fn empty_rows_produce_nothing() {
		let empty = ShapeRows {
			columns: vec!["id".to_string()],
			rows: vec![],
		};
		let cols = plain(&["id"]);
		assert_eq!(
			render_inserts("test::users", &empty, 500, &layout(&cols), &NameResolver::empty()).unwrap(),
			""
		);
	}

	#[test]
	fn unrepresentable_value_fails_loud_with_location() {
		let bad = ShapeRows {
			columns: vec!["note".to_string()],
			rows: vec![vec![Value::Utf8("both ' and \"".to_string())]],
		};
		let cols = plain(&["note"]);
		assert_eq!(
			render_inserts("test::notes", &bad, 500, &layout(&cols), &NameResolver::empty()),
			Err(ExportError::UnrepresentableText {
				shape: "test::notes".to_string(),
				column: "note".to_string()
			})
		);
	}

	fn resolver_with(id: u64, qualified: &str) -> NameResolver {
		let mut r = NameResolver::empty();
		r.sumtypes.insert(
			id,
			ResolvedSumType {
				qualified_name: qualified.to_string(),
				variants: vec![],
			},
		);
		r
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
		let rows = ShapeRows {
			columns: vec!["id".to_string(), "state_tag".to_string()],
			rows: vec![vec![Value::Int4(1), Value::Uint1(0)], vec![Value::Int4(2), Value::Uint1(1)]],
		};
		let out =
			render_inserts("shop::items", &rows, 500, &layout, &resolver_with(9, "shop::status")).unwrap();
		assert_eq!(
			out,
			"INSERT shop::items [\n  { id: 1, state: shop::status::active },\n  { id: 2, state: shop::status::inactive }\n];\n"
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
		let rows = ShapeRows {
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
		let out =
			render_inserts("shop::shapes", &rows, 500, &layout, &resolver_with(9, "shop::shape")).unwrap();
		assert_eq!(
			out,
			"INSERT shop::shapes [\n  { id: 1, shape: shop::shape::circle { radius: 5 } },\n  { id: 2, shape: shop::shape::rectangle { width: 3, height: 4 } }\n];\n"
		);
	}
}
