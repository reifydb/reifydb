// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column::Column;
use reifydb_value::value::{Value, partition::Partition, value_type::ValueType};

use crate::expression::{ColumnExpression, ConstantExpression, Expression};

pub fn extract_partition(condition: &Expression, columns: &[Column], partition_by: &[String]) -> Option<Partition> {
	if partition_by.is_empty() {
		return None;
	}

	let mut conjuncts = Vec::new();
	flatten_and(condition, &mut conjuncts);

	let mut values: Vec<Value> = Vec::with_capacity(partition_by.len());
	for col_name in partition_by {
		let col = columns.iter().find(|c| c.name == *col_name)?;
		if col.constraint.get_type() != ValueType::Utf8 {
			return None;
		}
		let text = conjuncts.iter().find_map(|e| eq_text_for(e, col_name))?;
		values.push(Value::Utf8(text));
	}

	Some(Partition::of(&values))
}

fn flatten_and<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
	match expr {
		Expression::And(and) => {
			flatten_and(&and.left, out);
			flatten_and(&and.right, out);
		}
		other => out.push(other),
	}
}

fn eq_text_for(expr: &Expression, col_name: &str) -> Option<String> {
	let Expression::Equal(eq) = expr else {
		return None;
	};
	if is_column(&eq.left, col_name) {
		return text_constant(&eq.right);
	}
	if is_column(&eq.right, col_name) {
		return text_constant(&eq.left);
	}
	None
}

fn is_column(expr: &Expression, col_name: &str) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col)) => col.name.text() == col_name,
		Expression::AccessSource(access) => access.column.name.text() == col_name,
		_ => false,
	}
}

fn text_constant(expr: &Expression) -> Option<String> {
	match expr {
		Expression::Constant(ConstantExpression::Text {
			fragment,
		}) => Some(fragment.text().to_string()),
		_ => None,
	}
}
