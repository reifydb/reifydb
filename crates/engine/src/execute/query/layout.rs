use reifydb_core::{
	interface::evaluate::expression::{ConstantExpression, Expression},
	value::columnar::layout::{ColumnLayout, ColumnsLayout},
};
use reifydb_type::{Fragment, ROW_NUMBER_COLUMN_NAME};

pub fn derive_columns_column_layout<'a>(
	expressions: &[Expression<'a>],
	preserve_row_numbers: bool,
) -> ColumnsLayout<'a> {
	let mut columns = Vec::new();

	// Add RowNumber column if preserved
	if preserve_row_numbers {
		columns.push(ColumnLayout {
			namespace: None,
			source: None,
			name: Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
		});
	}

	for expr in expressions {
		columns.push(columns_column_layout(expr));
	}

	ColumnsLayout {
		columns,
	}
}

fn columns_column_layout<'a>(expr: &Expression<'a>) -> ColumnLayout<'a> {
	match expr {
		Expression::Alias(alias_expr) => ColumnLayout {
			namespace: None,
			source: None,
			name: alias_expr.alias.0.clone(),
		},
		Expression::Column(col_expr) => ColumnLayout {
			namespace: None,
			source: None,
			name: col_expr.0.name.clone(),
		},
		Expression::AccessSource(access_expr) => {
			use reifydb_core::interface::identifier::ColumnSource;

			// Extract source name based on the ColumnSource type
			let source_name = match &access_expr.column.source {
				ColumnSource::Source {
					source,
					..
				} => source,
				ColumnSource::Alias(alias) => alias,
			};

			ColumnLayout {
				namespace: None,
				source: Some(source_name.clone()),
				name: access_expr.column.name.clone(),
			}
		}
		_ => {
			// For other expressions, generate a simplified name
			ColumnLayout {
				namespace: None,
				source: None,
				name: simplified_name(expr),
			}
		}
	}
}

fn simplified_name<'a>(expr: &Expression<'a>) -> Fragment<'a> {
	match expr {
		Expression::Add(expr) => Fragment::owned_internal(format!(
			"{}+{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Sub(expr) => Fragment::owned_internal(format!(
			"{}-{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Mul(expr) => Fragment::owned_internal(format!(
			"{}*{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Div(expr) => Fragment::owned_internal(format!(
			"{}/{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Rem(expr) => Fragment::owned_internal(format!(
			"{}%{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Column(col_expr) => col_expr.0.name.clone(),
		Expression::Constant(const_expr) => match const_expr {
			ConstantExpression::Number {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Text {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Bool {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Temporal {
				fragment,
			} => fragment.clone(),
			ConstantExpression::Undefined {
				..
			} => Fragment::owned_internal("undefined"),
		},
		Expression::AccessSource(access_expr) => {
			use reifydb_core::interface::identifier::ColumnSource;

			// Extract source name based on the ColumnSource type
			let source_name = match &access_expr.column.source {
				ColumnSource::Source {
					source,
					..
				} => source.text(),
				ColumnSource::Alias(alias) => alias.text(),
			};

			Fragment::owned_internal(format!("{}.{}", source_name, access_expr.column.name.text()))
		}
		Expression::Call(call_expr) => Fragment::owned_internal(format!(
			"{}({})",
			call_expr.func.name(),
			call_expr
				.args
				.iter()
				.map(|arg| simplified_name(arg).text().to_string())
				.collect::<Vec<_>>()
				.join(",")
		)),
		Expression::Prefix(prefix_expr) => Fragment::owned_internal(format!(
			"{}{}",
			prefix_expr.operator,
			simplified_name(&prefix_expr.expression).text()
		)),
		Expression::Cast(cast_expr) => simplified_name(&cast_expr.expression),
		Expression::Alias(alias_expr) => Fragment::owned_internal(alias_expr.alias.name()),
		Expression::Tuple(tuple_expr) => Fragment::owned_internal(format!(
			"({})",
			tuple_expr
				.expressions
				.iter()
				.map(|e| simplified_name(e).text().to_string())
				.collect::<Vec<_>>()
				.join(",")
		)),
		Expression::GreaterThan(expr) => Fragment::owned_internal(format!(
			"{}>{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::GreaterThanEqual(expr) => Fragment::owned_internal(format!(
			"{}>={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::LessThan(expr) => Fragment::owned_internal(format!(
			"{}<{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::LessThanEqual(expr) => Fragment::owned_internal(format!(
			"{}<={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Equal(expr) => Fragment::owned_internal(format!(
			"{}=={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::NotEqual(expr) => Fragment::owned_internal(format!(
			"{}!={}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Between(expr) => Fragment::owned_internal(format!(
			"{} BETWEEN {} AND {}",
			simplified_name(&expr.value).text(),
			simplified_name(&expr.lower).text(),
			simplified_name(&expr.upper).text()
		)),
		Expression::And(expr) => Fragment::owned_internal(format!(
			"{}and{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Or(expr) => Fragment::owned_internal(format!(
			"{}or{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Xor(expr) => Fragment::owned_internal(format!(
			"{}xor{}",
			simplified_name(&expr.left).text(),
			simplified_name(&expr.right).text()
		)),
		Expression::Type(type_expr) => type_expr.fragment.clone(),
		Expression::Parameter(_) => Fragment::owned_internal("parameter"),
	}
}
