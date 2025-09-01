use reifydb_core::{
	interface::evaluate::expression::{ConstantExpression, Expression},
	value::columnar::layout::{ColumnLayout, ColumnsLayout},
};
use reifydb_type::ROW_NUMBER_COLUMN_NAME;

pub fn derive_columns_column_layout(
	expressions: &[Expression],
	preserve_row_numbers: bool,
) -> ColumnsLayout {
	let mut columns = Vec::new();

	// Add RowNumber column if preserved
	if preserve_row_numbers {
		columns.push(ColumnLayout {
			schema: None,
			source: None,
			name: ROW_NUMBER_COLUMN_NAME.to_string(),
		});
	}

	for expr in expressions {
		columns.push(columns_column_layout(expr));
	}

	ColumnsLayout {
		columns,
	}
}

fn columns_column_layout(expr: &Expression) -> ColumnLayout {
	match expr {
		Expression::Alias(alias_expr) => ColumnLayout {
			schema: None,
			source: None,
			name: alias_expr.alias.name().to_string(),
		},
		Expression::Column(col_expr) => ColumnLayout {
			schema: None,
			source: None,
			name: col_expr.0.fragment().to_string(),
		},
		Expression::AccessSource(access_expr) => ColumnLayout {
			schema: None,
			source: Some(access_expr.source.fragment().to_string()),
			name: access_expr.column.fragment().to_string(),
		},
		_ => {
			// For other expressions, generate a simplified name
			ColumnLayout {
				schema: None,
				source: None,
				name: simplified_name(expr),
			}
		}
	}
}

fn simplified_name(expr: &Expression) -> String {
	match expr {
		Expression::Add(expr) => {
			format!(
				"{}+{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Sub(expr) => {
			format!(
				"{}-{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Mul(expr) => {
			format!(
				"{}*{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Div(expr) => {
			format!(
				"{}/{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Rem(expr) => {
			format!(
				"{}%{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Column(col_expr) => {
			col_expr.0.fragment().to_string()
		}
		Expression::Constant(const_expr) => match const_expr {
			ConstantExpression::Number {
				fragment,
			} => fragment.fragment().to_string(),
			ConstantExpression::Text {
				fragment,
			} => fragment.fragment().to_string(),
			ConstantExpression::Bool {
				fragment,
			} => fragment.fragment().to_string(),
			ConstantExpression::Temporal {
				fragment,
			} => fragment.fragment().to_string(),
			ConstantExpression::Undefined {
				..
			} => "undefined".to_string(),
		},
		Expression::AccessSource(access_expr) => {
			format!(
				"{}.{}",
				access_expr.source.fragment(),
				access_expr.column.fragment()
			)
		}
		Expression::Call(call_expr) => format!(
			"{}({})",
			call_expr.func.name(),
			call_expr
				.args
				.iter()
				.map(|arg| simplified_name(arg))
				.collect::<Vec<_>>()
				.join(",")
		),
		Expression::Prefix(prefix_expr) => {
			format!(
				"{}{}",
				prefix_expr.operator,
				simplified_name(&prefix_expr.expression)
			)
		}
		Expression::Cast(cast_expr) => {
			simplified_name(&cast_expr.expression)
		}
		Expression::Alias(alias_expr) => {
			alias_expr.alias.name().to_string()
		}
		Expression::Tuple(tuple_expr) => format!(
			"({})",
			tuple_expr
				.expressions
				.iter()
				.map(|e| simplified_name(e))
				.collect::<Vec<_>>()
				.join(",")
		),
		Expression::GreaterThan(expr) => {
			format!(
				"{}>{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::GreaterThanEqual(expr) => {
			format!(
				"{}>={}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::LessThan(expr) => {
			format!(
				"{}<{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::LessThanEqual(expr) => {
			format!(
				"{}<={}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Equal(expr) => {
			format!(
				"{}=={}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::NotEqual(expr) => {
			format!(
				"{}!={}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Between(expr) => {
			format!(
				"{} BETWEEN {} AND {}",
				simplified_name(&expr.value),
				simplified_name(&expr.lower),
				simplified_name(&expr.upper)
			)
		}
		Expression::And(expr) => {
			format!(
				"{}and{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Or(expr) => {
			format!(
				"{}or{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Xor(expr) => {
			format!(
				"{}xor{}",
				simplified_name(&expr.left),
				simplified_name(&expr.right)
			)
		}
		Expression::Type(type_expr) => {
			type_expr.fragment.fragment().to_string()
		}
		Expression::Parameter(_) => "parameter".to_string(),
	}
}
