use crate::expression::Expression;
use reifydb_core::frame::{FrameColumnLayout, FrameLayout};
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;

impl Expression {
    pub fn derive_frame_column_layout(
        expressions: &[Expression],
        preserve_row_ids: bool,
    ) -> FrameLayout {
        let mut columns = Vec::new();

        // Add RowId column if preserved
        if preserve_row_ids {
            columns.push(FrameColumnLayout {
                schema: None,
                table: None,
                name: ROW_ID_COLUMN_NAME.to_string(),
            });
        }

        for expr in expressions {
            columns.push(expr.frame_column_layout());
        }

        FrameLayout { columns }
    }

    fn frame_column_layout(&self) -> FrameColumnLayout {
        match self {
            Expression::Alias(alias_expr) => FrameColumnLayout {
                schema: None,
                table: None,
                name: alias_expr.alias.name().to_string(),
            },
            Expression::Column(col_expr) => {
                FrameColumnLayout { schema: None, table: None, name: col_expr.0.fragment.clone() }
            }
            Expression::AccessTable(access_expr) => FrameColumnLayout {
                schema: None,
                table: Some(access_expr.table.fragment.clone()),
                name: access_expr.column.fragment.clone(),
            },
            _ => {
                // For other expressions, generate a simplified name
                FrameColumnLayout { schema: None, table: None, name: self.simplified_name() }
            }
        }
    }

    fn simplified_name(&self) -> String {
        match self {
            Expression::Add(expr) => {
                format!("{}+{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Sub(expr) => {
                format!("{}-{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Mul(expr) => {
                format!("{}*{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Div(expr) => {
                format!("{}/{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Rem(expr) => {
                format!("{}%{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Column(col_expr) => col_expr.0.fragment.clone(),
            Expression::Constant(const_expr) => match const_expr {
                crate::expression::ConstantExpression::Number { span } => span.fragment.clone(),
                crate::expression::ConstantExpression::Text { span } => span.fragment.clone(),
                crate::expression::ConstantExpression::Bool { span } => span.fragment.clone(),
                crate::expression::ConstantExpression::Temporal { span } => span.fragment.clone(),
                crate::expression::ConstantExpression::Undefined { .. } => "undefined".to_string(),
            },
            Expression::AccessTable(access_expr) => {
                format!("{}.{}", access_expr.table.fragment, access_expr.column.fragment)
            }
            Expression::Call(call_expr) => format!(
                "{}({})",
                call_expr.func.name(),
                call_expr
                    .args
                    .iter()
                    .map(|arg| arg.simplified_name())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Expression::Prefix(prefix_expr) => {
                format!("{}{}", prefix_expr.operator, prefix_expr.expression.simplified_name())
            }
            Expression::Cast(cast_expr) => cast_expr.expression.simplified_name(),
            Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
            Expression::Keyed(keyed_expr) => keyed_expr.key.name().to_string(),
            Expression::Tuple(tuple_expr) => format!(
                "({})",
                tuple_expr
                    .expressions
                    .iter()
                    .map(|e| e.simplified_name())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Expression::GreaterThan(expr) => {
                format!("{}>{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::GreaterThanEqual(expr) => {
                format!("{}>={}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::LessThan(expr) => {
                format!("{}<{}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::LessThanEqual(expr) => {
                format!("{}<={}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Equal(expr) => {
                format!("{}=={}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::NotEqual(expr) => {
                format!("{}!={}", expr.left.simplified_name(), expr.right.simplified_name())
            }
            Expression::Between(expr) => {
                format!("{} BETWEEN {} AND {}", expr.value.simplified_name(), expr.lower.simplified_name(), expr.upper.simplified_name())
            }
            Expression::Type(type_expr) => type_expr.span.fragment.clone(),
        }
    }
}
