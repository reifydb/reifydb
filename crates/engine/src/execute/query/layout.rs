use crate::column::layout::{EngineColumnLayout, FrameLayout};
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use reifydb_rql::expression::{ConstantExpression, Expression};

pub fn derive_frame_column_layout(
    expressions: &[Expression],
    preserve_row_ids: bool,
) -> FrameLayout {
    let mut columns = Vec::new();

    // Add RowId column if preserved
    if preserve_row_ids {
        columns.push(EngineColumnLayout {
            schema: None,
            table: None,
            name: ROW_ID_COLUMN_NAME.to_string(),
        });
    }

    for expr in expressions {
        columns.push(frame_column_layout(expr));
    }

    FrameLayout { columns }
}

fn frame_column_layout(expr: &Expression) -> EngineColumnLayout {
    match expr {
        Expression::Alias(alias_expr) => EngineColumnLayout {
            schema: None,
            table: None,
            name: alias_expr.alias.name().to_string(),
        },
        Expression::Column(col_expr) => {
            EngineColumnLayout { schema: None, table: None, name: col_expr.0.fragment.clone() }
        }
        Expression::AccessTable(access_expr) => EngineColumnLayout {
            schema: None,
            table: Some(access_expr.table.fragment.clone()),
            name: access_expr.column.fragment.clone(),
        },
        _ => {
            // For other expressions, generate a simplified name
            EngineColumnLayout { schema: None, table: None, name: simplified_name(expr) }
        }
    }
}

fn simplified_name(expr: &Expression) -> String {
    match expr {
        Expression::Add(expr) => {
            format!("{}+{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Sub(expr) => {
            format!("{}-{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Mul(expr) => {
            format!("{}*{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Div(expr) => {
            format!("{}/{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Rem(expr) => {
            format!("{}%{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Column(col_expr) => col_expr.0.fragment.clone(),
        Expression::Constant(const_expr) => match const_expr {
            ConstantExpression::Number { span } => span.fragment.clone(),
            ConstantExpression::Text { span } => span.fragment.clone(),
            ConstantExpression::Bool { span } => span.fragment.clone(),
            ConstantExpression::Temporal { span } => span.fragment.clone(),
            ConstantExpression::Undefined { .. } => "undefined".to_string(),
        },
        Expression::AccessTable(access_expr) => {
            format!("{}.{}", access_expr.table.fragment, access_expr.column.fragment)
        }
        Expression::Call(call_expr) => format!(
            "{}({})",
            call_expr.func.name(),
            call_expr.args.iter().map(|arg| simplified_name(arg)).collect::<Vec<_>>().join(",")
        ),
        Expression::Prefix(prefix_expr) => {
            format!("{}{}", prefix_expr.operator, simplified_name(&prefix_expr.expression))
        }
        Expression::Cast(cast_expr) => simplified_name(&cast_expr.expression),
        Expression::Alias(alias_expr) => alias_expr.alias.name().to_string(),
        Expression::Keyed(keyed_expr) => keyed_expr.key.name().to_string(),
        Expression::Tuple(tuple_expr) => format!(
            "({})",
            tuple_expr.expressions.iter().map(|e| simplified_name(e)).collect::<Vec<_>>().join(",")
        ),
        Expression::GreaterThan(expr) => {
            format!("{}>{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::GreaterThanEqual(expr) => {
            format!("{}>={}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::LessThan(expr) => {
            format!("{}<{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::LessThanEqual(expr) => {
            format!("{}<={}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Equal(expr) => {
            format!("{}=={}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::NotEqual(expr) => {
            format!("{}!={}", simplified_name(&expr.left), simplified_name(&expr.right))
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
            format!("{}and{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Or(expr) => {
            format!("{}or{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Xor(expr) => {
            format!("{}xor{}", simplified_name(&expr.left), simplified_name(&expr.right))
        }
        Expression::Type(type_expr) => type_expr.span.fragment.clone(),
    }
}
