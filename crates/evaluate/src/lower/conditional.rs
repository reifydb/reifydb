// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::BooleanArray;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, expr::Case};
use reifydb_core::expression::{Expression, IfExpression};
use reifydb_value::{
	fragment::Fragment,
	value::{column_view::ColumnView, value_type::ValueType},
};

use crate::{
	Result,
	expression::{branch::BranchLayout, compile::is_truthy, context::EvalContext},
	lower::{
		Lowered, Node, bool_field, cast::cast_node, error::into_external, inner_type, is_untyped_none,
		lower_node, typed_field, udf, volatile,
	},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_if<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	_expression: &'e Expression,
	e: &'e IfExpression,
) -> Lowered<'e, Node> {
	let mut conditions = vec![truthy(lower_node(ctx, operator, &e.condition)?)?];
	let mut branches = vec![lower_node(ctx, operator, &e.then_expr)?];
	for else_if in &e.else_ifs {
		conditions.push(truthy(lower_node(ctx, operator, &else_if.condition)?)?);
		branches.push(lower_node(ctx, operator, &else_if.then_expr)?);
	}
	let otherwise = match &e.else_expr {
		Some(expr) => Some(lower_node(ctx, operator, expr)?),
		None => None,
	};

	let name = branches[0].field.name().clone();
	let all: Vec<&Node> = branches.iter().chain(otherwise.iter()).collect();
	let target = layout_type(&all, &e.fragment)?;
	let nullable = otherwise.is_none() || all.iter().any(|node| node.field.is_nullable());
	let first = all[0].field.clone();

	let Some(target) = target else {
		return Ok(Node {
			expr: Expr::Case(Case::new(
				None,
				conditions
					.into_iter()
					.zip(branches)
					.map(|(c, b)| (Box::new(c), Box::new(b.expr)))
					.collect(),
				otherwise.map(|node| Box::new(node.expr)),
			)),
			field: Arc::new(first.as_ref().clone().with_name(&name)),
		});
	};

	let shaped = |node: Node| -> Result<Expr> {
		if !is_untyped_none(&node.field)? && inner_type(&node.field)? == target {
			return Ok(node.expr);
		}
		Ok(cast_node(ctx, node, &target, &e.fragment).expr)
	};
	let mut when_then = Vec::with_capacity(conditions.len());
	for (condition, branch) in conditions.into_iter().zip(branches) {
		when_then.push((Box::new(condition), Box::new(shaped(branch)?)));
	}
	let otherwise = match otherwise {
		Some(node) => Some(Box::new(shaped(node)?)),
		None => None,
	};
	Ok(Node {
		expr: Expr::Case(Case::new(None, when_then, otherwise)),
		field: typed_field(&name, target, nullable),
	})
}

fn layout_type(nodes: &[&Node], fragment: &Fragment) -> Result<Option<ValueType>> {
	let describe = |node: &Node| -> Result<(String, ValueType, bool)> {
		let untyped = is_untyped_none(&node.field)?;
		let value_type = if untyped {
			ValueType::Any
		} else {
			inner_type(&node.field)?
		};
		Ok((node.field.name().clone(), value_type, untyped))
	};
	let described = nodes.iter().map(|node| describe(node)).collect::<Result<Vec<_>>>()?;
	let mut layout = BranchLayout::new(described[..1].iter().map(|(n, t, u)| (n.as_str(), t.clone(), *u)));
	for (name, ty, untyped) in &described[1..] {
		layout.admit([(name.as_str(), ty.clone(), *untyped)], fragment)?;
	}
	if described.iter().all(|(_, _, untyped)| *untyped) {
		return Ok(None);
	}
	Ok(Some(layout.types()[0].clone()))
}

fn truthy(node: Node) -> Result<Expr> {
	if !is_untyped_none(&node.field)? && inner_type(&node.field)? == ValueType::Boolean {
		return Ok(node.expr);
	}
	Ok(udf(
		TruthyUdf {
			signature: volatile(),
		},
		vec![node.expr],
	))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TruthyUdf {
	signature: Signature,
}

impl ScalarUDFImpl for TruthyUdf {
	fn name(&self) -> &str {
		"truthy"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("truthy types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(bool_field("truthy", false))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let array = args.args[0].clone().into_array(args.number_rows)?;
		let view = ColumnView::try_from((&array, args.arg_fields[0].as_ref())).map_err(into_external)?;
		let values: BooleanArray =
			(0..args.number_rows).map(|row| Some(is_truthy(&view.get_value(row)))).collect();
		Ok(ColumnarValue::Array(Arc::new(values)))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
