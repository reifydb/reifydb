// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::{Hash, Hasher},
	sync::Arc,
};

use arrow_array::BooleanArray;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, expr::Case};
use reifydb_core::expression::{Expression, IfExpression};
use reifydb_value::{
	error::{Diagnostic, Error},
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
	let expressions: Vec<&Expression> = [e.then_expr.as_ref()]
		.into_iter()
		.chain(e.else_ifs.iter().map(|else_if| else_if.then_expr.as_ref()))
		.chain(e.else_expr.as_deref())
		.collect();
	let mut columns = Vec::with_capacity(all.len());
	for (node, expression) in all.iter().zip(&expressions) {
		columns.push(branch_columns(ctx, operator, expression, node)?);
	}
	let (target, mut mismatches) = layout(&columns, &e.fragment)?;
	if columns.iter().any(|fields| fields.len() != columns[0].len())
		&& let Some(diagnostic) = mismatches.iter().flatten().next().cloned()
	{
		for (index, fields) in columns.iter().enumerate() {
			if fields.len() != 1 && mismatches[index].is_none() {
				mismatches[index] = Some(diagnostic.clone());
			}
		}
	}
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

	let shaped = |index: usize, node: Node| -> Result<Expr> {
		if let Some(diagnostic) = &mismatches[index] {
			return Ok(udf(
				MismatchUdf {
					diagnostic: diagnostic.clone(),
					index,
					field: typed_field(&name, target.clone(), true),
					signature: volatile(),
				},
				if columns[index].len() == 1 {
					vec![node.expr]
				} else {
					vec![]
				},
			));
		}
		if !is_untyped_none(&node.field)? && inner_type(&node.field)? == target {
			return Ok(node.expr);
		}
		Ok(cast_node(ctx, node, &target, &e.fragment).expr)
	};
	let branch_count = branches.len();
	let mut when_then = Vec::with_capacity(conditions.len());
	for (index, (condition, branch)) in conditions.into_iter().zip(branches).enumerate() {
		when_then.push((Box::new(condition), Box::new(shaped(index, branch)?)));
	}
	let otherwise = match otherwise {
		Some(node) => Some(Box::new(shaped(branch_count, node)?)),
		None => None,
	};
	Ok(Node {
		expr: Expr::Case(Case::new(None, when_then, otherwise)),
		field: typed_field(&name, target, nullable),
	})
}

fn branch_columns<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	node: &Node,
) -> Lowered<'e, Vec<FieldRef>> {
	let parts = match expression {
		Expression::Map(map) if map.expressions.len() != 1 => &map.expressions,
		Expression::Extend(extend) if extend.expressions.len() != 1 => &extend.expressions,
		_ => return Ok(vec![node.field.clone()]),
	};
	let mut fields = Vec::with_capacity(parts.len());
	for part in parts {
		fields.push(lower_node(ctx, operator, part)?.field);
	}
	Ok(fields)
}

type Described = Vec<(String, ValueType, bool)>;

fn layout(branches: &[Vec<FieldRef>], fragment: &Fragment) -> Result<(Option<ValueType>, Vec<Option<Diagnostic>>)> {
	let describe = |field: &FieldRef| -> Result<(String, ValueType, bool)> {
		let untyped = is_untyped_none(field)?;
		let value_type = if untyped {
			ValueType::Any
		} else {
			inner_type(field)?
		};
		Ok((field.name().clone(), value_type, untyped))
	};
	let described = branches
		.iter()
		.map(|fields| fields.iter().map(describe).collect::<Result<Described>>())
		.collect::<Result<Vec<_>>>()?;
	let entries =
		|index: usize| described[index].iter().map(|(name, ty, untyped)| (name.as_str(), ty.clone(), *untyped));
	let build = |accepted: &[usize]| -> Result<BranchLayout> {
		let mut layout = BranchLayout::new(entries(accepted[0]));
		for &index in &accepted[1..] {
			layout.admit(entries(index), fragment)?;
		}
		Ok(layout)
	};
	let mut accepted = vec![0];
	let mut mismatches = vec![None; described.len()];
	for index in 1..described.len() {
		match build(&accepted)?.admit(entries(index), fragment) {
			Ok(()) => accepted.push(index),
			Err(error) => mismatches[index] = Some(*error.0),
		}
	}
	if accepted.iter().all(|&index| described[index].iter().all(|(_, _, untyped)| *untyped)) {
		return Ok((None, mismatches));
	}
	Ok((Some(build(&accepted)?.types()[0].clone()), mismatches))
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

#[derive(Debug)]
struct MismatchUdf {
	diagnostic: Diagnostic,
	index: usize,
	field: FieldRef,
	signature: Signature,
}

impl PartialEq for MismatchUdf {
	fn eq(&self, other: &Self) -> bool {
		self.diagnostic.fragment == other.diagnostic.fragment && self.index == other.index
	}
}

impl Eq for MismatchUdf {}

impl Hash for MismatchUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.diagnostic.fragment.hash(state);
		self.index.hash(state);
	}
}

impl ScalarUDFImpl for MismatchUdf {
	fn name(&self) -> &str {
		"branch_mismatch"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("branch_mismatch types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(self.field.clone())
	}

	fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		Err(into_external(Error(Box::new(self.diagnostic.clone()))))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
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
