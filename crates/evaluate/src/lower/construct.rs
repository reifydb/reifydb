// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::{expression::Expression, value::column::factory};
use reifydb_value::{
	error::{Error, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{Value, column_view::ColumnView},
};

use crate::{
	Result,
	expression::context::EvalContext,
	lower::{Lowered, Node, bool_field, error::into_external, literal, lower_node, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Shape {
	Tuple,
	List,
}

impl Shape {
	fn value(self, items: Vec<Value>) -> Value {
		match self {
			Shape::Tuple => Value::Tuple(items),
			Shape::List => Value::List(items),
		}
	}
}

pub(super) fn lower_construct<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	shape: Shape,
	items: &'e [Expression],
	fragment: &Fragment,
) -> Lowered<'e, Node> {
	if shape == Shape::Tuple && items.len() == 1 {
		return lower_node(ctx, operator, &items[0]);
	}
	let name = fragment.text();
	if items.is_empty() {
		let (field, array) = factory::any(name, [shape.value(vec![])]);
		return Ok(literal::row_zero(operator, &field, &array)?);
	}
	let mut args = Vec::with_capacity(items.len());
	for item in items {
		args.push(lower_node(ctx, operator, item)?.expr);
	}
	let (field, _) = factory::any(name, []);
	Ok(Node {
		expr: udf(
			ConstructUdf {
				shape,
				fragment: fragment.clone(),
				field: field.clone(),
				signature: volatile(),
			},
			args,
		),
		field,
	})
}

pub(super) fn lower_projection<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expressions: &'e [Expression],
	fragment: &Fragment,
) -> Lowered<'e, Node> {
	if expressions.len() == 1 {
		return lower_node(ctx, operator, &expressions[0]);
	}
	let mut args = Vec::with_capacity(expressions.len());
	let mut field = bool_field("none", true);
	for (index, expression) in expressions.iter().enumerate() {
		let node = lower_node(ctx, operator, expression)?;
		if index == 0 {
			field = node.field.clone();
		}
		args.push(node.expr);
	}
	Ok(Node {
		expr: udf(
			ProjectionUdf {
				columns: expressions.len(),
				fragment: fragment.clone(),
				field: field.clone(),
				signature: volatile(),
			},
			args,
		),
		field,
	})
}

#[derive(Debug)]
struct ConstructUdf {
	shape: Shape,
	fragment: Fragment,
	field: FieldRef,
	signature: Signature,
}

impl PartialEq for ConstructUdf {
	fn eq(&self, other: &Self) -> bool {
		self.shape == other.shape && self.fragment == other.fragment
	}
}

impl Eq for ConstructUdf {}

impl Hash for ConstructUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.shape.hash(state);
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for ConstructUdf {
	fn name(&self) -> &str {
		"construct"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("construct types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(self.field.clone())
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let mut arrays = Vec::with_capacity(args.args.len());
		for value in args.args {
			arrays.push(value.into_array(args.number_rows)?);
		}
		let views = arrays
			.iter()
			.zip(args.arg_fields.iter())
			.map(|(array, field)| ColumnView::try_from((array, field.as_ref())))
			.collect::<Result<Vec<_>>>()
			.map_err(into_external)?;
		let rows = (0..args.number_rows)
			.map(|row| self.shape.value(views.iter().map(|view| view.get_value(row)).collect()));
		let (_, array) = factory::any(self.fragment.text(), rows);
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[derive(Debug)]
struct ProjectionUdf {
	columns: usize,
	fragment: Fragment,
	field: FieldRef,
	signature: Signature,
}

impl PartialEq for ProjectionUdf {
	fn eq(&self, other: &Self) -> bool {
		self.columns == other.columns && self.fragment == other.fragment
	}
}

impl Eq for ProjectionUdf {}

impl Hash for ProjectionUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.columns.hash(state);
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for ProjectionUdf {
	fn name(&self) -> &str {
		"projection"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("projection types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(self.field.clone())
	}

	fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let error: Error = TypeError::Runtime {
			kind: RuntimeErrorKind::ExpectedSingleColumn {
				actual: self.columns,
			},
			message: "expression produces more than one column where one is required".to_string(),
		}
		.into();
		Err(into_external(error))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
