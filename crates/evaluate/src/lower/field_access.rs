// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::{Hash, Hasher},
	sync::Arc,
};

use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::expression::{Expression, FieldAccessExpression, name::display_label};
use reifydb_value::{
	error::{Error, RuntimeErrorKind, TypeError},
	fragment::Fragment,
};

use crate::{
	expression::context::EvalContext,
	lower::{Lowered, Node, error::into_external, lower_node, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_field_access<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	access: &'e FieldAccessExpression,
) -> Lowered<Node> {
	let object = lower_node(ctx, operator, &access.object)?;
	let field = Arc::new(object.field.as_ref().clone().with_name(display_label(expression).text()));
	Ok(Node {
		expr: udf(
			FieldAccessUdf {
				field: access.field.text().to_string(),
				fragment: access.fragment.clone(),
				signature: volatile(),
			},
			vec![object.expr],
		),
		field,
	})
}

fn field_not_found(field: &str) -> Error {
	TypeError::Runtime {
		kind: RuntimeErrorKind::FieldNotFound {
			variable: "<expression>".to_string(),
			field: field.to_string(),
			available: vec![],
		},
		message: format!("Field '{}' not found on variable '<expression>'", field),
	}
	.into()
}

#[derive(Debug)]
struct FieldAccessUdf {
	field: String,
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for FieldAccessUdf {
	fn eq(&self, other: &Self) -> bool {
		self.fragment == other.fragment && self.field == other.field
	}
}

impl Eq for FieldAccessUdf {}

impl Hash for FieldAccessUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.fragment.hash(state);
		self.field.hash(state);
	}
}

impl ScalarUDFImpl for FieldAccessUdf {
	fn name(&self) -> &str {
		"field_access"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("field_access types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(args.arg_fields[0].clone())
	}

	fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		Err(into_external(field_not_found(&self.field)))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
