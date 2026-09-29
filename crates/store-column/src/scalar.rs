// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::builder::ColumnBuilder;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		column_view::ColumnView,
		value_type::{ValueType, field::to_field},
	},
};
use vortex_array::{IntoArray, VortexSessionExecute, arrays::ConstantArray, scalar::Scalar};
use vortex_session::VortexSession;

use crate::{
	convert::{to_arrow, to_vortex},
	error::{ColumnError, vortex},
	snapshot::ColumnChunks,
};

pub fn to_scalar(session: &VortexSession, name: &str, column: &ColumnChunks, value: &Value) -> Result<Scalar> {
	let rejected = || -> Error {
		ColumnError::PredicateValue {
			column: name.to_string(),
			ty: column.ty.to_string(),
			value: value.to_string(),
		}
		.into()
	};
	let Some(full) = column.field_type.value_type.clone() else {
		return Err(rejected());
	};
	let bare = match &full {
		ValueType::Option(inner) => inner.as_ref().clone(),
		other => other.clone(),
	};
	if value.get_type() != bare {
		return Err(rejected());
	}
	let mut builder = ColumnBuilder::with_capacity(full, 1);
	builder.push_value(value.clone());
	let one = builder.finish(name);
	if one.0.data_type() != to_field(name, &column.field_type).data_type() {
		return Err(rejected());
	}
	let array = to_vortex(session, &one)?;
	array.execute_scalar(0, &mut session.create_execution_ctx()).map_err(vortex("predicate"))
}

pub fn to_value(session: &VortexSession, name: &str, column: &ColumnChunks, scalar: Scalar) -> Result<Value> {
	let one = ConstantArray::new(scalar, 1).into_array();
	let exported = to_arrow(session, name, &column.field_type, one)?;
	Ok(ColumnView::try_from(&exported)?.get_value(0))
}
