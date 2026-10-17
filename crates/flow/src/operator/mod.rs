// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{interface::catalog::flow::OperatorId, value::batch::batch};
use reifydb_value::{
	Result,
	error::Error,
	value::system_columns::{SystemColumn, is_system_field, keep_system_columns},
};

use crate::error::FlowGraphError;

pub mod append;
pub(crate) mod dictionary;
pub mod extend;
pub mod filter;
pub mod map;
pub mod sink;
pub mod sort;
pub mod source;

const FORWARDED_SYSTEM_COLUMNS: [SystemColumn; 4] =
	[SystemColumn::RowNumbers, SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time];

pub(crate) fn forward_system_columns(batch: &RecordBatch) -> Result<RecordBatch> {
	keep_system_columns(batch, &FORWARDED_SYSTEM_COLUMNS)
}

pub(crate) fn with_system_columns_of(
	mut columns: Vec<(FieldRef, ArrayRef)>,
	from: &RecordBatch,
) -> Result<RecordBatch> {
	columns.extend(from
		.schema_ref()
		.fields()
		.iter()
		.zip(from.columns())
		.filter(|(field, _)| is_system_field(field))
		.map(|(field, array)| (field.clone(), array.clone())));
	batch(columns)
}

pub fn first_input(inputs: &[OperatorId]) -> Result<OperatorId> {
	inputs.first().copied().ok_or_else(|| Error::from(FlowGraphError::MissingInputEdge))
}
