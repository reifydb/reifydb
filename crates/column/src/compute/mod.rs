// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod canonical;

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::data::Column;
use reifydb_value::{Result, value::Value};

use crate::encoding;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
	Eq,
	Ne,
	Lt,
	LtEq,
	Gt,
	GtEq,
}

pub trait Compute: Send + Sync {
	fn filter(&self, _array: &Column, _mask: &BooleanBuffer) -> Option<Result<Column>> {
		None
	}

	fn compare(&self, _array: &Column, _rhs: &Value, _op: CompareOp) -> Option<Result<Column>> {
		None
	}

	fn min_max(&self, _array: &Column) -> Option<Result<(Value, Value)>> {
		None
	}
}

pub struct DefaultCompute;

impl Compute for DefaultCompute {}

pub fn filter(array: &Column, mask: &BooleanBuffer) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.filter(array, mask)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Column::from_canonical(canonical::filter::filter(&canon, mask)?))
}

pub fn compare(array: &Column, rhs: &Value, op: CompareOp) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.compare(array, rhs, op)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Column::from_canonical(canonical::compare::compare(&canon, rhs, op)?))
}

pub fn min_max(array: &Column) -> Result<(Value, Value)> {
	if let Some(result) = specialized(array, |c| c.min_max(array)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::min_max::min_max(&canon)
}

fn specialized<T>(array: &Column, hook: impl FnOnce(&dyn Compute) -> Option<Result<T>>) -> Option<Result<T>> {
	let registry = encoding::global();
	let encoding = registry.get(array.encoding())?;
	hook(encoding.compute())
}
