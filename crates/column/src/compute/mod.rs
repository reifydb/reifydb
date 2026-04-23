// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod canonical;

use reifydb_core::value::column::{array::Column, mask::RowMask};
use reifydb_type::{Result, value::Value};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchResult {
	Found(usize),
	NotFound(usize),
}

// Per-encoding compute specialization. Each method returns `Option<Result<_>>`:
// `None` means "this encoding doesn't specialize - fall back to canonical;"
// `Some(Ok(_))` is a real result; `Some(Err(_))` is a real error encountered
// while running the specialization. Free functions below dispatch through
// this trait and fall back to canonicalize-and-run when `None` is returned.
pub trait Compute: Send + Sync {
	fn filter(&self, _array: &Column, _mask: &RowMask) -> Option<Result<Column>> {
		None
	}

	fn take(&self, _array: &Column, _indices: &Column) -> Option<Result<Column>> {
		None
	}

	fn slice(&self, _array: &Column, _start: usize, _end: usize) -> Option<Result<Column>> {
		None
	}

	fn compare(&self, _array: &Column, _rhs: &Value, _op: CompareOp) -> Option<Result<Column>> {
		None
	}

	fn search_sorted(&self, _array: &Column, _needle: &Value) -> Option<Result<SearchResult>> {
		None
	}

	fn min_max(&self, _array: &Column) -> Option<Result<(Value, Value)>> {
		None
	}

	fn sum(&self, _array: &Column) -> Option<Result<Value>> {
		None
	}
}

pub struct DefaultCompute;

impl Compute for DefaultCompute {}

// Each free function first asks the array's encoding for a specialization; if
// the encoding returns `None`, the caller canonicalizes and runs the canonical
// kernel. This preserves the "compressed encodings can always fall back
// correctly" invariant - correctness only depends on the canonical path.

pub fn filter(array: &Column, mask: &RowMask) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.filter(array, mask)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Column::from_canonical(canonical::filter::filter(&canon, mask)?))
}

pub fn take(array: &Column, indices: &Column) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.take(array, indices)) {
		return result;
	}
	let canon = array.to_canonical()?;
	let idx = indices.to_canonical()?;
	Ok(Column::from_canonical(canonical::take::take(&canon, &idx)?))
}

pub fn slice(array: &Column, start: usize, end: usize) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.slice(array, start, end)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Column::from_canonical(canonical::slice::slice(&canon, start, end)?))
}

pub fn compare(array: &Column, rhs: &Value, op: CompareOp) -> Result<Column> {
	if let Some(result) = specialized(array, |c| c.compare(array, rhs, op)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Column::from_canonical(canonical::compare::compare(&canon, rhs, op)?))
}

pub fn search_sorted(array: &Column, needle: &Value) -> Result<SearchResult> {
	if let Some(result) = specialized(array, |c| c.search_sorted(array, needle)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::search_sorted::search_sorted(&canon, needle)
}

pub fn min_max(array: &Column) -> Result<(Value, Value)> {
	if let Some(result) = specialized(array, |c| c.min_max(array)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::min_max::min_max(&canon)
}

pub fn sum(array: &Column) -> Result<Value> {
	if let Some(result) = specialized(array, |c| c.sum(array)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::sum::sum(&canon)
}

fn specialized<T>(array: &Column, hook: impl FnOnce(&dyn Compute) -> Option<Result<T>>) -> Option<Result<T>> {
	let registry = encoding::global();
	let encoding = registry.get(array.encoding())?;
	hook(encoding.compute())
}
