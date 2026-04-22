// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod canonical;

use reifydb_type::{Result, value::Value};

use crate::{array::Array, encoding, mask::RowMask};

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
// `None` means "this encoding doesn't specialize — fall back to canonical;"
// `Some(Ok(_))` is a real result; `Some(Err(_))` is a real error encountered
// while running the specialization. Free functions below dispatch through
// this trait and fall back to canonicalize-and-run when `None` is returned.
pub trait Compute: Send + Sync {
	fn filter(&self, _array: &Array, _mask: &RowMask) -> Option<Result<Array>> {
		None
	}

	fn take(&self, _array: &Array, _indices: &Array) -> Option<Result<Array>> {
		None
	}

	fn slice(&self, _array: &Array, _start: usize, _end: usize) -> Option<Result<Array>> {
		None
	}

	fn compare(&self, _array: &Array, _rhs: &Value, _op: CompareOp) -> Option<Result<Array>> {
		None
	}

	fn search_sorted(&self, _array: &Array, _needle: &Value) -> Option<Result<SearchResult>> {
		None
	}

	fn min_max(&self, _array: &Array) -> Option<Result<(Value, Value)>> {
		None
	}

	fn sum(&self, _array: &Array) -> Option<Result<Value>> {
		None
	}
}

pub struct DefaultCompute;

impl Compute for DefaultCompute {}

// ----- Free-function dispatch -------------------------------------------------

// Each free function first asks the array's encoding for a specialization; if
// the encoding returns `None`, the caller canonicalizes and runs the canonical
// kernel. This preserves the "compressed encodings can always fall back
// correctly" invariant — correctness only depends on the canonical path.

pub fn filter(array: &Array, mask: &RowMask) -> Result<Array> {
	if let Some(result) = specialized(array, |c| c.filter(array, mask)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Array::from_canonical(canonical::filter(&canon, mask)?))
}

pub fn take(array: &Array, indices: &Array) -> Result<Array> {
	if let Some(result) = specialized(array, |c| c.take(array, indices)) {
		return result;
	}
	let canon = array.to_canonical()?;
	let idx = indices.to_canonical()?;
	Ok(Array::from_canonical(canonical::take(&canon, &idx)?))
}

pub fn slice(array: &Array, start: usize, end: usize) -> Result<Array> {
	if let Some(result) = specialized(array, |c| c.slice(array, start, end)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Array::from_canonical(canonical::slice(&canon, start, end)?))
}

pub fn compare(array: &Array, rhs: &Value, op: CompareOp) -> Result<Array> {
	if let Some(result) = specialized(array, |c| c.compare(array, rhs, op)) {
		return result;
	}
	let canon = array.to_canonical()?;
	Ok(Array::from_canonical(canonical::compare(&canon, rhs, op)?))
}

pub fn search_sorted(array: &Array, needle: &Value) -> Result<SearchResult> {
	if let Some(result) = specialized(array, |c| c.search_sorted(array, needle)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::search_sorted(&canon, needle)
}

pub fn min_max(array: &Array) -> Result<(Value, Value)> {
	if let Some(result) = specialized(array, |c| c.min_max(array)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::min_max(&canon)
}

pub fn sum(array: &Array) -> Result<Value> {
	if let Some(result) = specialized(array, |c| c.sum(array)) {
		return result;
	}
	let canon = array.to_canonical()?;
	canonical::sum(&canon)
}

fn specialized<T>(array: &Array, hook: impl FnOnce(&dyn Compute) -> Option<Result<T>>) -> Option<Result<T>> {
	let registry = encoding::global();
	let encoding = registry.get(array.encoding())?;
	hook(encoding.compute())
}
