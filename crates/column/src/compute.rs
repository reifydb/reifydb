// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, value::Value};

use crate::{array::Array, mask::RowMask};

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
// while running the specialization. Free functions in this module dispatch
// through this trait and fall back to canonicalize-and-run.
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
