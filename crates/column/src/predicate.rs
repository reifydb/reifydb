// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{array::Column, buffer::ColumnBuffer, mask::RowMask};
use reifydb_type::{Result, value::Value};

use crate::{
	compute::{self, CompareOp},
	error::ColumnError,
	selection::Selection,
	snapshot::{ColumnBlock, ColumnChunks},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColRef(pub String);

impl From<&str> for ColRef {
	fn from(s: &str) -> Self {
		Self(s.to_string())
	}
}

impl From<String> for ColRef {
	fn from(s: String) -> Self {
		Self(s)
	}
}

#[derive(Clone, Debug)]
pub enum Predicate {
	Eq(ColRef, Value),
	Ne(ColRef, Value),
	Lt(ColRef, Value),
	LtEq(ColRef, Value),
	Gt(ColRef, Value),
	GtEq(ColRef, Value),
	In(ColRef, Vec<Value>),
	IsNone(ColRef),
	IsNotNone(ColRef),
	And(Vec<Predicate>),
	Or(Vec<Predicate>),
	Not(Box<Predicate>),
}

// Evaluate a `Predicate` over a single-chunk `ColumnBlock`, producing a `Selection`
// that callers can feed to `compute::filter`. v1 asserts every column has at
// most one chunk; multi-chunk eval lands alongside batched scan output.
pub fn evaluate(block: &ColumnBlock, predicate: &Predicate) -> Result<Selection> {
	for ch in &block.columns {
		if ch.chunk_count() > 1 {
			return Err(ColumnError::MultiChunkUnsupported {
				operation: "predicate::evaluate",
				chunk_count: ch.chunk_count(),
			}
			.into());
		}
	}
	let len = block.len();
	let mask = evaluate_mask(block, predicate, len)?;
	Ok(mask_to_selection(mask))
}

fn evaluate_mask(block: &ColumnBlock, predicate: &Predicate, len: usize) -> Result<RowMask> {
	match predicate {
		Predicate::Eq(col, v) => compare_mask(block, col, v, CompareOp::Eq),
		Predicate::Ne(col, v) => compare_mask(block, col, v, CompareOp::Ne),
		Predicate::Lt(col, v) => compare_mask(block, col, v, CompareOp::Lt),
		Predicate::LtEq(col, v) => compare_mask(block, col, v, CompareOp::LtEq),
		Predicate::Gt(col, v) => compare_mask(block, col, v, CompareOp::Gt),
		Predicate::GtEq(col, v) => compare_mask(block, col, v, CompareOp::GtEq),
		Predicate::In(col, values) => {
			let mut acc = RowMask::none_set(len);
			for v in values {
				acc = acc.or(&compare_mask(block, col, v, CompareOp::Eq)?);
			}
			Ok(acc)
		}
		Predicate::IsNone(col) => Ok(is_none_mask(column(block, col)?)),
		Predicate::IsNotNone(col) => Ok(is_none_mask(column(block, col)?).not()),
		Predicate::And(clauses) => {
			let mut acc = RowMask::all_set(len);
			for c in clauses {
				acc = acc.and(&evaluate_mask(block, c, len)?);
			}
			Ok(acc)
		}
		Predicate::Or(clauses) => {
			let mut acc = RowMask::none_set(len);
			for c in clauses {
				acc = acc.or(&evaluate_mask(block, c, len)?);
			}
			Ok(acc)
		}
		Predicate::Not(inner) => Ok(evaluate_mask(block, inner, len)?.not()),
	}
}

fn compare_mask(block: &ColumnBlock, col: &ColRef, rhs: &Value, op: CompareOp) -> Result<RowMask> {
	let ch = column(block, col)?;
	let array = single_chunk(ch)?;
	let result = compute::compare(array, rhs, op)?;
	bool_array_to_mask(&result)
}

fn is_none_mask(ch: &ColumnChunks) -> RowMask {
	let len = ch.len();
	let mut mask = RowMask::none_set(len);
	// v1: single-chunk assumption checked by evaluate().
	if let Some(array) = ch.chunks.first()
		&& let Some(nones) = array.nones()
	{
		for i in 0..array.len() {
			if nones.is_none(i) {
				mask.set(i, true);
			}
		}
	}
	mask
}

fn column<'a>(block: &'a ColumnBlock, col: &ColRef) -> Result<&'a ColumnChunks> {
	block.column_by_name(&col.0).map(|(_, ch)| ch).ok_or_else(|| {
		ColumnError::ColumnNotInSchema {
			operation: "predicate::evaluate",
			name: col.0.clone(),
		}
		.into()
	})
}

fn single_chunk(ch: &ColumnChunks) -> Result<&Column> {
	ch.chunks.first().ok_or_else(|| {
		ColumnError::EmptyChunkedArray {
			operation: "predicate::evaluate",
		}
		.into()
	})
}

// Convert a bool canonical `Column` to a `RowMask`. None-valued rows count as
// "not selected" - three-valued-logic collapses to a two-valued mask at the
// `Selection` boundary.
fn bool_array_to_mask(array: &Column) -> Result<RowMask> {
	let canon = array.to_canonical()?;
	if !matches!(canon.buffer, ColumnBuffer::Bool(_)) {
		return Err(ColumnError::PredicateCompareNotBool.into());
	}
	let len = canon.len();
	let mut mask = RowMask::none_set(len);
	let nones = canon.nones.as_ref();
	for i in 0..len {
		let is_true = matches!(canon.buffer.get_value(i), Value::Boolean(true));
		if is_true && !nones.map(|n| n.is_none(i)).unwrap_or(false) {
			mask.set(i, true);
		}
	}
	Ok(mask)
}

fn mask_to_selection(mask: RowMask) -> Selection {
	let kept = mask.popcount();
	if kept == 0 {
		Selection::None_
	} else if kept == mask.len() {
		Selection::All
	} else {
		Selection::Mask(mask)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::value::column::{
		array::{Column, canonical::Canonical},
		buffer::ColumnBuffer,
	};
	use reifydb_type::value::r#type::Type;

	use super::*;

	fn mkblock(rows: [(i32, bool); 5]) -> ColumnBlock {
		let ids = ColumnBuffer::int4(rows.map(|(v, _)| v).to_vec());
		let flags = ColumnBuffer::bool(rows.map(|(_, v)| v).to_vec());
		let id_col = ColumnChunks::single(
			Type::Int4,
			false,
			Column::from_canonical(Canonical::from_column_buffer(&ids).unwrap()),
		);
		let flag_col = ColumnChunks::single(
			Type::Boolean,
			false,
			Column::from_canonical(Canonical::from_column_buffer(&flags).unwrap()),
		);
		let schema = Arc::new(vec![
			("id".to_string(), Type::Int4, false),
			("flag".to_string(), Type::Boolean, false),
		]);
		ColumnBlock::new(schema, vec![id_col, flag_col])
	}

	#[test]
	fn evaluate_eq_produces_mask() {
		let t = mkblock([(1, true), (2, false), (3, true), (2, true), (5, false)]);
		let p = Predicate::Eq(ColRef::from("id"), Value::Int4(2));
		let Selection::Mask(m) = evaluate(&t, &p).unwrap() else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.popcount(), 2);
		assert!(m.get(1));
		assert!(m.get(3));
	}

	#[test]
	fn evaluate_all_collapses_to_selection_all() {
		let t = mkblock([(1, true), (2, true), (3, true), (4, true), (5, true)]);
		let p = Predicate::GtEq(ColRef::from("id"), Value::Int4(0));
		assert!(matches!(evaluate(&t, &p).unwrap(), Selection::All));
	}

	#[test]
	fn evaluate_none_collapses_to_selection_none() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::Lt(ColRef::from("id"), Value::Int4(0));
		assert!(matches!(evaluate(&t, &p).unwrap(), Selection::None_));
	}

	#[test]
	fn evaluate_and_combines_with_intersection() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::And(vec![
			Predicate::Gt(ColRef::from("id"), Value::Int4(1)),
			Predicate::Eq(ColRef::from("flag"), Value::Boolean(true)),
		]);
		let Selection::Mask(m) = evaluate(&t, &p).unwrap() else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.popcount(), 2);
		assert!(m.get(2));
		assert!(m.get(4));
	}

	#[test]
	fn evaluate_in_matches_any_value() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::In(ColRef::from("id"), vec![Value::Int4(2), Value::Int4(5)]);
		let Selection::Mask(m) = evaluate(&t, &p).unwrap() else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.popcount(), 2);
		assert!(m.get(1));
		assert!(m.get(4));
	}

	#[test]
	fn evaluate_is_none_on_nullable_column() {
		let mut nullable_ids = ColumnBuffer::int4_with_capacity(4);
		nullable_ids.push::<i32>(10);
		nullable_ids.push_none();
		nullable_ids.push::<i32>(30);
		nullable_ids.push_none();
		let id_col = ColumnChunks::single(
			Type::Int4,
			true,
			Column::from_canonical(Canonical::from_column_buffer(&nullable_ids).unwrap()),
		);
		let schema = Arc::new(vec![("id".to_string(), Type::Int4, true)]);
		let t = ColumnBlock::new(schema, vec![id_col]);

		let Selection::Mask(m) = evaluate(&t, &Predicate::IsNone(ColRef::from("id"))).unwrap() else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.popcount(), 2);
		assert!(m.get(1));
		assert!(m.get(3));
	}
}
