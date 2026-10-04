// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use reifydb_value::{Result, value::Value};
use vortex_array::{
	IntoArray, VortexSessionExecute, arrays::ConstantArray, builtins::ArrayBuiltins,
	scalar_fn::fns::operators::Operator,
};
use vortex_mask::Mask;
use vortex_session::VortexSession;

use crate::{
	error::{ColumnError, vortex},
	scalar::to_scalar,
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

pub fn evaluate(block: &ColumnBlock, predicate: &Predicate, session: &VortexSession) -> Result<Selection> {
	let len = block.len();
	let mask = evaluate_mask(block, predicate, len, session)?;
	Ok(mask_to_selection(mask))
}

fn evaluate_mask(
	block: &ColumnBlock,
	predicate: &Predicate,
	len: usize,
	session: &VortexSession,
) -> Result<BooleanBuffer> {
	match predicate {
		Predicate::Eq(col, v) => compare_mask(block, col, v, Operator::Eq, session),
		Predicate::Ne(col, v) => compare_mask(block, col, v, Operator::NotEq, session),
		Predicate::Lt(col, v) => compare_mask(block, col, v, Operator::Lt, session),
		Predicate::LtEq(col, v) => compare_mask(block, col, v, Operator::Lte, session),
		Predicate::Gt(col, v) => compare_mask(block, col, v, Operator::Gt, session),
		Predicate::GtEq(col, v) => compare_mask(block, col, v, Operator::Gte, session),
		Predicate::In(col, values) => {
			let mut acc = BooleanBuffer::new_unset(len);
			for v in values {
				acc = &acc | &compare_mask(block, col, v, Operator::Eq, session)?;
			}
			Ok(acc)
		}
		Predicate::IsNone(col) => is_none_mask(column(block, col)?, session),
		Predicate::IsNotNone(col) => Ok(!&is_none_mask(column(block, col)?, session)?),
		Predicate::And(clauses) => {
			let mut acc = BooleanBuffer::new_set(len);
			for c in clauses {
				acc = &acc & &evaluate_mask(block, c, len, session)?;
			}
			Ok(acc)
		}
		Predicate::Or(clauses) => {
			let mut acc = BooleanBuffer::new_unset(len);
			for c in clauses {
				acc = &acc | &evaluate_mask(block, c, len, session)?;
			}
			Ok(acc)
		}
		Predicate::Not(inner) => Ok(!&evaluate_mask(block, inner, len, session)?),
	}
}

fn compare_mask(
	block: &ColumnBlock,
	col: &ColRef,
	rhs: &Value,
	op: Operator,
	session: &VortexSession,
) -> Result<BooleanBuffer> {
	let ch = column(block, col)?;
	if ch.chunks.is_empty() {
		return Ok(BooleanBuffer::new_unset(0));
	}
	let scalar = to_scalar(session, &col.0, ch, rhs)?;
	let mut ctx = session.create_execution_ctx();
	let mut mask = BooleanBufferBuilder::new(ch.len());
	for chunk in &ch.chunks {
		let rhs = ConstantArray::new(scalar.clone(), chunk.len()).into_array();
		let compared = chunk.binary(rhs, op).map_err(vortex("predicate"))?;
		let matched: Mask = compared.null_as_false().execute(&mut ctx).map_err(vortex("predicate"))?;
		mask.append_buffer(&to_bits(&matched));
	}
	Ok(mask.finish())
}

fn is_none_mask(ch: &ColumnChunks, session: &VortexSession) -> Result<BooleanBuffer> {
	let mut ctx = session.create_execution_ctx();
	let mut mask = BooleanBufferBuilder::new(ch.len());
	for chunk in &ch.chunks {
		let nones: Mask =
			chunk.is_null().map_err(vortex("predicate"))?.execute(&mut ctx).map_err(vortex("predicate"))?;
		mask.append_buffer(&to_bits(&nones));
	}
	Ok(mask.finish())
}

fn to_bits(mask: &Mask) -> BooleanBuffer {
	BooleanBuffer::from(mask.to_bit_buffer())
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

fn mask_to_selection(mask: BooleanBuffer) -> Selection {
	let kept = mask.count_set_bits();
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

	use arrow_array::ArrayRef as ArrowArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_core::value::column::{builder::ColumnBuilder, factory};
	use reifydb_value::value::value_type::{ValueType, field::from_field};

	use super::*;
	use crate::{convert::to_vortex, session::new_session};

	fn chunks(ty: ValueType, nullable: bool, parts: &[(FieldRef, ArrowArrayRef)]) -> ColumnChunks {
		let session = new_session();
		let field_type = from_field(&parts[0].0).unwrap();
		let arrays = parts.iter().map(|p| to_vortex(&session, p).unwrap()).collect();
		ColumnChunks::new(ty, nullable, field_type, arrays)
	}

	fn run(block: &ColumnBlock, predicate: &Predicate) -> Selection {
		evaluate(block, predicate, &new_session()).unwrap()
	}

	fn mkblock(rows: [(i32, bool); 5]) -> ColumnBlock {
		let ids = factory::int4("id", rows.map(|(v, _)| v).to_vec());
		let flags = factory::bool("flag", rows.map(|(_, v)| v).to_vec());
		let id_col = chunks(ValueType::Int4, false, &[ids]);
		let flag_col = chunks(ValueType::Boolean, false, &[flags]);
		let schema = Arc::new(vec![
			("id".to_string(), ValueType::Int4, false),
			("flag".to_string(), ValueType::Boolean, false),
		]);
		ColumnBlock::new(schema, vec![id_col, flag_col])
	}

	#[test]
	fn evaluate_eq_produces_mask() {
		let t = mkblock([(1, true), (2, false), (3, true), (2, true), (5, false)]);
		let p = Predicate::Eq(ColRef::from("id"), Value::Int4(2));
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(1));
		assert!(m.value(3));
	}

	#[test]
	fn evaluate_all_collapses_to_selection_all() {
		let t = mkblock([(1, true), (2, true), (3, true), (4, true), (5, true)]);
		let p = Predicate::GtEq(ColRef::from("id"), Value::Int4(0));
		assert!(matches!(run(&t, &p), Selection::All));
	}

	#[test]
	fn evaluate_none_collapses_to_selection_none() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::Lt(ColRef::from("id"), Value::Int4(0));
		assert!(matches!(run(&t, &p), Selection::None_));
	}

	#[test]
	fn evaluate_and_combines_with_intersection() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::And(vec![
			Predicate::Gt(ColRef::from("id"), Value::Int4(1)),
			Predicate::Eq(ColRef::from("flag"), Value::Boolean(true)),
		]);
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(2));
		assert!(m.value(4));
	}

	#[test]
	fn evaluate_in_matches_any_value() {
		let t = mkblock([(1, true), (2, false), (3, true), (4, false), (5, true)]);
		let p = Predicate::In(ColRef::from("id"), vec![Value::Int4(2), Value::Int4(5)]);
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(1));
		assert!(m.value(4));
	}

	#[test]
	fn evaluate_is_none_on_nullable_column() {
		let mut nullable_ids = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		nullable_ids.push::<i32>(10);
		nullable_ids.push_none();
		nullable_ids.push::<i32>(30);
		nullable_ids.push_none();
		let nullable_ids = nullable_ids.finish("id");
		let id_col = chunks(ValueType::Int4, true, &[nullable_ids]);
		let schema = Arc::new(vec![("id".to_string(), ValueType::Int4, true)]);
		let t = ColumnBlock::new(schema, vec![id_col]);

		let Selection::Mask(m) = run(&t, &Predicate::IsNone(ColRef::from("id"))) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(1));
		assert!(m.value(3));
	}

	fn int4_chunked(parts: &[&[i32]]) -> ColumnChunks {
		let parts: Vec<_> = parts.iter().map(|p| factory::int4("id", p.to_vec())).collect();
		chunks(ValueType::Int4, false, &parts)
	}

	fn mkblock_chunked(id_parts: &[&[i32]]) -> ColumnBlock {
		let id_col = int4_chunked(id_parts);
		let schema = Arc::new(vec![("id".to_string(), ValueType::Int4, false)]);
		ColumnBlock::new(schema, vec![id_col])
	}

	#[test]
	fn evaluate_eq_over_multi_chunk_column() {
		// Matches fall in all three chunks, so the mask must be indexed by block row, not chunk row.
		let t = mkblock_chunked(&[&[1, 2, 3], &[2, 4, 2], &[5, 2]]);
		let p = Predicate::Eq(ColRef::from("id"), Value::Int4(2));
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.len(), 8);
		assert_eq!(m.count_set_bits(), 4);
		assert!(m.value(1));
		assert!(m.value(3));
		assert!(m.value(5));
		assert!(m.value(7));
	}

	#[test]
	fn evaluate_and_or_across_multi_chunk_columns() {
		// Both columns are chunked identically; the combinator must align them by block row, not chunk index.
		let id_col = int4_chunked(&[&[1, 2, 3], &[4, 5, 6]]);
		let other_col = int4_chunked(&[&[10, 20, 10], &[20, 10, 20]]);
		let schema = Arc::new(vec![
			("id".to_string(), ValueType::Int4, false),
			("other".to_string(), ValueType::Int4, false),
		]);
		let t = ColumnBlock::new(schema, vec![id_col, other_col]);

		let p = Predicate::And(vec![
			Predicate::Gt(ColRef::from("id"), Value::Int4(2)),
			Predicate::Eq(ColRef::from("other"), Value::Int4(20)),
		]);
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.len(), 6);
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(3));
		assert!(m.value(5));
	}

	#[test]
	fn evaluate_is_none_across_multi_chunk_nullable() {
		// A none at row 1 of each chunk has to resolve to block rows 1 and 4, not chunk-local 1 twice.
		let mut a = ColumnBuilder::with_capacity(ValueType::Int4, 3);
		a.push::<i32>(10);
		a.push_none();
		a.push::<i32>(30);
		let a = a.finish("id");
		let mut b = ColumnBuilder::with_capacity(ValueType::Int4, 3);
		b.push::<i32>(40);
		b.push_none();
		b.push::<i32>(60);
		let b = b.finish("id");
		let id_col = chunks(ValueType::Int4, true, &[a, b]);
		let schema = Arc::new(vec![("id".to_string(), ValueType::Int4, true)]);
		let t = ColumnBlock::new(schema, vec![id_col]);

		let Selection::Mask(m) = run(&t, &Predicate::IsNone(ColRef::from("id"))) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.len(), 6);
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(1));
		assert!(m.value(4));
	}

	#[test]
	fn evaluate_in_across_multi_chunk_column() {
		let t = mkblock_chunked(&[&[1, 2], &[3, 4], &[5, 6]]);
		let p = Predicate::In(ColRef::from("id"), vec![Value::Int4(2), Value::Int4(5)]);
		let Selection::Mask(m) = run(&t, &p) else {
			panic!("expected Mask selection");
		};
		assert_eq!(m.len(), 6);
		assert_eq!(m.count_set_bits(), 2);
		assert!(m.value(1));
		assert!(m.value(4));
	}
}
