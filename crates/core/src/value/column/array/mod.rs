// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod canonical;

use std::{any::Any, sync::Arc};

use canonical::Canonical;
use reifydb_type::{
	Result,
	util::bitvec::BitVec,
	value::{Value, r#type::Type},
};

use crate::value::column::{
	buffer::ColumnBuffer, encoding::EncodingId, mask::RowMask, nones::NoneBitmap, stats::StatsSet,
};

// Comparison operator used by `ColumnData::compare`. Kept next to the trait
// because the read-side kernels (filter, compare) live on the trait itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
	Eq,
	Ne,
	Lt,
	LtEq,
	Gt,
	GtEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchResult {
	Found(usize),
	NotFound(usize),
}

// Polymorphic read interface for any column representation. `Canonical` is the
// identity-encoded impl (holds a `ColumnBuffer` directly); compressed encodings
// defined in `reifydb-column` (`ColumnConstant`, `ColumnRle`, etc.) implement
// this trait with encoding-specific specializations for the read operators.
//
// All read operators have default impls that materialize via `to_canonical`
// then delegate to the canonical implementation on the inner `ColumnBuffer` -
// compressed encodings override for fast paths that avoid materialization.
pub trait ColumnData: Send + Sync + 'static {
	fn ty(&self) -> Type;
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool {
		self.len() == 0
	}
	fn encoding(&self) -> EncodingId;

	fn is_nullable(&self) -> bool;
	fn nones(&self) -> Option<&NoneBitmap>;
	fn is_defined(&self, idx: usize) -> bool {
		!self.nones().map(|n| n.is_none(idx)).unwrap_or(false)
	}

	fn stats(&self) -> &StatsSet;

	fn get_value(&self, idx: usize) -> Value;
	fn iter(&self) -> Box<dyn Iterator<Item = Value> + '_> {
		Box::new((0..self.len()).map(move |i| self.get_value(i)))
	}
	fn as_string(&self, idx: usize) -> String;

	fn as_any(&self) -> &dyn Any;
	fn as_any_mut(&mut self) -> &mut dyn Any;

	fn children(&self) -> &[Column];
	fn metadata(&self) -> &dyn Any;

	fn to_canonical(&self) -> Result<Arc<Canonical>>;

	// Default read operators: materialize then run the canonical algorithm over
	// the inner `ColumnBuffer`. Compressed encodings override these for fast paths.
	fn filter(&self, mask: &RowMask) -> Result<Column> {
		let canon = self.to_canonical()?;
		Ok(Column::from_canonical(canonical_filter(&canon, mask)?))
	}

	fn take(&self, indices: &Column) -> Result<Column> {
		let canon = self.to_canonical()?;
		let idx = canon_indices(indices)?;
		Ok(Column::from_canonical(canonical_take(&canon, &idx)?))
	}

	fn slice(&self, start: usize, end: usize) -> Result<Column> {
		let canon = self.to_canonical()?;
		Ok(Column::from_canonical(canonical_slice(&canon, start, end)?))
	}
}

#[derive(Clone)]
pub struct Column(Arc<dyn ColumnData>);

impl Column {
	pub fn from_data(data: Arc<dyn ColumnData>) -> Self {
		Self(data)
	}

	pub fn from_canonical(canon: Canonical) -> Self {
		Self(Arc::new(canon))
	}

	pub fn from_column_buffer(buffer: ColumnBuffer) -> Self {
		Self::from_canonical(Canonical::from_buffer(buffer))
	}

	pub fn data(&self) -> &dyn ColumnData {
		&*self.0
	}

	pub fn ty(&self) -> Type {
		self.0.ty()
	}

	pub fn is_nullable(&self) -> bool {
		self.0.is_nullable()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn encoding(&self) -> EncodingId {
		self.0.encoding()
	}

	pub fn stats(&self) -> &StatsSet {
		self.0.stats()
	}

	pub fn nones(&self) -> Option<&NoneBitmap> {
		self.0.nones()
	}

	pub fn is_defined(&self, idx: usize) -> bool {
		self.0.is_defined(idx)
	}

	pub fn get_value(&self, idx: usize) -> Value {
		self.0.get_value(idx)
	}

	pub fn iter(&self) -> Box<dyn Iterator<Item = Value> + '_> {
		self.0.iter()
	}

	pub fn as_string(&self, idx: usize) -> String {
		self.0.as_string(idx)
	}

	pub fn children(&self) -> &[Column] {
		self.0.children()
	}

	pub fn metadata(&self) -> &dyn Any {
		self.0.metadata()
	}

	pub fn to_canonical(&self) -> Result<Arc<Canonical>> {
		self.0.to_canonical()
	}

	pub fn filter(&self, mask: &RowMask) -> Result<Column> {
		self.0.filter(mask)
	}

	pub fn take(&self, indices: &Column) -> Result<Column> {
		self.0.take(indices)
	}

	pub fn slice(&self, start: usize, end: usize) -> Result<Column> {
		self.0.slice(start, end)
	}

	// Return `&mut Canonical`, materializing from a compressed encoding if needed.
	pub fn materialize(&mut self) -> Result<&mut Canonical> {
		if Arc::get_mut(&mut self.0).map(|d| d.as_any().is::<Canonical>()).unwrap_or(false) {
			let d = Arc::get_mut(&mut self.0).unwrap();
			return Ok(d.as_any_mut().downcast_mut::<Canonical>().unwrap());
		}
		let canonical_arc = self.0.to_canonical()?;
		let owned = Arc::try_unwrap(canonical_arc).unwrap_or_else(|arc| (*arc).clone());
		self.0 = Arc::new(owned);
		let d = Arc::get_mut(&mut self.0).unwrap();
		Ok(d.as_any_mut().downcast_mut::<Canonical>().unwrap())
	}
}

// Default compute primitives for canonical columns. These are used by the
// default `filter`/`take`/`slice` impls on the `ColumnData` trait and are
// self-contained within reifydb-core (no dependency on reifydb-column).

fn canonical_filter(canon: &Canonical, mask: &RowMask) -> Result<Canonical> {
	assert_eq!(canon.len(), mask.len(), "filter: length mismatch");
	let kept = mask.popcount();

	let new_nones = canon.nones.as_ref().map(|n| {
		let mut out = NoneBitmap::all_present(kept);
		let mut j = 0usize;
		for i in 0..n.len() {
			if mask.get(i) {
				if n.is_none(i) {
					out.set_none(j);
				}
				j += 1;
			}
		}
		out
	});

	let mut new_buffer = canon.buffer.clone();
	new_buffer.filter(&row_mask_to_bitvec(mask))?;

	Ok(Canonical::new(canon.ty.clone(), canon.nullable, new_nones, new_buffer))
}

fn canonical_take(canon: &Canonical, indices: &[usize]) -> Result<Canonical> {
	let new_nones = canon.nones.as_ref().map(|n| {
		let mut out = NoneBitmap::all_present(indices.len());
		for (j, &i) in indices.iter().enumerate() {
			if n.is_none(i) {
				out.set_none(j);
			}
		}
		out
	});
	let new_buffer = canon.buffer.gather(indices);
	Ok(Canonical::new(canon.ty.clone(), canon.nullable, new_nones, new_buffer))
}

fn canonical_slice(canon: &Canonical, start: usize, end: usize) -> Result<Canonical> {
	assert!(start <= end);
	assert!(end <= canon.len());
	let new_nones = canon.nones.as_ref().map(|n| {
		let count = end - start;
		let mut out = NoneBitmap::all_present(count);
		for i in 0..count {
			if n.is_none(start + i) {
				out.set_none(i);
			}
		}
		out
	});
	let new_buffer = canon.buffer.slice(start, end);
	Ok(Canonical::new(canon.ty.clone(), canon.nullable, new_nones, new_buffer))
}

fn row_mask_to_bitvec(mask: &RowMask) -> BitVec {
	let mut bits = Vec::with_capacity(mask.len());
	for i in 0..mask.len() {
		bits.push(mask.get(i));
	}
	BitVec::from(bits)
}

fn canon_indices(indices: &Column) -> Result<Vec<usize>> {
	let canon = indices.to_canonical()?;
	let len = canon.len();
	let mut out = Vec::with_capacity(len);
	for i in 0..len {
		let v = canon.buffer.get_value(i);
		let n: usize = match v {
			Value::Uint1(n) => n as usize,
			Value::Uint2(n) => n as usize,
			Value::Uint4(n) => n as usize,
			Value::Uint8(n) => n as usize,
			Value::Int4(n) => n as usize,
			Value::Int8(n) => n as usize,
			_ => panic!("take: indices must be fixed-width unsigned/signed int"),
		};
		out.push(n);
	}
	Ok(out)
}
