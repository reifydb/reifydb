// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::Debug;

use reifydb_type::{
	storage::{DataBitVec, DataVec},
	util::bitvec::BitVec,
	value::{
		Value,
		container::{
			bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		duration::Duration,
		is::{IsNumber, IsTemporal, IsUuid},
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::ColumnBuffer;

impl ColumnBuffer {
	/// Merge two columns by mask: row `i` gets `self[i]` if `then_mask[i]`,
	/// `other[i]` if `else_mask[i]`, `None` otherwise. Callers must satisfy
	/// `self.len() >= total_len` and `other.len() >= total_len`.
	///
	/// Fast path: when both operands share a bare variant (Bool / numeric /
	/// temporal / Uuid), a typed kernel writes directly into a preallocated
	/// buffer, skipping the `Value` enum round-trip. If any row is unmapped by
	/// both masks, the kernel returns an `Option`-wrapped result with the
	/// validity bitmap set accordingly.
	///
	/// Fallback: mismatched variants, `Option`-wrapped operands, and
	/// variable-width variants (`Utf8`, `Blob`, etc.) go through a generic
	/// row-by-row path.
	pub fn scatter_merge(
		&self,
		other: &ColumnBuffer,
		then_mask: &BitVec,
		else_mask: &BitVec,
		total_len: usize,
	) -> ColumnBuffer {
		if let (
			ColumnBuffer::Option {
				inner: a_inner,
				bitvec: a_bv,
			},
			ColumnBuffer::Option {
				inner: b_inner,
				bitvec: b_bv,
			},
		) = (self, other)
		{
			let merged_inner = a_inner.scatter_merge(b_inner, then_mask, else_mask, total_len);
			let merged_bv = merge_validity_bitvecs(a_bv, b_bv, then_mask, else_mask, total_len);
			return match merged_inner {
				ColumnBuffer::Option {
					inner: nested_inner,
					bitvec: nested_bv,
				} => ColumnBuffer::Option {
					inner: nested_inner,
					bitvec: merged_bv.and(&nested_bv),
				},
				inner => ColumnBuffer::Option {
					inner: Box::new(inner),
					bitvec: merged_bv,
				},
			};
		}

		if let Some(result) = scatter_merge_typed(self, other, then_mask, else_mask, total_len) {
			return result;
		}

		scatter_merge_generic(self, other, then_mask, else_mask, total_len)
	}
}

fn merge_validity_bitvecs(
	then_bv: &BitVec,
	else_bv: &BitVec,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> BitVec {
	let mut out = BitVec::with_capacity(total_len);
	for i in 0..total_len {
		let bit = if DataBitVec::get(then_mask, i) {
			i < DataBitVec::len(then_bv) && DataBitVec::get(then_bv, i)
		} else if DataBitVec::get(else_mask, i) {
			i < DataBitVec::len(else_bv) && DataBitVec::get(else_bv, i)
		} else {
			false
		};
		DataBitVec::push(&mut out, bit);
	}
	out
}

fn scatter_merge_generic(
	self_col: &ColumnBuffer,
	other: &ColumnBuffer,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> ColumnBuffer {
	let result_type = self_col.get_type();
	let mut data = ColumnBuffer::with_capacity(result_type.clone(), total_len);
	for i in 0..total_len {
		if DataBitVec::get(then_mask, i) {
			data.push_value(self_col.get_value(i));
		} else if DataBitVec::get(else_mask, i) {
			data.push_value(other.get_value(i));
		} else {
			data.push_value(Value::none_of(result_type.clone()));
		}
	}
	data
}

/// Fast-path typed scatter merge. Returns `None` if the variant pair isn't
/// supported by a typed kernel; callers fall back to the generic path.
fn scatter_merge_typed(
	self_col: &ColumnBuffer,
	other: &ColumnBuffer,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> Option<ColumnBuffer> {
	macro_rules! number_kernel {
		($variant:ident, $t:ty) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) = number_scatter::<$t>(a, b, then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant(NumberContainer::new(data));
				return Some(finalize(inner, validity));
			}
		};
	}
	macro_rules! temporal_kernel {
		($variant:ident, $t:ty) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) = temporal_scatter::<$t>(a, b, then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant(TemporalContainer::new(data));
				return Some(finalize(inner, validity));
			}
		};
	}
	macro_rules! uuid_kernel {
		($variant:ident, $t:ty) => {
			if let (ColumnBuffer::$variant(a), ColumnBuffer::$variant(b)) = (self_col, other) {
				let (data, validity) = uuid_scatter::<$t>(a, b, then_mask, else_mask, total_len);
				let inner = ColumnBuffer::$variant(UuidContainer::new(data));
				return Some(finalize(inner, validity));
			}
		};
	}

	if let (ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) = (self_col, other) {
		let (data, validity) = bool_scatter(a, b, then_mask, else_mask, total_len);
		let inner = ColumnBuffer::Bool(BoolContainer::from_parts(data));
		return Some(finalize(inner, validity));
	}

	number_kernel!(Float4, f32);
	number_kernel!(Float8, f64);
	number_kernel!(Int1, i8);
	number_kernel!(Int2, i16);
	number_kernel!(Int4, i32);
	number_kernel!(Int8, i64);
	number_kernel!(Int16, i128);
	number_kernel!(Uint1, u8);
	number_kernel!(Uint2, u16);
	number_kernel!(Uint4, u32);
	number_kernel!(Uint8, u64);
	number_kernel!(Uint16, u128);

	temporal_kernel!(Date, Date);
	temporal_kernel!(DateTime, DateTime);
	temporal_kernel!(Time, Time);
	temporal_kernel!(Duration, Duration);

	uuid_kernel!(Uuid4, Uuid4);
	uuid_kernel!(Uuid7, Uuid7);

	None
}

fn finalize(inner: ColumnBuffer, validity: Option<BitVec>) -> ColumnBuffer {
	match validity {
		Some(bv) => ColumnBuffer::Option {
			inner: Box::new(inner),
			bitvec: bv,
		},
		None => inner,
	}
}

fn bool_scatter(
	a: &BoolContainer,
	b: &BoolContainer,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> (BitVec, Option<BitVec>) {
	let a_data = a.data();
	let b_data = b.data();
	let mut out = BitVec::with_capacity(total_len);
	let mut validity: Option<BitVec> = None;
	for i in 0..total_len {
		let in_then = DataBitVec::get(then_mask, i);
		let in_else = !in_then && DataBitVec::get(else_mask, i);
		let bit = if in_then && i < DataBitVec::len(a_data) {
			DataBitVec::get(a_data, i)
		} else if in_else && i < DataBitVec::len(b_data) {
			DataBitVec::get(b_data, i)
		} else {
			false
		};
		DataBitVec::push(&mut out, bit);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BitVec::with_capacity(total_len);
				for _ in 0..i {
					DataBitVec::push(&mut bv, true);
				}
				bv
			});
			DataBitVec::push(v, false);
		} else if let Some(v) = validity.as_mut() {
			DataBitVec::push(v, true);
		}
	}
	(out, validity)
}

fn number_scatter<T>(
	a: &NumberContainer<T>,
	b: &NumberContainer<T>,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> (Vec<T>, Option<BitVec>)
where
	T: IsNumber + Clone + Default + Debug,
{
	let a_data = a.data();
	let b_data = b.data();
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BitVec> = None;
	for i in 0..total_len {
		let in_then = DataBitVec::get(then_mask, i);
		let in_else = !in_then && DataBitVec::get(else_mask, i);
		let value = if in_then {
			DataVec::get(a_data, i).cloned().unwrap_or_default()
		} else if in_else {
			DataVec::get(b_data, i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BitVec::with_capacity(total_len);
				for _ in 0..i {
					DataBitVec::push(&mut bv, true);
				}
				bv
			});
			DataBitVec::push(v, false);
		} else if let Some(v) = validity.as_mut() {
			DataBitVec::push(v, true);
		}
	}
	(out, validity)
}

fn temporal_scatter<T>(
	a: &TemporalContainer<T>,
	b: &TemporalContainer<T>,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> (Vec<T>, Option<BitVec>)
where
	T: IsTemporal + Clone + Default + Debug,
{
	let a_data = a.data();
	let b_data = b.data();
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BitVec> = None;
	for i in 0..total_len {
		let in_then = DataBitVec::get(then_mask, i);
		let in_else = !in_then && DataBitVec::get(else_mask, i);
		let value = if in_then {
			DataVec::get(a_data, i).cloned().unwrap_or_default()
		} else if in_else {
			DataVec::get(b_data, i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BitVec::with_capacity(total_len);
				for _ in 0..i {
					DataBitVec::push(&mut bv, true);
				}
				bv
			});
			DataBitVec::push(v, false);
		} else if let Some(v) = validity.as_mut() {
			DataBitVec::push(v, true);
		}
	}
	(out, validity)
}

fn uuid_scatter<T>(
	a: &UuidContainer<T>,
	b: &UuidContainer<T>,
	then_mask: &BitVec,
	else_mask: &BitVec,
	total_len: usize,
) -> (Vec<T>, Option<BitVec>)
where
	T: IsUuid + Clone + Default + Debug,
{
	let a_data = a.data();
	let b_data = b.data();
	let mut out: Vec<T> = Vec::with_capacity(total_len);
	let mut validity: Option<BitVec> = None;
	for i in 0..total_len {
		let in_then = DataBitVec::get(then_mask, i);
		let in_else = !in_then && DataBitVec::get(else_mask, i);
		let value = if in_then {
			DataVec::get(a_data, i).cloned().unwrap_or_default()
		} else if in_else {
			DataVec::get(b_data, i).cloned().unwrap_or_default()
		} else {
			T::default()
		};
		out.push(value);
		if !in_then && !in_else {
			let v = validity.get_or_insert_with(|| {
				let mut bv = BitVec::with_capacity(total_len);
				for _ in 0..i {
					DataBitVec::push(&mut bv, true);
				}
				bv
			});
			DataBitVec::push(v, false);
		} else if let Some(v) = validity.as_mut() {
			DataBitVec::push(v, true);
		}
	}
	(out, validity)
}

#[cfg(test)]
mod tests {
	use reifydb_type::{util::bitvec::BitVec, value::Value};

	use crate::value::column::ColumnBuffer;

	#[test]
	fn scatter_merge_all_mapped_int4() {
		let a = ColumnBuffer::int4([10, 20, 30, 40]);
		let b = ColumnBuffer::int4([90, 80, 70, 60]);
		let then_mask = BitVec::from_slice(&[true, false, true, false]);
		let else_mask = BitVec::from_slice(&[false, true, false, true]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 4);
		assert!(matches!(merged, ColumnBuffer::Int4(_)));
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::Int4(80));
		assert_eq!(merged.get_value(2), Value::Int4(30));
		assert_eq!(merged.get_value(3), Value::Int4(60));
	}

	#[test]
	fn scatter_merge_unmapped_promotes_to_option() {
		let a = ColumnBuffer::int4([10, 20, 30]);
		let b = ColumnBuffer::int4([90, 80, 70]);
		// Row 1 is in neither mask - should yield None.
		let then_mask = BitVec::from_slice(&[true, false, true]);
		let else_mask = BitVec::from_slice(&[false, false, false]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 3);
		assert!(matches!(merged, ColumnBuffer::Option { .. }));
		assert_eq!(merged.get_value(0), Value::Int4(10));
		assert_eq!(merged.get_value(1), Value::none());
		assert_eq!(merged.get_value(2), Value::Int4(30));
	}

	#[test]
	fn scatter_merge_bool_all_mapped() {
		let a = ColumnBuffer::bool([true, true, false, false]);
		let b = ColumnBuffer::bool([false, false, true, true]);
		let then_mask = BitVec::from_slice(&[true, false, true, false]);
		let else_mask = BitVec::from_slice(&[false, true, false, true]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 4);
		assert!(matches!(merged, ColumnBuffer::Bool(_)));
		assert_eq!(merged.get_value(0), Value::Boolean(true));
		assert_eq!(merged.get_value(1), Value::Boolean(false));
		assert_eq!(merged.get_value(2), Value::Boolean(false));
		assert_eq!(merged.get_value(3), Value::Boolean(true));
	}

	#[test]
	fn scatter_merge_utf8_uses_generic_fallback() {
		let a = ColumnBuffer::utf8(["a", "b", "c"]);
		let b = ColumnBuffer::utf8(["x", "y", "z"]);
		let then_mask = BitVec::from_slice(&[true, false, true]);
		let else_mask = BitVec::from_slice(&[false, true, false]);

		let merged = a.scatter_merge(&b, &then_mask, &else_mask, 3);
		assert_eq!(merged.get_value(0), Value::Utf8("a".to_string()));
		assert_eq!(merged.get_value(1), Value::Utf8("y".to_string()));
		assert_eq!(merged.get_value(2), Value::Utf8("c".to_string()));
	}
}
