// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, error::Error, value::Value};
use serde::de::Error as _;

use crate::{
	array::{
		boolean::BoolArray,
		canonical::{CanonicalArray, CanonicalStorage},
		fixed::FixedStorage,
	},
	compute::CompareOp,
};

// Produce a boolean canonical array where each row is `true` iff
// `array[row] <op> rhs`. None values in the input propagate to None in the
// output (RQL three-valued logic). Returns a `CanonicalArray` whose
// storage is `CanonicalStorage::Bool` and whose `ty` is `Type::Boolean`.
pub fn compare(array: &CanonicalArray, rhs: &Value, op: CompareOp) -> Result<CanonicalArray> {
	use reifydb_type::value::r#type::Type;

	let len = array.len();
	let mut out = BoolArray::new(len);

	match &array.storage {
		CanonicalStorage::Fixed(f) => compare_fixed(&f.storage, rhs, op, &mut out)?,
		CanonicalStorage::Bool(b) => {
			let r = match rhs {
				Value::Boolean(v) => *v,
				_ => return Err(Error::custom("compare: bool column requires Boolean rhs")),
			};
			for i in 0..len {
				out.set(i, apply_cmp_order(op, cmp_bool(b.get(i), r)));
			}
		}
		CanonicalStorage::VarLen(v) => {
			let r = match rhs {
				Value::Utf8(s) => s.as_bytes(),
				_ => return Err(Error::custom("compare: varlen column requires Utf8 rhs")),
			};
			for i in 0..len {
				out.set(i, apply_cmp_order(op, v.bytes_at(i).cmp(r)));
			}
		}
		CanonicalStorage::BigNum(_) => {
			return Err(Error::custom("compare: BigNum comparison not yet implemented"));
		}
	}

	// Propagate None: if the input row is None, the output is None.
	let new_nones = array.nones.clone();

	Ok(CanonicalArray::new(Type::Boolean, array.nullable, new_nones, CanonicalStorage::Bool(out)))
}

fn compare_fixed(storage: &FixedStorage, rhs: &Value, op: CompareOp, out: &mut BoolArray) -> Result<()> {
	macro_rules! branch {
		($variant:ident, $v:expr, $rhs_extract:expr) => {{
			let r = $rhs_extract;
			for (i, &lhs) in $v.iter().enumerate() {
				out.set(
					i,
					apply_cmp_order(op, lhs.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Less)),
				);
			}
		}};
	}
	macro_rules! cmp_int {
		($variant:ident, $v:expr, $ty:ty, $ext:ident) => {{
			let r = match rhs {
				Value::$ext(v) => *v as $ty,
				_ => {
					return Err(Error::custom(concat!(
						"compare: ",
						stringify!($variant),
						" column requires matching rhs"
					)));
				}
			};
			for (i, &lhs) in $v.iter().enumerate() {
				out.set(i, apply_cmp_order(op, lhs.cmp(&r)));
			}
		}};
	}
	match storage {
		FixedStorage::I8(v) => cmp_int!(I8, v, i8, Int1),
		FixedStorage::I16(v) => cmp_int!(I16, v, i16, Int2),
		FixedStorage::I32(v) => cmp_int!(I32, v, i32, Int4),
		FixedStorage::I64(v) => cmp_int!(I64, v, i64, Int8),
		FixedStorage::I128(v) => cmp_int!(I128, v, i128, Int16),
		FixedStorage::U8(v) => cmp_int!(U8, v, u8, Uint1),
		FixedStorage::U16(v) => cmp_int!(U16, v, u16, Uint2),
		FixedStorage::U32(v) => cmp_int!(U32, v, u32, Uint4),
		FixedStorage::U64(v) => cmp_int!(U64, v, u64, Uint8),
		FixedStorage::U128(v) => cmp_int!(U128, v, u128, Uint16),
		FixedStorage::F32(v) => {
			let r = match rhs {
				Value::Float4(v) => v.value(),
				_ => return Err(Error::custom("compare: F32 column requires Float4 rhs")),
			};
			branch!(F32, v, r);
		}
		FixedStorage::F64(v) => {
			let r = match rhs {
				Value::Float8(v) => v.value(),
				_ => return Err(Error::custom("compare: F64 column requires Float8 rhs")),
			};
			branch!(F64, v, r);
		}
	}
	Ok(())
}

fn cmp_bool(lhs: bool, rhs: bool) -> std::cmp::Ordering {
	(lhs as u8).cmp(&(rhs as u8))
}

fn apply_cmp_order(op: CompareOp, order: std::cmp::Ordering) -> bool {
	use std::cmp::Ordering::*;
	match op {
		CompareOp::Eq => matches!(order, Equal),
		CompareOp::Ne => !matches!(order, Equal),
		CompareOp::Lt => matches!(order, Less),
		CompareOp::LtEq => matches!(order, Less | Equal),
		CompareOp::Gt => matches!(order, Greater),
		CompareOp::GtEq => matches!(order, Greater | Equal),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::data::ColumnData;

	use super::*;

	#[test]
	fn compare_int4_equality() {
		let cd = ColumnData::int4([10i32, 20, 30, 20, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Eq).unwrap();
		let CanonicalStorage::Bool(b) = &out.storage else {
			panic!("expected bool storage");
		};
		assert!(!b.get(0));
		assert!(b.get(1));
		assert!(!b.get(2));
		assert!(b.get(3));
		assert!(!b.get(4));
	}

	#[test]
	fn compare_int4_greater_than() {
		let cd = ColumnData::int4([10i32, 20, 30, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = compare(&ca, &Value::Int4(20), CompareOp::Gt).unwrap();
		let CanonicalStorage::Bool(b) = &out.storage else {
			panic!("expected bool storage");
		};
		assert!(!b.get(0));
		assert!(!b.get(1));
		assert!(b.get(2));
		assert!(b.get(3));
	}
}
