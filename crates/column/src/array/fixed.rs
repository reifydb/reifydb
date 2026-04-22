// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

// Typed backing storage for `FixedArray`. Each variant holds a `Vec<T>` with
// `T`'s natural alignment; this matters for `try_as_slice<T>` to hand out a
// correctly aligned `&[T]` without reinterpret-casting a `Vec<u8>`.
#[derive(Clone, Debug)]
pub enum FixedStorage {
	I8(Vec<i8>),
	I16(Vec<i16>),
	I32(Vec<i32>),
	I64(Vec<i64>),
	I128(Vec<i128>),
	U8(Vec<u8>),
	U16(Vec<u16>),
	U32(Vec<u32>),
	U64(Vec<u64>),
	U128(Vec<u128>),
	F32(Vec<f32>),
	F64(Vec<f64>),
}

impl FixedStorage {
	pub fn len(&self) -> usize {
		match self {
			Self::I8(v) => v.len(),
			Self::I16(v) => v.len(),
			Self::I32(v) => v.len(),
			Self::I64(v) => v.len(),
			Self::I128(v) => v.len(),
			Self::U8(v) => v.len(),
			Self::U16(v) => v.len(),
			Self::U32(v) => v.len(),
			Self::U64(v) => v.len(),
			Self::U128(v) => v.len(),
			Self::F32(v) => v.len(),
			Self::F64(v) => v.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[derive(Clone, Debug)]
pub struct FixedArray {
	pub ty: Type,
	pub storage: FixedStorage,
}

impl FixedArray {
	pub fn new(ty: Type, storage: FixedStorage) -> Self {
		Self {
			ty,
			storage,
		}
	}

	pub fn len(&self) -> usize {
		self.storage.len()
	}

	pub fn is_empty(&self) -> bool {
		self.storage.is_empty()
	}

	// Borrow the backing storage as `&[T]` when `T` matches this array's
	// logical `Type`; returns `None` on mismatch. Lets canonical kernels
	// hand a plain slice to a tight loop so the Rust compiler can auto-
	// vectorize without hand-written SIMD.
	pub fn try_as_slice<T: Primitive>(&self) -> Option<&[T]> {
		if T::TYPE != self.ty {
			return None;
		}
		T::extract_slice(&self.storage)
	}
}

// Sealed marker for primitive-width types that back `FixedArray`. The
// `extract_slice` hook lets `try_as_slice<T>` dispatch on the storage variant
// in a compile-time-dispatched way.
pub trait Primitive: Copy + Send + Sync + 'static + sealed::Sealed {
	const TYPE: Type;
	fn extract_slice(storage: &FixedStorage) -> Option<&[Self]>;
}

mod sealed {
	pub trait Sealed {}
}

macro_rules! impl_primitive {
	($t:ty, $logical:expr, $variant:ident) => {
		impl sealed::Sealed for $t {}
		impl Primitive for $t {
			const TYPE: Type = $logical;
			fn extract_slice(storage: &FixedStorage) -> Option<&[Self]> {
				match storage {
					FixedStorage::$variant(v) => Some(v.as_slice()),
					_ => None,
				}
			}
		}
	};
}

impl_primitive!(i8, Type::Int1, I8);
impl_primitive!(i16, Type::Int2, I16);
impl_primitive!(i32, Type::Int4, I32);
impl_primitive!(i64, Type::Int8, I64);
impl_primitive!(i128, Type::Int16, I128);
impl_primitive!(u8, Type::Uint1, U8);
impl_primitive!(u16, Type::Uint2, U16);
impl_primitive!(u32, Type::Uint4, U32);
impl_primitive!(u64, Type::Uint8, U64);
impl_primitive!(u128, Type::Uint16, U128);
impl_primitive!(f32, Type::Float4, F32);
impl_primitive!(f64, Type::Float8, F64);

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn try_as_slice_matches_type() {
		let a = FixedArray::new(Type::Int4, FixedStorage::I32(vec![1, 2, 3]));
		let s: &[i32] = a.try_as_slice::<i32>().unwrap();
		assert_eq!(s, &[1, 2, 3]);
	}

	#[test]
	fn try_as_slice_rejects_wrong_type() {
		let a = FixedArray::new(Type::Int4, FixedStorage::I32(vec![1, 2, 3]));
		assert!(a.try_as_slice::<i64>().is_none());
		assert!(a.try_as_slice::<u32>().is_none());
	}
}
