// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{any::Any, ops::Deref, sync::Arc};

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	Result,
	storage::{Cow, DataBitVec, Storage},
	util::bitvec::BitVec,
	value::{Value, blob::Blob, r#type::Type},
};

use crate::{
	array::{
		Array, ArrayData,
		bignum::{BigNum, BigNumArray},
		boolean::BoolArray,
		fixed::{FixedArray, FixedStorage},
		varlen::VarLenArray,
	},
	encoding::EncodingId,
	error::ColumnError,
	nones::NoneBitmap,
	stats::StatsSet,
};

#[derive(Clone, Debug)]
pub enum CanonicalStorage {
	Bool(BoolArray),
	Fixed(FixedArray),
	VarLen(VarLenArray),
	BigNum(BigNumArray),
}

impl CanonicalStorage {
	pub fn len(&self) -> usize {
		match self {
			Self::Bool(b) => b.len(),
			Self::Fixed(f) => f.len(),
			Self::VarLen(v) => v.len(),
			Self::BigNum(b) => b.len(),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[derive(Clone, Debug)]
pub struct CanonicalArray {
	pub ty: Type,
	pub nullable: bool,
	pub nones: Option<NoneBitmap>,
	pub storage: CanonicalStorage,
	stats: StatsSet,
}

impl CanonicalArray {
	pub fn new(ty: Type, nullable: bool, nones: Option<NoneBitmap>, storage: CanonicalStorage) -> Self {
		Self {
			ty,
			nullable,
			nones,
			storage,
			stats: StatsSet::new(),
		}
	}

	pub fn len(&self) -> usize {
		self.storage.len()
	}

	pub fn is_empty(&self) -> bool {
		self.storage.is_empty()
	}

	pub fn stats(&self) -> &StatsSet {
		&self.stats
	}

	pub fn slice(&self, start: usize, end: usize) -> Result<Self> {
		crate::compute::canonical::slice::slice(self, start, end)
	}

	// Bridge from ReifyDB's `ColumnData` into a canonical columnar array.
	// `ColumnData::Option { inner, bitvec }` wraps a non-nullable inner with a
	// definedness bitmap - set bit in `bitvec` means the row is defined, cleared
	// means None. Our `NoneBitmap` uses the opposite convention (set bit = None),
	// so the bridge inverts per-row.
	pub fn from_column_data(cd: &ColumnData) -> Result<Self> {
		match cd {
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				let mut inner_ca = Self::from_column_data(inner)?;
				let len = inner_ca.len();
				let mut nones = NoneBitmap::all_present(len);
				for row in 0..len {
					if !<<Cow as Storage>::BitVec as DataBitVec>::get(bitvec, row) {
						nones.set_none(row);
					}
				}
				inner_ca.nullable = true;
				inner_ca.nones = Some(nones);
				Ok(inner_ca)
			}
			ColumnData::Bool(c) => {
				let bv = c.deref();
				let mut ba = BoolArray::new(bv.len());
				for i in 0..bv.len() {
					ba.set(i, bv.get(i));
				}
				Ok(Self::new(Type::Boolean, false, None, CanonicalStorage::Bool(ba)))
			}
			ColumnData::Int1(c) => Ok(Self::fixed(Type::Int1, FixedStorage::I8(c.deref().to_vec()))),
			ColumnData::Int2(c) => Ok(Self::fixed(Type::Int2, FixedStorage::I16(c.deref().to_vec()))),
			ColumnData::Int4(c) => Ok(Self::fixed(Type::Int4, FixedStorage::I32(c.deref().to_vec()))),
			ColumnData::Int8(c) => Ok(Self::fixed(Type::Int8, FixedStorage::I64(c.deref().to_vec()))),
			ColumnData::Int16(c) => Ok(Self::fixed(Type::Int16, FixedStorage::I128(c.deref().to_vec()))),
			ColumnData::Uint1(c) => Ok(Self::fixed(Type::Uint1, FixedStorage::U8(c.deref().to_vec()))),
			ColumnData::Uint2(c) => Ok(Self::fixed(Type::Uint2, FixedStorage::U16(c.deref().to_vec()))),
			ColumnData::Uint4(c) => Ok(Self::fixed(Type::Uint4, FixedStorage::U32(c.deref().to_vec()))),
			ColumnData::Uint8(c) => Ok(Self::fixed(Type::Uint8, FixedStorage::U64(c.deref().to_vec()))),
			ColumnData::Uint16(c) => Ok(Self::fixed(Type::Uint16, FixedStorage::U128(c.deref().to_vec()))),
			ColumnData::Float4(c) => Ok(Self::fixed(Type::Float4, FixedStorage::F32(c.deref().to_vec()))),
			ColumnData::Float8(c) => Ok(Self::fixed(Type::Float8, FixedStorage::F64(c.deref().to_vec()))),
			ColumnData::Utf8 {
				container,
				..
			} => {
				let slice: &[String] = container.deref();
				let va = VarLenArray::from_strings(Type::Utf8, slice.iter().cloned());
				Ok(Self::new(Type::Utf8, false, None, CanonicalStorage::VarLen(va)))
			}
			ColumnData::Blob {
				container,
				..
			} => {
				let mut va = VarLenArray::new(Type::Blob);
				for i in 0..container.len() {
					va.push_bytes(container.get(i).map(|b| b.as_ref()).unwrap_or(&[]));
				}
				Ok(Self::new(Type::Blob, false, None, CanonicalStorage::VarLen(va)))
			}
			ColumnData::Int {
				container,
				..
			} => Ok(Self::bignum(Type::Int, collect_bignums(container.len(), |i| container.get_value(i)))),
			ColumnData::Uint {
				container,
				..
			} => Ok(Self::bignum(Type::Uint, collect_bignums(container.len(), |i| container.get_value(i)))),
			ColumnData::Decimal {
				container,
				..
			} => Ok(Self::bignum(
				Type::Decimal,
				collect_bignums(container.len(), |i| container.get_value(i)),
			)),
			other => Err(unsupported_for_now(other).into()),
		}
	}

	fn fixed(ty: Type, storage: FixedStorage) -> Self {
		let fa = FixedArray::new(ty.clone(), storage);
		Self::new(ty, false, None, CanonicalStorage::Fixed(fa))
	}

	fn bignum(ty: Type, values: Vec<BigNum>) -> Self {
		let ba = BigNumArray::from_values(ty.clone(), values);
		Self::new(ty, false, None, CanonicalStorage::BigNum(ba))
	}

	// Inverse of `from_column_data`. `NoneBitmap` uses set bit = None; the
	// `ColumnData::Option.bitvec` wrapping uses set bit = defined, so the
	// per-row polarity is inverted when rebuilding the definedness bitvec.
	pub fn to_column_data(&self) -> Result<ColumnData> {
		let inner = match &self.storage {
			CanonicalStorage::Bool(b) => {
				let values: Vec<bool> = (0..b.len()).map(|i| b.get(i)).collect();
				ColumnData::bool(values)
			}
			CanonicalStorage::Fixed(f) => match &f.storage {
				FixedStorage::I8(v) => ColumnData::int1(v.clone()),
				FixedStorage::I16(v) => ColumnData::int2(v.clone()),
				FixedStorage::I32(v) => ColumnData::int4(v.clone()),
				FixedStorage::I64(v) => ColumnData::int8(v.clone()),
				FixedStorage::I128(v) => ColumnData::int16(v.clone()),
				FixedStorage::U8(v) => ColumnData::uint1(v.clone()),
				FixedStorage::U16(v) => ColumnData::uint2(v.clone()),
				FixedStorage::U32(v) => ColumnData::uint4(v.clone()),
				FixedStorage::U64(v) => ColumnData::uint8(v.clone()),
				FixedStorage::U128(v) => ColumnData::uint16(v.clone()),
				FixedStorage::F32(v) => ColumnData::float4(v.clone()),
				FixedStorage::F64(v) => ColumnData::float8(v.clone()),
			},
			CanonicalStorage::VarLen(v) => match self.ty {
				Type::Utf8 => {
					let strings: Vec<String> = (0..v.len())
						.map(|i| std::str::from_utf8(v.bytes_at(i)).map(str::to_string))
						.collect::<std::result::Result<_, _>>()
						.map_err(|e| ColumnError::ToColumnDataInvalidUtf8 {
							reason: e.to_string(),
						})?;
					ColumnData::utf8(strings)
				}
				Type::Blob => {
					let blobs: Vec<Blob> =
						(0..v.len()).map(|i| Blob::from(v.bytes_at(i).to_vec())).collect();
					ColumnData::blob(blobs)
				}
				ref other => {
					return Err(ColumnError::ToColumnDataUnexpectedVarLen {
						ty: other.clone(),
					}
					.into());
				}
			},
			CanonicalStorage::BigNum(b) => match self.ty {
				Type::Int => ColumnData::int(b.values.iter().map(|n| match n {
					BigNum::Int(v) => v.clone(),
					other => unreachable!("BigNum ty=Int mismatched variant: {other:?}"),
				})),
				Type::Uint => ColumnData::uint(b.values.iter().map(|n| match n {
					BigNum::Uint(v) => v.clone(),
					other => unreachable!("BigNum ty=Uint mismatched variant: {other:?}"),
				})),
				Type::Decimal => ColumnData::decimal(b.values.iter().map(|n| match n {
					BigNum::Decimal(v) => v.clone(),
					other => unreachable!("BigNum ty=Decimal mismatched variant: {other:?}"),
				})),
				ref other => {
					return Err(ColumnError::ToColumnDataUnexpectedBigNum {
						ty: other.clone(),
					}
					.into());
				}
			},
		};

		match &self.nones {
			Some(nones) => {
				let len = self.len();
				let mut bits = Vec::with_capacity(len);
				for row in 0..len {
					bits.push(!nones.is_none(row));
				}
				Ok(ColumnData::Option {
					inner: Box::new(inner),
					bitvec: BitVec::from(bits),
				})
			}
			None => Ok(inner),
		}
	}
}

fn collect_bignums(len: usize, mut get: impl FnMut(usize) -> Value) -> Vec<BigNum> {
	let mut out = Vec::with_capacity(len);
	for i in 0..len {
		out.push(BigNum::from(get(i)));
	}
	out
}

fn unsupported_for_now(cd: &ColumnData) -> ColumnError {
	let variant = match cd {
		ColumnData::Date(_) => "Date",
		ColumnData::DateTime(_) => "DateTime",
		ColumnData::Time(_) => "Time",
		ColumnData::Duration(_) => "Duration",
		ColumnData::Uuid4(_) => "Uuid4",
		ColumnData::Uuid7(_) => "Uuid7",
		ColumnData::IdentityId(_) => "IdentityId",
		ColumnData::Any(_) => "Any",
		ColumnData::DictionaryId(_) => "DictionaryId",
		_ => "Unknown",
	};
	ColumnError::FromColumnDataUnsupported {
		variant,
	}
}

static UNIT_METADATA: () = ();
static EMPTY_CHILDREN: Vec<Array> = Vec::new();

impl ArrayData for CanonicalArray {
	fn ty(&self) -> Type {
		self.ty.clone()
	}

	fn is_nullable(&self) -> bool {
		self.nullable
	}

	fn len(&self) -> usize {
		CanonicalArray::len(self)
	}

	fn encoding(&self) -> EncodingId {
		match &self.storage {
			CanonicalStorage::Bool(_) => EncodingId::CANONICAL_BOOL,
			CanonicalStorage::Fixed(_) => EncodingId::CANONICAL_FIXED,
			CanonicalStorage::VarLen(_) => EncodingId::CANONICAL_VARLEN,
			CanonicalStorage::BigNum(_) => EncodingId::CANONICAL_BIGNUM,
		}
	}

	fn stats(&self) -> &StatsSet {
		&self.stats
	}

	fn nones(&self) -> Option<&NoneBitmap> {
		self.nones.as_ref()
	}

	fn children(&self) -> &[Array] {
		&EMPTY_CHILDREN
	}

	fn metadata(&self) -> &dyn Any {
		&UNIT_METADATA
	}

	fn to_canonical(&self) -> Result<Arc<CanonicalArray>> {
		Ok(Arc::new(self.clone()))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	#[test]
	fn from_column_data_preserves_int4_values() {
		let cd = ColumnData::int4([10i32, 20, 30, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(ca.len(), 4);
		assert_eq!(ca.ty, Type::Int4);
		assert!(!ca.nullable);
		assert!(ca.nones.is_none());
		let CanonicalStorage::Fixed(f) = &ca.storage else {
			panic!("expected Fixed storage");
		};
		assert_eq!(f.try_as_slice::<i32>().unwrap(), &[10, 20, 30, 40]);
	}

	// Load-bearing test per R1: ReifyDB's `ColumnData::Option { bitvec }`
	// treats a set bit as "defined" (not None); our `NoneBitmap` treats a
	// set bit as None. The bridge must invert per-row.
	#[test]
	fn canonical_from_column_data_preserves_nones_semantics() {
		// Use push + push_none to build a nullable column cleanly.
		let mut cd = ColumnData::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(ca.len(), 4);
		assert!(ca.nullable);
		let nones = ca.nones.as_ref().expect("should have NoneBitmap");
		assert!(!nones.is_none(0), "row 0 should be defined");
		assert!(nones.is_none(1), "row 1 should be None");
		assert!(!nones.is_none(2), "row 2 should be defined");
		assert!(nones.is_none(3), "row 3 should be None");
		assert_eq!(nones.none_count(), 2);
	}

	#[test]
	fn from_column_data_utf8_round_trips() {
		let cd = ColumnData::utf8(["alpha", "bravo", "charlie"]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(ca.len(), 3);
		assert_eq!(ca.ty, Type::Utf8);
		let CanonicalStorage::VarLen(v) = &ca.storage else {
			panic!("expected VarLen storage");
		};
		assert_eq!(v.bytes_at(0), b"alpha");
		assert_eq!(v.bytes_at(1), b"bravo");
		assert_eq!(v.bytes_at(2), b"charlie");
	}

	#[test]
	fn from_column_data_bool_round_trips() {
		let cd = ColumnData::bool([true, false, true]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(ca.len(), 3);
		assert_eq!(ca.ty, Type::Boolean);
		let CanonicalStorage::Bool(b) = &ca.storage else {
			panic!("expected Bool storage");
		};
		assert!(b.get(0));
		assert!(!b.get(1));
		assert!(b.get(2));
	}

	#[test]
	fn canonical_array_encoding_matches_storage_family() {
		let cd = ColumnData::int4([1i32, 2, 3]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		assert_eq!(ca.encoding(), EncodingId::CANONICAL_FIXED);
	}

	#[test]
	fn to_column_data_round_trips_bool() {
		let cd = ColumnData::bool([true, false, true, true]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = ca.to_column_data().unwrap();
		assert_eq!(out, cd);
	}

	#[test]
	fn to_column_data_round_trips_int4() {
		let cd = ColumnData::int4([10i32, 20, 30, 40]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = ca.to_column_data().unwrap();
		assert_eq!(out, cd);
	}

	#[test]
	fn to_column_data_round_trips_utf8() {
		let cd = ColumnData::utf8(["alpha", "bravo", "charlie"]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = ca.to_column_data().unwrap();
		assert_eq!(out, cd);
	}

	// Load-bearing test: the NoneBitmap set=None polarity must be inverted
	// back to ColumnData::Option's set=defined polarity on the return trip.
	#[test]
	fn to_column_data_round_trips_nullable_int4() {
		let mut cd = ColumnData::int4_with_capacity(4);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = ca.to_column_data().unwrap();
		assert_eq!(out, cd);
	}

	#[test]
	fn to_column_data_round_trips_bignum_int() {
		use reifydb_type::value::int::Int;
		let cd = ColumnData::int([Int::from_i64(-7), Int::from_i64(0), Int::from_i64(42)]);
		let ca = CanonicalArray::from_column_data(&cd).unwrap();
		let out = ca.to_column_data().unwrap();
		assert_eq!(out, cd);
	}
}
