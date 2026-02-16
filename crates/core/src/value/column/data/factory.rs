// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};

use crate::value::column::ColumnData;

macro_rules! impl_number_factory {
	($name:ident, $name_opt:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty, $default:expr) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnData::$variant(NumberContainer::from_vec(data))
		}

		pub fn $name_opt(data: impl IntoIterator<Item = Option<$t>>) -> Self {
			let mut values = Vec::new();
			let mut bitvec = Vec::new();
			let mut has_none = false;
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push($default);
						bitvec.push(false);
						has_none = true;
					}
				}
			}
			let inner = ColumnData::$variant(NumberContainer::from_vec(values));
			if has_none {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec: BitVec::from(bitvec),
				}
			} else {
				inner
			}
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(NumberContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnData::$variant(NumberContainer::from_vec(data));
			if bitvec.all_ones() {
				inner
			} else {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec,
				}
			}
		}
	};
}

macro_rules! impl_temporal_factory {
	($name:ident, $name_opt:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnData::$variant(TemporalContainer::from_vec(data))
		}

		pub fn $name_opt(data: impl IntoIterator<Item = Option<$t>>) -> Self {
			let mut values = Vec::new();
			let mut bitvec = Vec::new();
			let mut has_none = false;
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push(<$t>::default());
						bitvec.push(false);
						has_none = true;
					}
				}
			}
			let inner = ColumnData::$variant(TemporalContainer::from_vec(values));
			if has_none {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec: BitVec::from(bitvec),
				}
			} else {
				inner
			}
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(TemporalContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnData::$variant(TemporalContainer::from_vec(data));
			if bitvec.all_ones() {
				inner
			} else {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec,
				}
			}
		}
	};
}

macro_rules! impl_uuid_factory {
	($name:ident, $name_opt:ident, $name_cap:ident, $name_bv:ident, $variant:ident, $t:ty) => {
		pub fn $name(data: impl IntoIterator<Item = $t>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			ColumnData::$variant(UuidContainer::from_vec(data))
		}

		pub fn $name_opt(data: impl IntoIterator<Item = Option<$t>>) -> Self {
			let mut values = Vec::new();
			let mut bitvec = Vec::new();
			let mut has_none = false;
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push(<$t>::default());
						bitvec.push(false);
						has_none = true;
					}
				}
			}
			let inner = ColumnData::$variant(UuidContainer::from_vec(values));
			if has_none {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec: BitVec::from(bitvec),
				}
			} else {
				inner
			}
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(UuidContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			let inner = ColumnData::$variant(UuidContainer::from_vec(data));
			if bitvec.all_ones() {
				inner
			} else {
				ColumnData::Option {
					inner: Box::new(inner),
					bitvec,
				}
			}
		}
	};
}

impl ColumnData {
	pub fn bool(data: impl IntoIterator<Item = bool>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Bool(BoolContainer::from_vec(data))
	}

	pub fn bool_optional(data: impl IntoIterator<Item = Option<bool>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(false);
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Bool(BoolContainer::from_vec(values));
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn bool_with_capacity(capacity: usize) -> Self {
		ColumnData::Bool(BoolContainer::with_capacity(capacity))
	}

	pub fn bool_with_bitvec(data: impl IntoIterator<Item = bool>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Bool(BoolContainer::from_vec(data));
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	impl_number_factory!(float4, float4_optional, float4_with_capacity, float4_with_bitvec, Float4, f32, 0.0);
	impl_number_factory!(float8, float8_optional, float8_with_capacity, float8_with_bitvec, Float8, f64, 0.0);
	impl_number_factory!(int1, int1_optional, int1_with_capacity, int1_with_bitvec, Int1, i8, 0);
	impl_number_factory!(int2, int2_optional, int2_with_capacity, int2_with_bitvec, Int2, i16, 0);
	impl_number_factory!(int4, int4_optional, int4_with_capacity, int4_with_bitvec, Int4, i32, 0);
	impl_number_factory!(int8, int8_optional, int8_with_capacity, int8_with_bitvec, Int8, i64, 0);
	impl_number_factory!(int16, int16_optional, int16_with_capacity, int16_with_bitvec, Int16, i128, 0);
	impl_number_factory!(uint1, uint1_optional, uint1_with_capacity, uint1_with_bitvec, Uint1, u8, 0);
	impl_number_factory!(uint2, uint2_optional, uint2_with_capacity, uint2_with_bitvec, Uint2, u16, 0);
	impl_number_factory!(uint4, uint4_optional, uint4_with_capacity, uint4_with_bitvec, Uint4, u32, 0);
	impl_number_factory!(uint8, uint8_optional, uint8_with_capacity, uint8_with_bitvec, Uint8, u64, 0);
	impl_number_factory!(uint16, uint16_optional, uint16_with_capacity, uint16_with_bitvec, Uint16, u128, 0);

	pub fn utf8(data: impl IntoIterator<Item = impl Into<String>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().map(|c| c.into()).collect::<Vec<_>>();
		ColumnData::Utf8 {
			container: Utf8Container::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_optional(data: impl IntoIterator<Item = Option<String>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(String::new());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Utf8 {
			container: Utf8Container::from_vec(values),
			max_bytes: MaxBytes::MAX,
		};
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn utf8_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Utf8 {
			container: Utf8Container::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn utf8_with_bitvec(data: impl IntoIterator<Item = impl Into<String>>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().map(Into::into).collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Utf8 {
			container: Utf8Container::from_vec(data),
			max_bytes: MaxBytes::MAX,
		};
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	impl_temporal_factory!(date, date_optional, date_with_capacity, date_with_bitvec, Date, Date);
	impl_temporal_factory!(
		datetime,
		datetime_optional,
		datetime_with_capacity,
		datetime_with_bitvec,
		DateTime,
		DateTime
	);
	impl_temporal_factory!(time, time_optional, time_with_capacity, time_with_bitvec, Time, Time);
	impl_temporal_factory!(
		duration,
		duration_optional,
		duration_with_capacity,
		duration_with_bitvec,
		Duration,
		Duration
	);

	impl_uuid_factory!(uuid4, uuid4_optional, uuid4_with_capacity, uuid4_with_bitvec, Uuid4, Uuid4);
	impl_uuid_factory!(uuid7, uuid7_optional, uuid7_with_capacity, uuid7_with_bitvec, Uuid7, Uuid7);

	pub fn blob(data: impl IntoIterator<Item = Blob>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Blob {
			container: BlobContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_optional(data: impl IntoIterator<Item = Option<Blob>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Blob::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Blob {
			container: BlobContainer::from_vec(values),
			max_bytes: MaxBytes::MAX,
		};
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn blob_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Blob {
			container: BlobContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn blob_with_bitvec(data: impl IntoIterator<Item = Blob>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Blob {
			container: BlobContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		};
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn identity_id(identity_ids: impl IntoIterator<Item = IdentityId>) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		ColumnData::IdentityId(IdentityIdContainer::from_vec(data))
	}

	pub fn identity_id_optional(identity_ids: impl IntoIterator<Item = Option<IdentityId>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in identity_ids {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(IdentityId::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::IdentityId(IdentityIdContainer::from_vec(values));
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn identity_id_with_capacity(capacity: usize) -> Self {
		ColumnData::IdentityId(IdentityIdContainer::with_capacity(capacity))
	}

	pub fn identity_id_with_bitvec(
		identity_ids: impl IntoIterator<Item = IdentityId>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::IdentityId(IdentityIdContainer::from_vec(data));
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn int(data: impl IntoIterator<Item = Int>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Int {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn int_optional(data: impl IntoIterator<Item = Option<Int>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Int::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Int {
			container: NumberContainer::from_vec(values),
			max_bytes: MaxBytes::MAX,
		};
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn uint(data: impl IntoIterator<Item = Uint>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Uint {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_optional(data: impl IntoIterator<Item = Option<Uint>>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Uint::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Uint {
			container: NumberContainer::from_vec(values),
			max_bytes: MaxBytes::MAX,
		};
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn int_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Int {
			container: NumberContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		ColumnData::Uint {
			container: NumberContainer::with_capacity(capacity),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn int_with_bitvec(data: impl IntoIterator<Item = Int>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Int {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		};
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn uint_with_bitvec(data: impl IntoIterator<Item = Uint>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Uint {
			container: NumberContainer::from_vec(data),
			max_bytes: MaxBytes::MAX,
		};
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn decimal(data: impl IntoIterator<Item = Decimal>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Decimal {
			container: NumberContainer::from_vec(data),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn decimal_optional(data: impl IntoIterator<Item = Option<Decimal>>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Decimal::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Decimal {
			container: NumberContainer::from_vec(values),
			precision: Precision::MAX,
			scale: Scale::new(0),
		};
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn decimal_with_capacity(capacity: usize) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		ColumnData::Decimal {
			container: NumberContainer::with_capacity(capacity),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn decimal_with_bitvec(data: impl IntoIterator<Item = Decimal>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::{precision::Precision, scale::Scale};
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Decimal {
			container: NumberContainer::from_vec(data),
			precision: Precision::MAX,
			scale: Scale::new(0),
		};
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn any(data: impl IntoIterator<Item = Box<reifydb_type::value::Value>>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Any(AnyContainer::from_vec(data))
	}

	pub fn any_optional(data: impl IntoIterator<Item = Option<Box<reifydb_type::value::Value>>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Box::new(reifydb_type::value::Value::none()));
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::Any(AnyContainer::from_vec(values));
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn any_with_capacity(capacity: usize) -> Self {
		ColumnData::Any(AnyContainer::with_capacity(capacity))
	}

	pub fn any_with_bitvec(
		data: impl IntoIterator<Item = Box<reifydb_type::value::Value>>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::Any(AnyContainer::from_vec(data));
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	pub fn dictionary_id(data: impl IntoIterator<Item = DictionaryEntryId>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::DictionaryId(DictionaryContainer::from_vec(data))
	}

	pub fn dictionary_id_optional(data: impl IntoIterator<Item = Option<DictionaryEntryId>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();
		let mut has_none = false;

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(DictionaryEntryId::default());
					bitvec.push(false);
					has_none = true;
				}
			}
		}

		let inner = ColumnData::DictionaryId(DictionaryContainer::from_vec(values));
		if has_none {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec: BitVec::from(bitvec),
			}
		} else {
			inner
		}
	}

	pub fn dictionary_id_with_capacity(capacity: usize) -> Self {
		ColumnData::DictionaryId(DictionaryContainer::with_capacity(capacity))
	}

	pub fn dictionary_id_with_bitvec(
		data: impl IntoIterator<Item = DictionaryEntryId>,
		bitvec: impl Into<BitVec>,
	) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		let inner = ColumnData::DictionaryId(DictionaryContainer::from_vec(data));
		if bitvec.all_ones() {
			inner
		} else {
			ColumnData::Option {
				inner: Box::new(inner),
				bitvec,
			}
		}
	}

	/// Create a single-element None of the given type (bitvec=[false]).
	/// This preserves the column type so comparisons
	/// see the correct inner type rather than `Option<Boolean>`.
	pub fn typed_none(ty: &Type) -> Self {
		match ty {
			Type::Option(inner) => Self::typed_none(inner),
			_ => Self::none_typed(ty.clone(), 1),
		}
	}

	/// Create typed column data with all none values (bitvec all false).
	/// Always returns an Option-wrapped column to avoid the *_with_bitvec
	/// optimization that strips the Option wrapper when the bitvec is all-ones
	/// (which is vacuously true for empty bitvecs).
	pub fn none_typed(ty: Type, len: usize) -> Self {
		let bitvec = BitVec::repeat(len, false);
		let inner = match ty {
			Type::Boolean => Self::bool(vec![false; len]),
			Type::Float4 => Self::float4(vec![0.0f32; len]),
			Type::Float8 => Self::float8(vec![0.0f64; len]),
			Type::Int1 => Self::int1(vec![0i8; len]),
			Type::Int2 => Self::int2(vec![0i16; len]),
			Type::Int4 => Self::int4(vec![0i32; len]),
			Type::Int8 => Self::int8(vec![0i64; len]),
			Type::Int16 => Self::int16(vec![0i128; len]),
			Type::Utf8 => Self::utf8(vec![String::new(); len]),
			Type::Uint1 => Self::uint1(vec![0u8; len]),
			Type::Uint2 => Self::uint2(vec![0u16; len]),
			Type::Uint4 => Self::uint4(vec![0u32; len]),
			Type::Uint8 => Self::uint8(vec![0u64; len]),
			Type::Uint16 => Self::uint16(vec![0u128; len]),
			Type::Date => Self::date(vec![Date::default(); len]),
			Type::DateTime => Self::datetime(vec![DateTime::default(); len]),
			Type::Time => Self::time(vec![Time::default(); len]),
			Type::Duration => Self::duration(vec![Duration::default(); len]),
			Type::Blob => Self::blob(vec![Blob::new(vec![]); len]),
			Type::Uuid4 => Self::uuid4(vec![Uuid4::default(); len]),
			Type::Uuid7 => Self::uuid7(vec![Uuid7::default(); len]),
			Type::IdentityId => Self::identity_id(vec![IdentityId::default(); len]),
			Type::Int => Self::int(vec![Int::default(); len]),
			Type::Uint => Self::uint(vec![Uint::default(); len]),
			Type::Decimal {
				..
			} => Self::decimal(vec![Decimal::from(0); len]),
			Type::Any => Self::any(vec![Box::new(reifydb_type::value::Value::none()); len]),
			Type::DictionaryId => Self::dictionary_id(vec![DictionaryEntryId::default(); len]),
			Type::Option(inner) => return Self::none_typed(*inner, len),
		};
		ColumnData::Option {
			inner: Box::new(inner),
			bitvec,
		}
	}
}
