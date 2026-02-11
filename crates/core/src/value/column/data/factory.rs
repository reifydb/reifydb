// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			undefined::UndefinedContainer, utf8::Utf8Container, uuid::UuidContainer,
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
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push($default);
						bitvec.push(false);
					}
				}
			}
			ColumnData::$variant(NumberContainer::new(values, BitVec::from(bitvec)))
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(NumberContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			ColumnData::$variant(NumberContainer::new(data, bitvec))
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
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push(<$t>::default());
						bitvec.push(false);
					}
				}
			}
			ColumnData::$variant(TemporalContainer::new(values, BitVec::from(bitvec)))
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(TemporalContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			ColumnData::$variant(TemporalContainer::new(data, bitvec))
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
			for opt in data {
				match opt {
					Some(value) => {
						values.push(value);
						bitvec.push(true);
					}
					None => {
						values.push(<$t>::default());
						bitvec.push(false);
					}
				}
			}
			ColumnData::$variant(UuidContainer::new(values, BitVec::from(bitvec)))
		}

		pub fn $name_cap(capacity: usize) -> Self {
			ColumnData::$variant(UuidContainer::with_capacity(capacity))
		}

		pub fn $name_bv(data: impl IntoIterator<Item = $t>, bitvec: impl Into<BitVec>) -> Self {
			let data = data.into_iter().collect::<Vec<_>>();
			let bitvec = bitvec.into();
			assert_eq!(bitvec.len(), data.len());
			ColumnData::$variant(UuidContainer::new(data, bitvec))
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(false);
					bitvec.push(false);
				}
			}
		}

		ColumnData::Bool(BoolContainer::new(values, BitVec::from(bitvec)))
	}

	pub fn bool_with_capacity(capacity: usize) -> Self {
		ColumnData::Bool(BoolContainer::with_capacity(capacity))
	}

	pub fn bool_with_bitvec(data: impl IntoIterator<Item = bool>, bitvec: impl Into<BitVec>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Bool(BoolContainer::new(data, bitvec))
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(String::new());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Utf8 {
			container: Utf8Container::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
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
		ColumnData::Utf8 {
			container: Utf8Container::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Blob::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Blob {
			container: BlobContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
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
		ColumnData::Blob {
			container: BlobContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn identity_id(identity_ids: impl IntoIterator<Item = IdentityId>) -> Self {
		let data = identity_ids.into_iter().collect::<Vec<_>>();
		ColumnData::IdentityId(IdentityIdContainer::from_vec(data))
	}

	pub fn identity_id_optional(identity_ids: impl IntoIterator<Item = Option<IdentityId>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in identity_ids {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(IdentityId::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::IdentityId(IdentityIdContainer::new(values, BitVec::from(bitvec)))
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
		ColumnData::IdentityId(IdentityIdContainer::new(data, bitvec))
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Int::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Int {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Uint::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Uint {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			max_bytes: MaxBytes::MAX,
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
		ColumnData::Int {
			container: NumberContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
		}
	}

	pub fn uint_with_bitvec(data: impl IntoIterator<Item = Uint>, bitvec: impl Into<BitVec>) -> Self {
		use reifydb_type::value::constraint::bytes::MaxBytes;
		let data = data.into_iter().collect::<Vec<_>>();
		let bitvec = bitvec.into();
		assert_eq!(bitvec.len(), data.len());
		ColumnData::Uint {
			container: NumberContainer::new(data, bitvec),
			max_bytes: MaxBytes::MAX,
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

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Decimal::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::Decimal {
			container: NumberContainer::new(values, BitVec::from(bitvec)),
			precision: Precision::MAX,
			scale: Scale::new(0),
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
		ColumnData::Decimal {
			container: NumberContainer::new(data, bitvec),
			precision: Precision::MAX,
			scale: Scale::new(0),
		}
	}

	pub fn any(data: impl IntoIterator<Item = Box<reifydb_type::value::Value>>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::Any(AnyContainer::from_vec(data))
	}

	pub fn any_optional(data: impl IntoIterator<Item = Option<Box<reifydb_type::value::Value>>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(Box::new(reifydb_type::value::Value::Undefined));
					bitvec.push(false);
				}
			}
		}

		ColumnData::Any(AnyContainer::new(values, BitVec::from(bitvec)))
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
		ColumnData::Any(AnyContainer::new(data, bitvec))
	}

	pub fn dictionary_id(data: impl IntoIterator<Item = DictionaryEntryId>) -> Self {
		let data = data.into_iter().collect::<Vec<_>>();
		ColumnData::DictionaryId(DictionaryContainer::from_vec(data))
	}

	pub fn dictionary_id_optional(data: impl IntoIterator<Item = Option<DictionaryEntryId>>) -> Self {
		let mut values = Vec::new();
		let mut bitvec = Vec::new();

		for opt in data {
			match opt {
				Some(value) => {
					values.push(value);
					bitvec.push(true);
				}
				None => {
					values.push(DictionaryEntryId::default());
					bitvec.push(false);
				}
			}
		}

		ColumnData::DictionaryId(DictionaryContainer::new(values, BitVec::from(bitvec)))
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
		ColumnData::DictionaryId(DictionaryContainer::new(data, bitvec))
	}

	pub fn undefined(len: usize) -> Self {
		ColumnData::Undefined(UndefinedContainer::new(len))
	}

	/// Create typed column data with all undefined values (bitvec all false).
	pub fn undefined_typed(ty: Type, len: usize) -> Self {
		match ty {
			Type::Boolean => Self::bool_with_bitvec(vec![false; len], BitVec::repeat(len, false)),
			Type::Float4 => Self::float4_with_bitvec(vec![0.0f32; len], BitVec::repeat(len, false)),
			Type::Float8 => Self::float8_with_bitvec(vec![0.0f64; len], BitVec::repeat(len, false)),
			Type::Int1 => Self::int1_with_bitvec(vec![0i8; len], BitVec::repeat(len, false)),
			Type::Int2 => Self::int2_with_bitvec(vec![0i16; len], BitVec::repeat(len, false)),
			Type::Int4 => Self::int4_with_bitvec(vec![0i32; len], BitVec::repeat(len, false)),
			Type::Int8 => Self::int8_with_bitvec(vec![0i64; len], BitVec::repeat(len, false)),
			Type::Int16 => Self::int16_with_bitvec(vec![0i128; len], BitVec::repeat(len, false)),
			Type::Utf8 => Self::utf8_with_bitvec(vec![String::new(); len], BitVec::repeat(len, false)),
			Type::Uint1 => Self::uint1_with_bitvec(vec![0u8; len], BitVec::repeat(len, false)),
			Type::Uint2 => Self::uint2_with_bitvec(vec![0u16; len], BitVec::repeat(len, false)),
			Type::Uint4 => Self::uint4_with_bitvec(vec![0u32; len], BitVec::repeat(len, false)),
			Type::Uint8 => Self::uint8_with_bitvec(vec![0u64; len], BitVec::repeat(len, false)),
			Type::Uint16 => Self::uint16_with_bitvec(vec![0u128; len], BitVec::repeat(len, false)),
			Type::Date => Self::date_with_bitvec(vec![Date::default(); len], BitVec::repeat(len, false)),
			Type::DateTime => {
				Self::datetime_with_bitvec(vec![DateTime::default(); len], BitVec::repeat(len, false))
			}
			Type::Time => Self::time_with_bitvec(vec![Time::default(); len], BitVec::repeat(len, false)),
			Type::Duration => {
				Self::duration_with_bitvec(vec![Duration::default(); len], BitVec::repeat(len, false))
			}
			Type::Blob => Self::blob_with_bitvec(vec![Blob::new(vec![]); len], BitVec::repeat(len, false)),
			Type::Uuid4 => Self::uuid4_with_bitvec(vec![Uuid4::default(); len], BitVec::repeat(len, false)),
			Type::Uuid7 => Self::uuid7_with_bitvec(vec![Uuid7::default(); len], BitVec::repeat(len, false)),
			Type::IdentityId => Self::identity_id_with_bitvec(
				vec![IdentityId::default(); len],
				BitVec::repeat(len, false),
			),
			Type::Int => Self::int_with_bitvec(vec![Int::default(); len], BitVec::repeat(len, false)),
			Type::Uint => Self::uint_with_bitvec(vec![Uint::default(); len], BitVec::repeat(len, false)),
			Type::Decimal {
				..
			} => Self::decimal_with_bitvec(vec![Decimal::from(0); len], BitVec::repeat(len, false)),
			Type::Any => Self::any_with_bitvec(
				vec![Box::new(reifydb_type::value::Value::Undefined); len],
				BitVec::repeat(len, false),
			),
			Type::DictionaryId => Self::dictionary_id_with_bitvec(
				vec![DictionaryEntryId::default(); len],
				BitVec::repeat(len, false),
			),
			Type::Undefined => Self::undefined(len),
		}
	}
}
