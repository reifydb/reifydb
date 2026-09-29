// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	Array, ArrayRef, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
	Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, NullArray,
	Time64NanosecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{
	data::canonical::{Canonical, encoding_for_type},
	encoding::EncodingId,
};
use reifydb_value::{
	Result,
	error::Error,
	util::bitmap,
	value::{
		Value,
		column_view::ViewData,
		container::{
			any_array, bool_array,
			decimal_array::{DecimalArray, deserialize_decimal_array, serialize_decimal_array},
			dictionary_array, digest_array, fixed_array, primitive,
			temporal_array::{
				deserialize_dates, deserialize_datetimes, deserialize_durations, deserialize_times,
				serialize_dates, serialize_datetimes, serialize_durations, serialize_times,
			},
			uuid_array::{
				deserialize_identity_ids, deserialize_uuid4s, deserialize_uuid7s,
				serialize_identity_ids, serialize_uuid4s, serialize_uuid7s,
			},
			varlen_array, wide_int_array,
		},
		value_type::{ValueType, field::FieldType},
	},
};
use serde::{Deserialize, Serialize};

use crate::{
	encoding,
	error::ColumnError,
	snapshot::{ColumnBlock, ColumnChunks},
};

const FORMAT_VERSION: u16 = 2;

#[derive(Serialize, Deserialize)]
pub enum PersistedArray {
	Canonical {
		field_type: FieldType,
		data: PersistedData,
		nones: Option<PersistedNones>,
	},
	Constant {
		value: Value,
		len: u64,
	},
	AllNone {
		len: u64,
	},
}

#[derive(Serialize, Deserialize)]
pub enum PersistedData {
	Bool(#[serde(with = "bool_array")] BooleanArray),
	Float4(#[serde(with = "primitive")] Float32Array),
	Float8(#[serde(with = "primitive")] Float64Array),
	Int1(#[serde(with = "primitive")] Int8Array),
	Int2(#[serde(with = "primitive")] Int16Array),
	Int4(#[serde(with = "primitive")] Int32Array),
	Int8(#[serde(with = "primitive")] Int64Array),
	Int16(
		#[serde(
			serialize_with = "wide_int_array::serialize::<i128, _>",
			deserialize_with = "wide_int_array::deserialize::<i128, _>"
		)]
		FixedSizeBinaryArray,
	),
	Uint1(#[serde(with = "primitive")] UInt8Array),
	Uint2(#[serde(with = "primitive")] UInt16Array),
	Uint4(#[serde(with = "primitive")] UInt32Array),
	Uint8(#[serde(with = "primitive")] UInt64Array),
	Uint16(
		#[serde(
			serialize_with = "wide_int_array::serialize::<u128, _>",
			deserialize_with = "wide_int_array::deserialize::<u128, _>"
		)]
		FixedSizeBinaryArray,
	),
	Utf8(
		#[serde(
			serialize_with = "varlen_array::serialize",
			deserialize_with = "varlen_array::deserialize_utf8"
		)]
		LargeStringArray,
	),
	Date(#[serde(serialize_with = "serialize_dates", deserialize_with = "deserialize_dates")] Date32Array),
	DateTime(
		#[serde(serialize_with = "serialize_datetimes", deserialize_with = "deserialize_datetimes")]
		TimestampNanosecondArray,
	),
	Time(
		#[serde(serialize_with = "serialize_times", deserialize_with = "deserialize_times")]
		Time64NanosecondArray,
	),
	Duration(
		#[serde(serialize_with = "serialize_durations", deserialize_with = "deserialize_durations")]
		IntervalMonthDayNanoArray,
	),
	IdentityId(
		#[serde(serialize_with = "serialize_identity_ids", deserialize_with = "deserialize_identity_ids")]
		FixedSizeBinaryArray,
	),
	Uuid4(
		#[serde(serialize_with = "serialize_uuid4s", deserialize_with = "deserialize_uuid4s")]
		FixedSizeBinaryArray,
	),
	Uuid7(
		#[serde(serialize_with = "serialize_uuid7s", deserialize_with = "deserialize_uuid7s")]
		FixedSizeBinaryArray,
	),
	Blob(
		#[serde(
			serialize_with = "varlen_array::serialize",
			deserialize_with = "varlen_array::deserialize_blob"
		)]
		LargeBinaryArray,
	),
	Decimal(
		#[serde(serialize_with = "serialize_decimal_array", deserialize_with = "deserialize_decimal_array")]
		DecimalArray,
	),
	Any(#[serde(with = "any_array")] LargeBinaryArray),
	DictionaryId(#[serde(with = "dictionary_array")] FixedSizeBinaryArray),
	Digest(#[serde(with = "digest_array")] LargeBinaryArray),
	None {
		len: u64,
	},
}

#[derive(Serialize, Deserialize)]
pub struct PersistedNones(#[serde(with = "bitmap")] BooleanBuffer);

impl PersistedArray {
	pub fn encoding_id(&self, ty: &ValueType) -> EncodingId {
		match self {
			PersistedArray::Canonical {
				..
			} => encoding_for_type(ty),
			PersistedArray::Constant {
				..
			} => EncodingId::CONSTANT,
			PersistedArray::AllNone {
				..
			} => EncodingId::ALL_NONE,
		}
	}
}

pub(crate) fn persist_canonical(canonical: &Canonical) -> PersistedArray {
	let data = match canonical.view().data {
		ViewData::Bool(c) => PersistedData::Bool(c.clone()),
		ViewData::Float4(c) => PersistedData::Float4(c.clone()),
		ViewData::Float8(c) => PersistedData::Float8(c.clone()),
		ViewData::Int1(c) => PersistedData::Int1(c.clone()),
		ViewData::Int2(c) => PersistedData::Int2(c.clone()),
		ViewData::Int4(c) => PersistedData::Int4(c.clone()),
		ViewData::Int8(c) => PersistedData::Int8(c.clone()),
		ViewData::Int16(c) => PersistedData::Int16(c.clone()),
		ViewData::Uint1(c) => PersistedData::Uint1(c.clone()),
		ViewData::Uint2(c) => PersistedData::Uint2(c.clone()),
		ViewData::Uint4(c) => PersistedData::Uint4(c.clone()),
		ViewData::Uint8(c) => PersistedData::Uint8(c.clone()),
		ViewData::Uint16(c) => PersistedData::Uint16(c.clone()),
		ViewData::Utf8 {
			container,
			..
		} => PersistedData::Utf8(container.clone()),
		ViewData::Date(c) => PersistedData::Date(c.clone()),
		ViewData::DateTime(c) => PersistedData::DateTime(c.clone()),
		ViewData::Time(c) => PersistedData::Time(c.clone()),
		ViewData::Duration(c) => PersistedData::Duration(c.clone()),
		ViewData::IdentityId(c) => PersistedData::IdentityId(c.clone()),
		ViewData::Uuid4(c) => PersistedData::Uuid4(c.clone()),
		ViewData::Uuid7(c) => PersistedData::Uuid7(c.clone()),
		ViewData::Blob {
			container,
			..
		} => PersistedData::Blob(container.clone()),
		ViewData::Decimal(c) => PersistedData::Decimal(DecimalArray::from(c)),
		ViewData::Any {
			container,
			..
		} => PersistedData::Any(container.clone()),
		ViewData::DictionaryId {
			container,
			..
		} => PersistedData::DictionaryId(container.clone()),
		ViewData::Digest {
			container,
			..
		} => PersistedData::Digest(container.clone()),
		ViewData::None {
			array,
		} => PersistedData::None {
			len: array.len() as u64,
		},
	};
	let nones = match data {
		PersistedData::None {
			..
		} => None,
		_ => canonical.buffer().logical_nulls().map(|nones| PersistedNones(nones.into_inner())),
	};
	PersistedArray::Canonical {
		field_type: canonical.field_type().clone(),
		data,
		nones,
	}
}

pub(crate) fn load_canonical(
	field_type: FieldType,
	data: PersistedData,
	nones: Option<PersistedNones>,
) -> Result<Canonical> {
	let nones = nones.map(|PersistedNones(bits)| NullBuffer::new(bits));
	let array = match data {
		PersistedData::Bool(c) => attached(c, nones, bool_array::attach_nulls)?,
		PersistedData::Float4(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Float8(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Int1(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Int2(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Int4(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Int8(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Uint1(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Uint2(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Uint4(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Uint8(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Date(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::DateTime(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Time(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Duration(c) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Int16(c)
		| PersistedData::Uint16(c)
		| PersistedData::IdentityId(c)
		| PersistedData::Uuid4(c)
		| PersistedData::Uuid7(c)
		| PersistedData::DictionaryId(c) => attached(c, nones, fixed_array::attach_nulls)?,
		PersistedData::Utf8(c) => attached(c, nones, varlen_array::attach_nulls)?,
		PersistedData::Blob(c) => attached(c, nones, varlen_array::attach_nulls)?,
		PersistedData::Decimal(DecimalArray::Decimal128(c)) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Decimal(DecimalArray::Decimal256(c)) => attached(c, nones, primitive::attach_nulls)?,
		PersistedData::Any(c) | PersistedData::Digest(c) => {
			let own = c.logical_nulls();
			attached(c, nones, |c, nones| {
				varlen_array::attach_nulls(c, NullBuffer::union(nones.as_ref(), own.as_ref()))
			})?
		}
		PersistedData::None {
			len,
		} => Arc::new(NullArray::new(len as usize)),
	};
	Canonical::new(field_type, array)
}

fn attached<A: Array + 'static>(
	array: A,
	nones: Option<NullBuffer>,
	attach: impl FnOnce(A, Option<NullBuffer>) -> A,
) -> Result<ArrayRef> {
	if let Some(nones) = &nones
		&& nones.len() != array.len()
	{
		return Err(Error::from(ColumnError::PersistDeserialize {
			reason: format!("none bitmap of {} bits does not match its {} rows", nones.len(), array.len()),
		}));
	}
	Ok(Arc::new(attach(array, nones)))
}

#[derive(Serialize, Deserialize)]
struct PersistedColumn {
	chunks: Vec<PersistedArray>,
}

#[derive(Serialize, Deserialize)]
struct PersistedBlock {
	format_version: u16,
	schema: Vec<(String, ValueType, bool)>,
	columns: Vec<PersistedColumn>,
}

pub fn serialize_block(block: &ColumnBlock) -> Result<Vec<u8>> {
	let registry = encoding::global();
	let mut columns = Vec::with_capacity(block.columns.len());
	for column in &block.columns {
		let mut chunks = Vec::with_capacity(column.chunks.len());
		for chunk in &column.chunks {
			let id = chunk.encoding();
			let encoding = registry.get(id).ok_or_else(|| unregistered_on_serialize(id))?;
			chunks.push(encoding.persist(chunk)?);
		}
		columns.push(PersistedColumn {
			chunks,
		});
	}

	let persisted = PersistedBlock {
		format_version: FORMAT_VERSION,
		schema: block.schema.as_ref().clone(),
		columns,
	};

	to_stdvec(&persisted).map_err(|e| {
		Error::from(ColumnError::PersistSerialize {
			reason: e.to_string(),
		})
	})
}

pub fn deserialize_block(bytes: &[u8]) -> Result<ColumnBlock> {
	let persisted: PersistedBlock = from_bytes(bytes).map_err(|e| {
		Error::from(ColumnError::PersistDeserialize {
			reason: e.to_string(),
		})
	})?;

	if persisted.format_version != FORMAT_VERSION {
		return Err(Error::from(ColumnError::PersistVersionUnsupported {
			version: persisted.format_version,
		}));
	}

	if persisted.columns.len() != persisted.schema.len() {
		return Err(Error::from(ColumnError::PersistDeserialize {
			reason: format!(
				"schema has {} columns but {} column payloads were stored",
				persisted.schema.len(),
				persisted.columns.len()
			),
		}));
	}

	let registry = encoding::global();
	let schema = Arc::new(persisted.schema);
	let mut columns = Vec::with_capacity(persisted.columns.len());
	for (index, persisted_column) in persisted.columns.into_iter().enumerate() {
		let (_, ty, nullable) = &schema[index];
		let mut chunks = Vec::with_capacity(persisted_column.chunks.len());
		for persisted_chunk in persisted_column.chunks {
			let id = persisted_chunk.encoding_id(ty);
			let encoding = registry.get(id).ok_or_else(|| unregistered_on_deserialize(id))?;
			chunks.push(encoding.load(persisted_chunk, ty)?);
		}
		columns.push(ColumnChunks::new(ty.clone(), *nullable, chunks));
	}

	Ok(ColumnBlock::new(schema, columns))
}

pub fn unexpected_data(id: EncodingId) -> Error {
	Error::from(ColumnError::PersistSerialize {
		reason: format!("encoding {} was asked to persist a column it does not own", id.0),
	})
}

pub fn unexpected_payload(id: EncodingId) -> Error {
	Error::from(ColumnError::PersistDeserialize {
		reason: format!("encoding {} was asked to load a payload it does not own", id.0),
	})
}

fn unregistered_on_serialize(id: EncodingId) -> Error {
	Error::from(ColumnError::PersistSerialize {
		reason: format!("encoding {} is not registered", id.0),
	})
}

fn unregistered_on_deserialize(id: EncodingId) -> Error {
	Error::from(ColumnError::PersistDeserialize {
		reason: format!("encoding {} is not registered", id.0),
	})
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_schema::FieldRef;
	use reifydb_core::value::column::{
		builder::ColumnBuilder,
		data::{Column, canonical::Canonical},
		factory,
	};
	use reifydb_value::value::{
		Value,
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::{ValueType, field::from_field},
	};

	use super::*;
	use crate::snapshot::{ColumnBlock, ColumnChunks};

	fn col(column: (FieldRef, ArrayRef)) -> Column {
		Column::from_canonical(Canonical::from_column(&column).unwrap())
	}

	fn block_values(block: &ColumnBlock) -> Vec<Vec<Value>> {
		block.columns
			.iter()
			.map(|column| {
				let mut out = Vec::new();
				for chunk in &column.chunks {
					for i in 0..chunk.len() {
						out.push(chunk.data().get_value(i));
					}
				}
				out
			})
			.collect()
	}

	fn assert_round_trips(block: ColumnBlock) {
		let bytes = serialize_block(&block).unwrap();
		let restored = deserialize_block(&bytes).unwrap();
		assert_eq!(*block.schema, *restored.schema, "schema must survive the round trip");
		assert_eq!(block.len(), restored.len(), "row count must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored), "values must survive the round trip");
	}

	#[test]
	fn round_trips_fixed_width_column() {
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, false)]);
		let column = ColumnChunks::single(ValueType::Int4, false, col(factory::int4("a", [1i32, 2, 3, 4])));
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_varlen_column() {
		let schema = Arc::new(vec![("s".to_string(), ValueType::Utf8, false)]);
		let column = ColumnChunks::single(
			ValueType::Utf8,
			false,
			col(factory::utf8("s", ["alpha", "bravo", "charlie"])),
		);
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_multi_column_block() {
		let schema = Arc::new(vec![
			("id".to_string(), ValueType::Uint8, false),
			("name".to_string(), ValueType::Utf8, false),
		]);
		let columns = vec![
			ColumnChunks::single(ValueType::Uint8, false, col(factory::uint8("id", vec![1u64, 2, 3]))),
			ColumnChunks::single(ValueType::Utf8, false, col(factory::utf8("name", ["x", "y", "z"]))),
		];
		assert_round_trips(ColumnBlock::new(schema, columns));
	}

	#[test]
	fn round_trips_nullable_column_preserving_none_positions() {
		let mut buffer = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		buffer.push::<i32>(10);
		buffer.push_none();
		buffer.push::<i32>(30);
		buffer.push_none();
		let canonical = Canonical::from_column(&buffer.finish("a")).unwrap();
		assert!(canonical.view().is_nullable(), "buffer with push_none must canonicalize to a nullable column");

		let column = ColumnChunks::single(ValueType::Int4, true, Column::from_canonical(canonical));
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, true)]);
		let block = ColumnBlock::new(schema, vec![column]);

		let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();

		assert!(restored.columns[0].nullable, "nullability must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored));
		let chunk = &restored.columns[0].chunks[0];
		let is_defined = |row: usize| chunk.nones().is_none_or(|nones| nones.is_valid(row));
		assert!(is_defined(0));
		assert!(!is_defined(1), "none at index 1 must be preserved");
		assert!(is_defined(2));
		assert!(!is_defined(3), "none at index 3 must be preserved");
	}

	#[test]
	fn optional_columns_keep_their_nones_and_field_type_through_persist() {
		// Without the none bitmap and field type on disk, a reload turns nones to zeros and drops type details.
		let d = |text: &str| Decimal::parse(text).unwrap();
		let mut dictionary = ColumnBuilder::with_capacity(ValueType::DictionaryId, 3);
		dictionary.set_dictionary_id(DictionaryId(7));
		dictionary.push(DictionaryEntryId::U4(1));
		dictionary.push_none();
		dictionary.push(DictionaryEntryId::U4(3));
		let columns = vec![
			factory::int4_optional("i", vec![Some(1), None, Some(3)]),
			factory::utf8_with_bitvec("u", ["x", "", "z"], vec![true, false, true]),
			factory::decimal_with_bitvec(
				"d",
				Precision::new(10),
				Scale::new(2),
				[d("1.50"), d("0.00"), d("-2.25")],
				vec![true, false, true],
			),
			factory::any_optional(
				"y",
				vec![Some(Value::Int4(5)), None, Some(Value::Utf8("w".to_string()))],
			),
			dictionary.finish("k"),
		];
		let mut schema = Vec::new();
		let mut chunks = Vec::new();
		for column in &columns {
			let field_type = from_field(&column.0).unwrap();
			let Some(ValueType::Option(bare)) = field_type.value_type.clone() else {
				panic!("fixture column {} must be optional, got {field_type:?}", column.0.name());
			};
			schema.push((column.0.name().clone(), *bare.clone(), true));
			chunks.push(ColumnChunks::single(*bare, true, col(column.clone())));
		}
		let block = ColumnBlock::new(Arc::new(schema), chunks);

		let restored = deserialize_block(&serialize_block(&block).unwrap()).unwrap();

		assert_eq!(block_values(&block), block_values(&restored), "values must survive the round trip");
		for (index, (original, reloaded)) in block.columns.iter().zip(&restored.columns).enumerate() {
			let original = original.chunks[0].to_canonical().unwrap();
			let reloaded = reloaded.chunks[0].to_canonical().unwrap();
			assert_eq!(
				reloaded.field_type(),
				original.field_type(),
				"column {index} must keep its field type"
			);
			let nones = reloaded
				.buffer()
				.logical_nulls()
				.expect("a column with nones must reload with a bitmap");
			assert_eq!(
				(0..nones.len()).map(|row| nones.is_null(row)).collect::<Vec<_>>(),
				vec![false, true, false],
				"column {index} must keep its none at row 1"
			);
		}
	}

	#[test]
	fn deserialize_rejects_garbage_without_panicking() {
		assert!(deserialize_block(&[0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
	}
}
