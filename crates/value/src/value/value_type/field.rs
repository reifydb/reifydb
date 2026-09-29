// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{result::Result as StdResult, sync::Arc};

use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType, Field, FieldRef, IntervalUnit, TimeUnit, extension::ExtensionType};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{from_str, json};

use crate::{
	Result,
	error::{Diagnostic, Error},
	value::{
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			decimal_array, dictionary_array::DICTIONARY_ENTRY_WIDTH, temporal_array::DATETIME_TIMEZONE,
		},
		dictionary::DictionaryId,
		value_type::ValueType,
	},
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldType {
	pub value_type: Option<ValueType>,
	pub max_bytes: Option<MaxBytes>,
	pub dictionary_id: Option<DictionaryId>,
	pub declared_type: Option<ValueType>,
}

impl From<ValueType> for FieldType {
	fn from(value_type: ValueType) -> Self {
		FieldType {
			value_type: Some(value_type),
			..FieldType::default()
		}
	}
}

pub fn to_field(name: &str, field_type: &FieldType) -> Field {
	let nullable = matches!(field_type.value_type, None | Some(ValueType::Option(_)));
	let bare = field_type.value_type.as_ref().map(|value_type| match value_type {
		ValueType::Option(inner) => inner.as_ref(),
		other => other,
	});
	assert!(details_fit(bare, field_type), "field {name}: the details do not fit the value type in {field_type:?}");
	let Some(bare) = bare else {
		return Field::new(name, DataType::Null, true);
	};
	let field = |data_type: DataType| Field::new(name, data_type, nullable);
	let wide = DataType::FixedSizeBinary(16);
	match bare {
		ValueType::Boolean => field(DataType::Boolean),
		ValueType::Int1 => field(DataType::Int8),
		ValueType::Int2 => field(DataType::Int16),
		ValueType::Int4 => field(DataType::Int32),
		ValueType::Int8 => field(DataType::Int64),
		ValueType::Uint1 => field(DataType::UInt8),
		ValueType::Uint2 => field(DataType::UInt16),
		ValueType::Uint4 => field(DataType::UInt32),
		ValueType::Uint8 => field(DataType::UInt64),
		ValueType::Float4 => field(DataType::Float32),
		ValueType::Float8 => field(DataType::Float64),
		ValueType::Decimal {
			precision,
			scale,
		} => field(decimal_array::data_type(*precision, *scale)),
		ValueType::Date => field(DataType::Date32),
		ValueType::DateTime => field(DataType::Timestamp(TimeUnit::Nanosecond, Some(DATETIME_TIMEZONE.into()))),
		ValueType::Time => field(DataType::Time64(TimeUnit::Nanosecond)),
		ValueType::Duration => field(DataType::Interval(IntervalUnit::MonthDayNano)),
		ValueType::Int16 => field(wide).with_extension_type(Int16Tag(())),
		ValueType::Uint16 => field(wide).with_extension_type(Uint16Tag(())),
		ValueType::Uuid4 => field(wide).with_extension_type(Uuid4Tag(())),
		ValueType::Uuid7 => field(wide).with_extension_type(Uuid7Tag(())),
		ValueType::IdentityId => field(wide).with_extension_type(IdentityIdTag(())),
		ValueType::DictionaryId => field(DataType::FixedSizeBinary(DICTIONARY_ENTRY_WIDTH as i32))
			.with_extension_type(DictionaryIdTag(field_type.dictionary_id)),
		ValueType::Utf8 => match field_type.max_bytes {
			Some(max_bytes) => field(DataType::LargeUtf8).with_extension_type(Utf8Tag(max_bytes)),
			None => field(DataType::LargeUtf8),
		},
		ValueType::Blob => field(DataType::LargeBinary).with_extension_type(BlobTag(field_type.max_bytes)),
		ValueType::Any | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_) => {
			field(DataType::LargeBinary).with_extension_type(AnyTag(field_type.declared_type.clone()))
		}
		ValueType::Digest {
			inner,
			accuracy,
		} => field(DataType::LargeBinary).with_extension_type(DigestTag(DigestMetadata {
			inner: inner.as_ref().clone(),
			accuracy: *accuracy,
		})),
		ValueType::Option(_) => {
			panic!("field {name}: a nested Option has no arrow field, found {field_type:?}")
		}
	}
}

pub fn named(name: &str, field_type: FieldType, array: ArrayRef) -> (FieldRef, ArrayRef) {
	(Arc::new(to_field(name, &field_type)), array)
}

pub fn from_field(field: &Field) -> Result<FieldType> {
	let mut field_type = FieldType::default();
	let bare = match field.extension_type_name() {
		None if *field.data_type() == DataType::Null => return Ok(field_type),
		None => untagged(field)?,
		Some(Int16Tag::NAME) => tagged::<Int16Tag>(field).map(|_| ValueType::Int16)?,
		Some(Uint16Tag::NAME) => tagged::<Uint16Tag>(field).map(|_| ValueType::Uint16)?,
		Some(Uuid4Tag::NAME) => tagged::<Uuid4Tag>(field).map(|_| ValueType::Uuid4)?,
		Some(Uuid7Tag::NAME) => tagged::<Uuid7Tag>(field).map(|_| ValueType::Uuid7)?,
		Some(IdentityIdTag::NAME) => tagged::<IdentityIdTag>(field).map(|_| ValueType::IdentityId)?,
		Some(DictionaryIdTag::NAME) => {
			field_type.dictionary_id = tagged::<DictionaryIdTag>(field)?.0;
			ValueType::DictionaryId
		}
		Some(Utf8Tag::NAME) => {
			field_type.max_bytes = Some(tagged::<Utf8Tag>(field)?.0);
			ValueType::Utf8
		}
		Some(BlobTag::NAME) => {
			field_type.max_bytes = tagged::<BlobTag>(field)?.0;
			ValueType::Blob
		}
		Some(AnyTag::NAME) => {
			field_type.declared_type = tagged::<AnyTag>(field)?.0;
			field_type.declared_type.clone().unwrap_or(ValueType::Any)
		}
		Some(DigestTag::NAME) => {
			let DigestTag(metadata) = tagged(field)?;
			ValueType::Digest {
				inner: Box::new(metadata.inner),
				accuracy: metadata.accuracy,
			}
		}
		Some(name) => {
			return Err(field_error(format!("field {} has unknown extension type {name}", field.name())));
		}
	};
	field_type.value_type = Some(if field.is_nullable() {
		ValueType::Option(Box::new(bare))
	} else {
		bare
	});
	Ok(field_type)
}

fn details_fit(bare: Option<&ValueType>, field_type: &FieldType) -> bool {
	let max_bytes_fit = field_type.max_bytes.is_none() || matches!(bare, Some(ValueType::Utf8 | ValueType::Blob));
	let dictionary_fit = field_type.dictionary_id.is_none() || matches!(bare, Some(ValueType::DictionaryId));
	let declared_fit = match bare {
		Some(any @ (ValueType::Any | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_))) => {
			field_type.declared_type.as_ref().unwrap_or(&ValueType::Any) == any
		}
		_ => field_type.declared_type.is_none(),
	};
	max_bytes_fit && dictionary_fit && declared_fit
}

fn untagged(field: &Field) -> Result<ValueType> {
	Ok(match field.data_type() {
		DataType::Boolean => ValueType::Boolean,
		DataType::Int8 => ValueType::Int1,
		DataType::Int16 => ValueType::Int2,
		DataType::Int32 => ValueType::Int4,
		DataType::Int64 => ValueType::Int8,
		DataType::UInt8 => ValueType::Uint1,
		DataType::UInt16 => ValueType::Uint2,
		DataType::UInt32 => ValueType::Uint4,
		DataType::UInt64 => ValueType::Uint8,
		DataType::Float32 => ValueType::Float4,
		DataType::Float64 => ValueType::Float8,
		DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
			decimal(*precision, *scale)?
		}
		DataType::Date32 => ValueType::Date,
		DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if zone.as_ref() == DATETIME_TIMEZONE => {
			ValueType::DateTime
		}
		DataType::Time64(TimeUnit::Nanosecond) => ValueType::Time,
		DataType::Interval(IntervalUnit::MonthDayNano) => ValueType::Duration,
		DataType::LargeUtf8 => ValueType::Utf8,
		other => {
			return Err(field_error(format!(
				"field {} has untagged arrow type {other}, which maps to no value type",
				field.name()
			)));
		}
	})
}

fn decimal(precision: u8, scale: i8) -> Result<ValueType> {
	let precision = Precision::try_new(precision)?;
	let scale = u8::try_from(scale).map_err(|_| field_error(format!("decimal scale {scale} is negative")))?;
	Ok(ValueType::Decimal {
		precision,
		scale: Scale::try_new_with_precision(scale, precision)?,
	})
}

fn tagged<E: ExtensionType>(field: &Field) -> Result<E> {
	field.try_extension_type::<E>().map_err(|error| field_error(format!("field {}: {error}", field.name())))
}

pub(crate) fn field_error(message: String) -> Error {
	Error(Box::new(Diagnostic {
		code: "INTERNAL_ERROR".to_string(),
		message,
		..Diagnostic::default()
	}))
}

macro_rules! tag {
	($tag:ident, $name:literal, $metadata:ty, $data_type:expr, $write:ident, $read:ident) => {
		struct $tag($metadata);

		impl ExtensionType for $tag {
			const NAME: &'static str = $name;

			type Metadata = $metadata;

			fn metadata(&self) -> &Self::Metadata {
				&self.0
			}

			fn serialize_metadata(&self) -> Option<String> {
				$write(&self.0)
			}

			fn deserialize_metadata(metadata: Option<&str>) -> StdResult<Self::Metadata, ArrowError> {
				$read(Self::NAME, metadata)
			}

			fn supports_data_type(&self, data_type: &DataType) -> StdResult<(), ArrowError> {
				let expected = $data_type;
				if *data_type == expected {
					Ok(())
				} else {
					Err(ArrowError::InvalidArgumentError(format!(
						"{} needs arrow type {expected}, found {data_type}",
						Self::NAME
					)))
				}
			}

			fn try_new(data_type: &DataType, metadata: Self::Metadata) -> StdResult<Self, ArrowError> {
				let tag = Self(metadata);
				tag.supports_data_type(data_type)?;
				Ok(tag)
			}
		}
	};
}

tag!(Int16Tag, "reifydb.int16", (), DataType::FixedSizeBinary(16), write_nothing, read_nothing);
tag!(Uint16Tag, "reifydb.uint16", (), DataType::FixedSizeBinary(16), write_nothing, read_nothing);
tag!(Uuid4Tag, "reifydb.uuid4", (), DataType::FixedSizeBinary(16), write_nothing, read_nothing);
tag!(Uuid7Tag, "reifydb.uuid7", (), DataType::FixedSizeBinary(16), write_nothing, read_nothing);
tag!(IdentityIdTag, "reifydb.identity_id", (), DataType::FixedSizeBinary(16), write_nothing, read_nothing);
tag!(
	DictionaryIdTag,
	"reifydb.dictionary_id",
	Option<DictionaryId>,
	DataType::FixedSizeBinary(DICTIONARY_ENTRY_WIDTH as i32),
	write_dictionary_id,
	read_dictionary_id
);
tag!(Utf8Tag, "reifydb.utf8", MaxBytes, DataType::LargeUtf8, write_max_bytes, read_max_bytes);
tag!(
	BlobTag,
	"reifydb.blob",
	Option<MaxBytes>,
	DataType::LargeBinary,
	write_optional_max_bytes,
	read_optional_max_bytes
);
tag!(AnyTag, "reifydb.any", Option<ValueType>, DataType::LargeBinary, write_declared_type, read_declared_type);
tag!(DigestTag, "reifydb.digest", DigestMetadata, DataType::LargeBinary, write_digest, read_digest);

#[derive(Deserialize)]
struct MaxBytesMetadata {
	max_bytes: MaxBytes,
}

#[derive(Deserialize)]
struct DictionaryMetadata {
	dictionary_id: u64,
}

#[derive(Deserialize)]
struct DeclaredTypeMetadata {
	declared_type: ValueType,
}

#[derive(Deserialize)]
struct DigestMetadata {
	inner: ValueType,
	accuracy: u32,
}

fn write_nothing(_: &()) -> Option<String> {
	None
}

fn read_nothing(name: &str, metadata: Option<&str>) -> StdResult<(), ArrowError> {
	match metadata {
		None => Ok(()),
		Some(text) => Err(ArrowError::InvalidArgumentError(format!("{name} takes no metadata, found {text}"))),
	}
}

fn write_dictionary_id(dictionary_id: &Option<DictionaryId>) -> Option<String> {
	dictionary_id.map(|id| json!({ "dictionary_id": id.0 }).to_string())
}

fn read_dictionary_id(name: &str, metadata: Option<&str>) -> StdResult<Option<DictionaryId>, ArrowError> {
	metadata.map(|text| parse::<DictionaryMetadata>(name, text).map(|parsed| DictionaryId(parsed.dictionary_id)))
		.transpose()
}

fn write_max_bytes(max_bytes: &MaxBytes) -> Option<String> {
	Some(json!({ "max_bytes": max_bytes.value() }).to_string())
}

fn read_max_bytes(name: &str, metadata: Option<&str>) -> StdResult<MaxBytes, ArrowError> {
	let text = metadata
		.ok_or_else(|| ArrowError::InvalidArgumentError(format!("{name} needs its max_bytes metadata")))?;
	parse::<MaxBytesMetadata>(name, text).map(|parsed| parsed.max_bytes)
}

fn write_optional_max_bytes(max_bytes: &Option<MaxBytes>) -> Option<String> {
	max_bytes.as_ref().and_then(write_max_bytes)
}

fn read_optional_max_bytes(name: &str, metadata: Option<&str>) -> StdResult<Option<MaxBytes>, ArrowError> {
	metadata.map(|text| read_max_bytes(name, Some(text))).transpose()
}

fn write_declared_type(declared_type: &Option<ValueType>) -> Option<String> {
	declared_type.as_ref().map(|declared_type| json!({ "declared_type": declared_type }).to_string())
}

fn read_declared_type(name: &str, metadata: Option<&str>) -> StdResult<Option<ValueType>, ArrowError> {
	metadata.map(|text| parse::<DeclaredTypeMetadata>(name, text).map(|parsed| parsed.declared_type)).transpose()
}

fn write_digest(digest: &DigestMetadata) -> Option<String> {
	Some(json!({ "inner": digest.inner, "accuracy": digest.accuracy }).to_string())
}

fn read_digest(name: &str, metadata: Option<&str>) -> StdResult<DigestMetadata, ArrowError> {
	let text = metadata.ok_or_else(|| {
		ArrowError::InvalidArgumentError(format!("{name} needs its inner and accuracy metadata"))
	})?;
	parse::<DigestMetadata>(name, text)
}

fn parse<T: DeserializeOwned>(name: &str, text: &str) -> StdResult<T, ArrowError> {
	from_str(text).map_err(|error| ArrowError::InvalidArgumentError(format!("{name} metadata {text}: {error}")))
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use arrow_schema::{DataType, Field, TimeUnit, extension::EXTENSION_TYPE_NAME_KEY};
	use serde_json::{Value, from_str};

	use super::{FieldType, from_field, to_field};
	use crate::value::{
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		dictionary::DictionaryId,
		value_type::ValueType,
	};

	fn plain(value_type: ValueType) -> FieldType {
		FieldType {
			value_type: Some(value_type),
			..FieldType::default()
		}
	}

	fn container(value_type: ValueType) -> FieldType {
		FieldType {
			value_type: Some(value_type.clone()),
			declared_type: Some(value_type),
			..FieldType::default()
		}
	}

	fn every_bare_field_type() -> Vec<FieldType> {
		let mut field_types: Vec<FieldType> = [
			ValueType::Boolean,
			ValueType::Float4,
			ValueType::Float8,
			ValueType::Int1,
			ValueType::Int2,
			ValueType::Int4,
			ValueType::Int8,
			ValueType::Int16,
			ValueType::Utf8,
			ValueType::Uint1,
			ValueType::Uint2,
			ValueType::Uint4,
			ValueType::Uint8,
			ValueType::Uint16,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Time,
			ValueType::Duration,
			ValueType::IdentityId,
			ValueType::Uuid4,
			ValueType::Uuid7,
			ValueType::Blob,
			ValueType::Decimal {
				precision: Precision::new(10),
				scale: Scale::new(2),
			},
			ValueType::Decimal {
				precision: Precision::MAX,
				scale: Scale::new(10),
			},
			ValueType::Any,
			ValueType::DictionaryId,
			ValueType::Digest {
				inner: Box::new(ValueType::Float8),
				accuracy: 10_000,
			},
		]
		.into_iter()
		.map(plain)
		.collect();
		field_types.extend([
			FieldType {
				max_bytes: Some(MaxBytes::new(64)),
				..plain(ValueType::Utf8)
			},
			FieldType {
				max_bytes: Some(MaxBytes::new(64)),
				..plain(ValueType::Blob)
			},
			FieldType {
				dictionary_id: Some(DictionaryId(7)),
				..plain(ValueType::DictionaryId)
			},
			container(ValueType::List(Box::new(ValueType::Int4))),
			container(ValueType::Record(vec![
				("a".to_string(), ValueType::Int4),
				("b".to_string(), ValueType::Utf8),
			])),
			container(ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8])),
		]);
		field_types
	}

	fn optional(field_type: &FieldType) -> FieldType {
		FieldType {
			value_type: field_type
				.value_type
				.clone()
				.map(|value_type| ValueType::Option(Box::new(value_type))),
			..field_type.clone()
		}
	}

	fn tagged_field(data_type: DataType, name: &str) -> Field {
		Field::new("c", data_type, false)
			.with_metadata(HashMap::from([(EXTENSION_TYPE_NAME_KEY.to_string(), name.to_string())]))
	}

	fn refusal(field: &Field) -> String {
		from_field(field).expect_err("the field must be refused").diagnostic().message
	}

	#[test]
	fn every_value_type_round_trips_bare_and_in_option() {
		// A detail lost between FieldType and Field would silently change a column type at the batch edge.
		for bare in every_bare_field_type() {
			for field_type in [bare.clone(), optional(&bare)] {
				let field = to_field("c", &field_type);
				assert_eq!(from_field(&field).unwrap(), field_type, "round trip through {field:?}");
			}
		}
	}

	#[test]
	fn an_untyped_none_and_an_optional_any_stay_distinct() {
		// Both report Option(Any), so the map must keep them apart or the None variant is lost.
		let untyped = to_field("c", &FieldType::default());
		let optional_any = to_field("c", &plain(ValueType::Option(Box::new(ValueType::Any))));
		assert_eq!(untyped.data_type(), &DataType::Null);
		assert!(untyped.is_nullable());
		assert_eq!(optional_any.data_type(), &DataType::LargeBinary);
		assert_eq!(optional_any.extension_type_name(), Some("reifydb.any"));
		assert!(optional_any.is_nullable());
		assert_eq!(from_field(&untyped).unwrap(), FieldType::default());
		assert_eq!(from_field(&optional_any).unwrap(), plain(ValueType::Option(Box::new(ValueType::Any))));
	}

	#[test]
	fn tags_and_metadata_match_the_type_map() {
		// Tag names and metadata are a stored format, so each must match the type map table exactly.
		let cases = [
			(plain(ValueType::Int4), DataType::Int32, None, None),
			(plain(ValueType::Int16), DataType::FixedSizeBinary(16), Some("reifydb.int16"), None),
			(plain(ValueType::Uint16), DataType::FixedSizeBinary(16), Some("reifydb.uint16"), None),
			(plain(ValueType::Uuid4), DataType::FixedSizeBinary(16), Some("reifydb.uuid4"), None),
			(plain(ValueType::Uuid7), DataType::FixedSizeBinary(16), Some("reifydb.uuid7"), None),
			(
				plain(ValueType::IdentityId),
				DataType::FixedSizeBinary(16),
				Some("reifydb.identity_id"),
				None,
			),
			(
				plain(ValueType::DictionaryId),
				DataType::FixedSizeBinary(17),
				Some("reifydb.dictionary_id"),
				None,
			),
			(
				FieldType {
					dictionary_id: Some(DictionaryId(7)),
					..plain(ValueType::DictionaryId)
				},
				DataType::FixedSizeBinary(17),
				Some("reifydb.dictionary_id"),
				Some(r#"{"dictionary_id":7}"#),
			),
			(plain(ValueType::Utf8), DataType::LargeUtf8, None, None),
			(
				FieldType {
					max_bytes: Some(MaxBytes::new(64)),
					..plain(ValueType::Utf8)
				},
				DataType::LargeUtf8,
				Some("reifydb.utf8"),
				Some(r#"{"max_bytes":64}"#),
			),
			(plain(ValueType::Blob), DataType::LargeBinary, Some("reifydb.blob"), None),
			(
				FieldType {
					max_bytes: Some(MaxBytes::new(64)),
					..plain(ValueType::Blob)
				},
				DataType::LargeBinary,
				Some("reifydb.blob"),
				Some(r#"{"max_bytes":64}"#),
			),
			(plain(ValueType::Any), DataType::LargeBinary, Some("reifydb.any"), None),
			(
				container(ValueType::List(Box::new(ValueType::Int4))),
				DataType::LargeBinary,
				Some("reifydb.any"),
				Some(r#"{"declared_type":{"List":"Int4"}}"#),
			),
			(
				plain(ValueType::Digest {
					inner: Box::new(ValueType::Float8),
					accuracy: 10_000,
				}),
				DataType::LargeBinary,
				Some("reifydb.digest"),
				Some(r#"{"accuracy":10000,"inner":"Float8"}"#),
			),
			(
				plain(ValueType::DateTime),
				DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
				None,
				None,
			),
			(
				plain(ValueType::Decimal {
					precision: Precision::new(38),
					scale: Scale::new(2),
				}),
				DataType::Decimal128(38, 2),
				None,
				None,
			),
			(
				plain(ValueType::Decimal {
					precision: Precision::new(39),
					scale: Scale::new(2),
				}),
				DataType::Decimal256(39, 2),
				None,
				None,
			),
		];
		for (field_type, data_type, name, metadata) in cases {
			let field = to_field("c", &field_type);
			assert_eq!(field.data_type(), &data_type, "arrow type of {field_type:?}");
			assert_eq!(field.extension_type_name(), name, "tag of {field_type:?}");
			assert_eq!(
				field.extension_type_metadata().map(|text| from_str::<Value>(text).unwrap()),
				metadata.map(|text| from_str::<Value>(text).unwrap()),
				"metadata of {field_type:?}"
			);
		}
	}

	#[test]
	fn list_record_and_tuple_ride_in_any_with_their_declared_type() {
		// Containers are Any bytes, so the declared type is the only place their shape survives the edge.
		for value_type in [
			ValueType::List(Box::new(ValueType::Int4)),
			ValueType::Record(vec![("a".to_string(), ValueType::Int4)]),
			ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]),
		] {
			let field = to_field("c", &container(value_type.clone()));
			assert_eq!(field.data_type(), &DataType::LargeBinary);
			assert_eq!(field.extension_type_name(), Some("reifydb.any"));
			assert_eq!(from_field(&field).unwrap().value_type, Some(value_type));
		}
	}

	#[test]
	fn an_untagged_fixed_size_binary_16_is_refused() {
		// Five types share these 16 bytes, so an untagged one must fail, never guess one of them.
		let message = refusal(&Field::new("c", DataType::FixedSizeBinary(16), false));
		assert!(message.contains("FixedSizeBinary(16)"), "{message}");
	}

	#[test]
	fn an_untagged_large_binary_is_refused() {
		// Blob, Any and Digest share LargeBinary, so an untagged one must fail, never guess one of them.
		let message = refusal(&Field::new("c", DataType::LargeBinary, false));
		assert!(message.contains("LargeBinary"), "{message}");
	}

	#[test]
	fn an_unknown_reifydb_tag_is_refused() {
		// A tag this build does not know must fail, otherwise a newer column type reads as its storage type.
		let message = refusal(&tagged_field(DataType::LargeBinary, "reifydb.foo"));
		assert!(message.contains("reifydb.foo"), "{message}");
	}

	#[test]
	fn a_tag_over_the_wrong_arrow_type_is_refused() {
		// A tag must match its storage width exactly, otherwise rows decode at the wrong stride.
		let message = refusal(&tagged_field(DataType::FixedSizeBinary(17), "reifydb.uuid4"));
		assert!(message.contains("reifydb.uuid4"), "{message}");
	}

	#[test]
	fn an_untagged_large_utf8_reads_as_utf8_with_no_limit() {
		// Computed columns carry no tag, so a plain LargeUtf8 must read as Utf8 with no size limit.
		assert_eq!(from_field(&Field::new("c", DataType::LargeUtf8, false)).unwrap(), plain(ValueType::Utf8));
	}

	#[test]
	#[should_panic(expected = "the details do not fit the value type")]
	fn a_detail_that_does_not_fit_its_type_panics() {
		// A max_bytes on an Int4 has no place in the field, so dropping it silently would lose it.
		to_field(
			"c",
			&FieldType {
				max_bytes: Some(MaxBytes::new(8)),
				..plain(ValueType::Int4)
			},
		);
	}

	#[test]
	#[should_panic(expected = "a nested Option has no arrow field")]
	fn a_nested_option_panics() {
		// Arrow has one nullable flag, so a second Option layer can not be written and must not be dropped.
		to_field("c", &plain(ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Int4))))));
	}

	#[test]
	fn from_a_value_type_sets_only_the_value_type() {
		// A conversion that invented a dictionary id or size limit would change what the column accepts.
		let optional = ValueType::Option(Box::new(ValueType::Utf8));
		assert_eq!(
			FieldType::from(optional.clone()),
			FieldType {
				value_type: Some(optional),
				max_bytes: None,
				dictionary_id: None,
				declared_type: None,
			}
		);
		assert_eq!(FieldType::from(ValueType::Int4), plain(ValueType::Int4));
	}
}
