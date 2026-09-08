// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, collections::Bound};

use reifydb_codec::key::{
	ByteSink, decode_u64_from,
	deserializer::KeyDeserializer,
	encode_u64,
	encoded::{EncodedKey, EncodedKeyBuilder, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::KeyCodec;
use reifydb_value::{
	Result,
	value::{dictionary::DictionaryId, sumtype::SumTypeId},
};
use smallvec::{SmallVec, smallvec};

use super::{KeyRangeCodec, KeyTag};
use crate::{
	interface::catalog::{
		id::{
			BindingId, ColumnId, ColumnPropertyId, HandlerId, IndexId, NamespaceId, PrimaryKeyId,
			RelationshipId, SinkId, SourceId, TableId, ViewId,
		},
		object::ObjectId,
	},
	key::{
		any::{Field, KeyFields, RawEncoding, Width, index_tag},
		bound::{TaggedKeyBound, TaggedKeyBoundRange, object_fields},
	},
	return_internal_error,
	value::index::{encoded::EncodedIndexKey, range::EncodedIndexKeyRange},
};

pub fn serialize_object_id<B: ByteSink>(object: &ObjectId, out: &mut B) {
	out.push(object.type_tag());
	out.extend_from_slice(&encode_u64(object.as_u64()));
}

pub fn deserialize_object_id(input: &mut &[u8]) -> Result<ObjectId> {
	if input.is_empty() {
		return_internal_error!("Invalid ObjectId encoding: empty input");
	}

	let type_byte = input[0];
	*input = &input[1..];
	let id = decode_u64_from(input)?;

	match ObjectId::from_type_tag(type_byte, id) {
		Some(object) => Ok(object),
		None => return_internal_error!("Invalid ObjectId type byte: 0x{:02x}.", type_byte),
	}
}

pub fn serialize_index_id<B: ByteSink>(index: &IndexId, out: &mut B) {
	match index {
		IndexId::Primary(PrimaryKeyId(id)) => {
			out.push(0x01);
			out.extend_from_slice(&encode_u64(*id));
		}
	}
}

pub fn deserialize_index_id(input: &mut &[u8]) -> Result<IndexId> {
	if input.is_empty() {
		return_internal_error!("Invalid IndexId encoding: empty input");
	}

	let type_byte = input[0];
	*input = &input[1..];
	let id = decode_u64_from(input)?;

	match type_byte {
		0x01 => Ok(IndexId::Primary(PrimaryKeyId(id))),

		_ => return_internal_error!("Invalid IndexId type byte: 0x{:02x}.", type_byte),
	}
}

pub trait KeySerializerCatalogExt {
	fn extend_object_id(&mut self, object: impl Into<ObjectId>) -> &mut Self;
	fn extend_index_id(&mut self, index: impl Into<IndexId>) -> &mut Self;
}

impl KeySerializerCatalogExt for KeySerializer {
	fn extend_object_id(&mut self, object: impl Into<ObjectId>) -> &mut Self {
		let mut buf = Vec::new();
		serialize_object_id(&object.into(), &mut buf);
		self.extend_raw(&buf);
		self
	}

	fn extend_index_id(&mut self, index: impl Into<IndexId>) -> &mut Self {
		let mut buf = Vec::new();
		serialize_index_id(&index.into(), &mut buf);
		self.extend_raw(&buf);
		self
	}
}

pub trait KeyDeserializerCatalogExt {
	fn read_object_id(&mut self) -> Result<ObjectId>;
	fn read_index_id(&mut self) -> Result<IndexId>;
}

impl KeyDeserializerCatalogExt for KeyDeserializer<'_> {
	fn read_object_id(&mut self) -> Result<ObjectId> {
		let mut slice = self.remaining_bytes();
		let before = slice.len();
		let object_id = deserialize_object_id(&mut slice)?;
		self.read_raw(before - slice.len())?;
		Ok(object_id)
	}

	fn read_index_id(&mut self) -> Result<IndexId> {
		let mut slice = self.remaining_bytes();
		let before = slice.len();
		let index_id = deserialize_index_id(&mut slice)?;
		self.read_raw(before - slice.len())?;
		Ok(index_id)
	}
}

pub trait EncodedKeyBuilderCatalogExt {
	fn object_id(self, object: impl Into<ObjectId>) -> Self;
	fn index_id(self, index: impl Into<IndexId>) -> Self;
}

impl EncodedKeyBuilderCatalogExt for EncodedKeyBuilder {
	fn object_id(self, object: impl Into<ObjectId>) -> Self {
		let mut buf = Vec::new();
		serialize_object_id(&object.into(), &mut buf);
		self.raw(&buf)
	}

	fn index_id(self, index: impl Into<IndexId>) -> Self {
		let mut buf = Vec::new();
		serialize_index_id(&index.into(), &mut buf);
		self.raw(&buf)
	}
}

#[cfg(test)]
pub mod index_entry_key_tests {
	use reifydb_codec::key::encode_u64;

	use super::{
		serialize_index_id as serialize_index_id_inner, serialize_object_id as serialize_object_id_inner, *,
	};
	use crate::interface::catalog::vtable::VTableId;

	fn serialize_object_id(object: &ObjectId) -> Vec<u8> {
		let mut out = Vec::new();
		serialize_object_id_inner(object, &mut out);
		out
	}

	fn serialize_index_id(index: &IndexId) -> Vec<u8> {
		let mut out = Vec::new();
		serialize_index_id_inner(index, &mut out);
		out
	}

	#[test]
	fn test_object_id_ordering() {
		let object1 = ObjectId::table(1);
		let object2 = ObjectId::table(2);
		let object100 = ObjectId::table(100);
		let object200 = ObjectId::table(200);

		let bytes1 = serialize_object_id(&object1);
		let bytes2 = serialize_object_id(&object2);
		let bytes100 = serialize_object_id(&object100);
		let bytes200 = serialize_object_id(&object200);

		assert!(bytes2 < bytes1, "object(2) should be < object(1) in bytes");
		assert!(bytes200 < bytes100, "object(200) should be < object(100) in bytes");
		assert!(bytes100 < bytes2, "object(100) should be < object(2) in bytes");
	}

	#[test]
	fn test_range_boundaries() {
		let object10 = ObjectId::table(10);
		let object9 = object10.prev();

		let bytes10 = serialize_object_id(&object10);
		let bytes9 = serialize_object_id(&object9);

		assert!(bytes9 > bytes10, "object(9) should be > object(10) in bytes");

		let view10 = ObjectId::view(10);
		let view9 = view10.prev();

		let vbytes10 = serialize_object_id(&view10);
		let vbytes9 = serialize_object_id(&view9);

		assert!(vbytes9 > vbytes10, "view(9) should be > view(10) in bytes");

		let virtual10 = ObjectId::vtable(10);
		let virtual9 = virtual10.prev();

		let tvbytes10 = serialize_object_id(&virtual10);
		let tvbytes9 = serialize_object_id(&virtual9);

		assert!(tvbytes9 > tvbytes10, "vtable(9) should be > vtable(10) in bytes");

		assert_ne!(bytes10, vbytes10, "table(10) should != view(10)");
		assert_ne!(bytes10, tvbytes10, "table(10) should != vtable(10)");
		assert_ne!(vbytes10, tvbytes10, "view(10) should != vtable(10)");
		assert_eq!(bytes10[0], 0x01, "table type byte should be 0x01");
		assert_eq!(vbytes10[0], 0x02, "view type byte should be 0x02");
		assert_eq!(tvbytes10[0], 0x03, "vtable type byte should be 0x03");

		let row_key_10_100 = vec![0xFC];
		let mut key1 = row_key_10_100.clone();
		key1.extend(&bytes10);
		key1.extend(&encode_u64(100u64));

		let mut key2 = row_key_10_100.clone();
		key2.extend(&bytes10);
		key2.extend(&encode_u64(200u64));

		let mut end_key = vec![0xFC];
		end_key.extend(&bytes9);

		assert!(key1 >= bytes10, "key1 should be >= start(object10)");
		assert!(key1 < end_key, "key1 should be < end(object9)");
		assert!(key2 >= bytes10, "key2 should be >= start(object10)");
		assert!(key2 < end_key, "key2 should be < end(object9)");
	}

	#[test]
	fn test_vtable_serialization() {
		let virtual_object = ObjectId::vtable(42);
		let bytes = serialize_object_id(&virtual_object);
		let mut slice = &bytes[..];
		let deserialized = deserialize_object_id(&mut slice).unwrap();
		assert_eq!(virtual_object, deserialized);
		assert!(slice.is_empty());

		assert_eq!(bytes[0], 0x03);

		let virtual_id = VTableId(123);
		let object_from_id = ObjectId::from(virtual_id);
		let bytes_from_id = serialize_object_id(&object_from_id);
		let mut slice = &bytes_from_id[..];
		let deserialized_id = deserialize_object_id(&mut slice).unwrap();
		assert_eq!(object_from_id, deserialized_id);
		assert!(slice.is_empty());

		let virtual1 = ObjectId::vtable(1);
		let virtual2 = ObjectId::vtable(2);
		let bytes1 = serialize_object_id(&virtual1);
		let bytes2 = serialize_object_id(&virtual2);

		assert!(bytes2 < bytes1, "vtable(2) should be < vtable(1) in bytes");
	}

	#[test]
	fn test_index_id_serialization() {
		let index = IndexId::primary(42);
		let bytes = serialize_index_id(&index);
		let mut slice = &bytes[..];
		let deserialized = deserialize_index_id(&mut slice).unwrap();
		assert_eq!(index.as_u64(), deserialized.as_u64());
		assert!(slice.is_empty());

		assert_eq!(bytes[0], 0x01);

		let primary_id = PrimaryKeyId(123);
		let index_from_id = IndexId::Primary(primary_id);
		let bytes_from_id = serialize_index_id(&index_from_id);
		let mut slice = &bytes_from_id[..];
		let deserialized_id = deserialize_index_id(&mut slice).unwrap();
		assert_eq!(index_from_id.as_u64(), deserialized_id.as_u64());
		assert!(slice.is_empty());
	}

	#[test]
	fn test_index_id_ordering() {
		let index1 = IndexId::primary(1);
		let index2 = IndexId::primary(2);
		let index100 = IndexId::primary(100);
		let index200 = IndexId::primary(200);

		let bytes1 = serialize_index_id(&index1);
		let bytes2 = serialize_index_id(&index2);
		let bytes100 = serialize_index_id(&index100);
		let bytes200 = serialize_index_id(&index200);

		assert!(bytes2 < bytes1, "index(2) should be < index(1) in bytes");
		assert!(bytes200 < bytes100, "index(200) should be < index(100) in bytes");
		assert!(bytes100 < bytes2, "index(100) should be < index(2) in bytes");
	}

	#[test]
	fn test_index_id_range_boundaries() {
		let index10 = IndexId::primary(10);
		let index11 = IndexId::primary(11);

		let bytes10 = serialize_index_id(&index10);
		let bytes11 = serialize_index_id(&index11);

		assert!(bytes11 < bytes10, "index(11) should be < index(10) in bytes");

		assert_eq!(bytes10.len(), 9, "IndexId(10) should be 9 bytes");
		assert_eq!(bytes10[0], 0x01, "Primary variant should have type byte 0x01");

		let next_index = IndexId::primary(11);
		let next_bytes = serialize_index_id(&next_index);

		assert!(next_bytes < bytes10, "index(11) should be < index(10) for proper range boundaries");
	}

	#[test]
	fn test_index_entry_key_encoding_with_discriminator() {
		let object = ObjectId::table(42);
		let index = IndexId::primary(7);

		let object_bytes = serialize_object_id(&object);
		let index_bytes = serialize_index_id(&index);

		assert_eq!(object_bytes.len(), 9, "ObjectId(42) should be 9 bytes");
		assert_eq!(index_bytes.len(), 9, "IndexId(7) should be 9 bytes");

		assert_eq!(object_bytes[0], 0x01, "Table object should have type byte 0x01");
		assert_eq!(index_bytes[0], 0x01, "Primary index should have type byte 0x01");

		let total_prefix_size = 1 + 1 + object_bytes.len() + index_bytes.len();
		assert_eq!(total_prefix_size, 20, "Total IndexEntryKey prefix should be 20 bytes");
	}
}

#[cfg(test)]
mod moved_catalog_key_tests {
	use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer};

	use super::{KeyDeserializerCatalogExt, KeySerializerCatalogExt};
	use crate::interface::catalog::{
		id::{IndexId, PrimaryKeyId, TableId},
		object::ObjectId,
	};

	#[test]
	fn test_index_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_index_id(IndexId::Primary(PrimaryKeyId(123456789)));
		let result = serializer.finish();

		// A fixed-width id keeps every IndexId the same length, so a constant prefix scan stays exact.
		assert_eq!(result.len(), 9);
		assert_eq!(result[0], 0x01); // Primary variant prefix

		// Ids are stored bitwise-inverted, so the smaller id encodes to the larger bytes; the
		// comparison skips byte 0 because that is the variant prefix.
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_index_id(IndexId::Primary(PrimaryKeyId(1)));
		let result2 = serializer2.finish();

		assert!(result2[1..] > result[1..]);
	}

	#[test]
	fn test_object_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_object_id(ObjectId::Table(TableId(987654321)));
		let result = serializer.finish();

		// A fixed-width id keeps every ObjectId the same length, so a constant prefix scan stays exact.
		assert_eq!(result.len(), 9);
		assert_eq!(result[0], 0x01); // Table variant prefix

		// Inverted encoding: the larger id sorts below the smaller one; byte 0 is the variant prefix.
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_object_id(ObjectId::Table(TableId(987654322)));
		let result2 = serializer2.finish();

		assert!(result2[1..] < result[1..]);
	}

	#[test]
	fn test_read_object_id() {
		let mut ser = KeySerializer::new();
		let object = ObjectId::table(42);
		ser.extend_object_id(object);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_object_id().unwrap(), object);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_index_id() {
		let mut ser = KeySerializer::new();
		let index = IndexId::primary(999);
		ser.extend_index_id(index);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_index_id().unwrap(), index);
		assert!(de.is_empty());
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Dictionary)]
pub struct DictionaryKey {
	pub dictionary: DictionaryId,
}

impl DictionaryKey {
	pub fn new(dictionary: DictionaryId) -> Self {
		Self {
			dictionary,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>) -> EncodedKey {
		Self::new(dictionary.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = DictionaryEntry)]
pub struct DictionaryEntryKey {
	pub dictionary: DictionaryId,
	pub hash: [u8; 16],
}

impl DictionaryEntryKey {
	pub fn new(dictionary: DictionaryId, hash: [u8; 16]) -> Self {
		Self {
			dictionary,
			hash,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>, hash: [u8; 16]) -> EncodedKey {
		Self::new(dictionary.into(), hash).encode()
	}

	pub fn full_scan(dictionary: DictionaryId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, dictionary.0 as u128)])
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct DictionaryEntryIndexKey {
	pub dictionary: DictionaryId,
	pub id: u128,
}

impl DictionaryEntryIndexKey {
	pub fn new(dictionary: DictionaryId, id: u128) -> Self {
		Self {
			dictionary,
			id,
		}
	}

	pub fn encoded(dictionary: impl Into<DictionaryId>, id: u128) -> EncodedKey {
		Self::new(dictionary.into(), id).encode()
	}

	pub fn full_scan(dictionary: DictionaryId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, dictionary.0 as u128)])
	}
}

impl DictionaryEntryIndexKey {
	pub const TAG: KeyTag = KeyTag::DictionaryEntryIndex;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::TAG as u8).extend_u64(self.dictionary).extend_u128_varint(self.id);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let dictionary = de.read_u64().ok()?;
		let id = de.read_u128_varint().ok()?;

		Some(Self {
			dictionary: DictionaryId(dictionary),
			id,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEntryIndexKeyRange {
	pub dictionary: DictionaryId,
	pub start_id: Option<u128>,
	pub end_id: Option<u128>,
}

impl DictionaryEntryIndexKeyRange {
	pub fn new(dictionary: DictionaryId, start_id: Option<u128>, end_id: Option<u128>) -> Self {
		Self {
			dictionary,
			start_id,
			end_id,
		}
	}

	pub fn full(dictionary: DictionaryId) -> Self {
		Self {
			dictionary,
			start_id: None,
			end_id: None,
		}
	}
}

impl KeyRangeCodec for DictionaryEntryIndexKeyRange {
	const TAG: KeyTag = KeyTag::DictionaryEntryIndex;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::TAG as u8).extend_u64(self.dictionary);
		if let Some(id) = self.start_id {
			serializer.extend_u128_varint(id);
		}
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		if let Some(id) = self.end_id {
			let mut serializer = KeySerializer::with_capacity(25);
			serializer.extend_u8(Self::TAG as u8).extend_u64(self.dictionary).extend_u128_varint(id - 1);
			Some(serializer.to_encoded_key())
		} else {
			let mut serializer = KeySerializer::with_capacity(9);
			serializer.extend_u8(Self::TAG as u8).extend_u64(*self.dictionary - 1);
			Some(serializer.to_encoded_key())
		}
	}

	fn decode(_range: &EncodedKeyRange) -> (Option<Self>, Option<Self>) {
		(None, None)
	}
}

#[cfg(test)]
pub mod dictionary_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_dictionary_key_encode_decode() {
		let key = DictionaryKey {
			dictionary: DictionaryId(0x1234),
		};
		let encoded = key.encode();
		let decoded = DictionaryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
	}

	#[test]
	fn test_dictionary_entry_key_encode_decode() {
		let key = DictionaryEntryKey {
			dictionary: DictionaryId(42),
			hash: [
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
				0x0f, 0x10,
			],
		};
		let encoded = key.encode();
		let decoded = DictionaryEntryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
		assert_eq!(decoded.hash, key.hash);
	}

	#[test]
	fn a_dictionary_entry_hash_made_of_0xff_bytes_still_round_trips() {
		// the hand-rolled encoder escaped 0xff and appended a terminator while decode read 16 raw
		// bytes, so any hash carrying an 0xff came back shifted and the key ran two bytes long
		let key = DictionaryEntryKey {
			dictionary: DictionaryId(42),
			hash: [0xff; 16],
		};
		let encoded = key.encode();
		assert_eq!(encoded.len(), 1 + 8 + 16);
		assert_eq!(DictionaryEntryKey::decode(&encoded).unwrap(), key);
	}

	#[test]
	fn test_dictionary_entry_index_key_encode_decode() {
		let key = DictionaryEntryIndexKey {
			dictionary: DictionaryId(99),
			id: 12345,
		};
		let encoded = key.encode();
		let decoded = DictionaryEntryIndexKey::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
		assert_eq!(decoded.id, key.id);
	}

	#[test]
	fn test_dictionary_key_full_scan() {
		let range = DictionaryKey::full_scan();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_key_full_scan() {
		let range = DictionaryEntryKey::full_scan(DictionaryId(42)).encode();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_index_key_full_scan() {
		let range = DictionaryEntryIndexKey::full_scan(DictionaryId(42)).encode();
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_index_key_range() {
		let range = DictionaryEntryIndexKeyRange::full(DictionaryId(42));
		let start = range.start();
		let end = range.end();
		assert!(start.is_some());
		assert!(end.is_some());
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Index)]
pub struct IndexKey {
	pub object: ObjectId,
	pub index: IndexId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectIndexKeyRange {
	pub object: ObjectId,
}

impl ObjectIndexKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let object = de.read_object_id().ok()?;

		Some(ObjectIndexKeyRange {
			object,
		})
	}
}

impl KeyRangeCodec for ObjectIndexKeyRange {
	const TAG: KeyTag = KeyTag::Index;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::TAG as u8).extend_object_id(self.object);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::TAG as u8).extend_object_id(self.object.prev());
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl IndexKey {
	pub fn new(object: impl Into<ObjectId>, index: impl Into<IndexId>) -> Self {
		Self {
			object: object.into(),
			index: index.into(),
		}
	}

	pub fn encoded(object: impl Into<ObjectId>, index: impl Into<IndexId>) -> EncodedKey {
		Self {
			object: object.into(),
			index: index.into(),
		}
		.encode()
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, object_fields(object.into()))
	}
}

#[cfg(test)]
pub mod index_key_tests {
	use super::IndexKey;
	use crate::interface::catalog::{id::IndexId, object::ObjectId};

	#[test]
	fn test_encode_decode() {
		let key = IndexKey {
			object: ObjectId::table(0xABCD),
			index: IndexId::primary(0x123456789ABCDEF0u64),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xF3, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43,
			0x21, 0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = IndexKey::decode(&encoded).unwrap();
		assert_eq!(key.object, 0xABCD);
		assert_eq!(key.index, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = IndexKey {
			object: ObjectId::table(1),
			index: IndexId::primary(100),
		};
		let key2 = IndexKey {
			object: ObjectId::table(1),
			index: IndexId::primary(200),
		};
		let key3 = IndexKey {
			object: ObjectId::table(2),
			index: IndexId::primary(50),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct IndexEntryKey {
	pub object: ObjectId,
	pub index: IndexId,
	pub key: EncodedIndexKey,
}

impl IndexEntryKey {
	pub fn new(object: impl Into<ObjectId>, index: IndexId, key: EncodedIndexKey) -> Self {
		Self {
			object: object.into(),
			index,
			key,
		}
	}

	pub fn encoded(object: impl Into<ObjectId>, index: IndexId, key: EncodedIndexKey) -> EncodedKey {
		Self::new(object, index, key).encode()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntryKeyRange {
	pub object: ObjectId,
	pub index: IndexId,
}

impl IndexEntryKeyRange {
	fn decode_key(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let object = de.read_object_id().ok()?;
		let index = de.read_index_id().ok()?;

		Some(IndexEntryKeyRange {
			object,
			index,
		})
	}
}

impl KeyRangeCodec for IndexEntryKeyRange {
	const TAG: KeyTag = KeyTag::IndexEntry;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::TAG as u8).extend_object_id(self.object).extend_index_id(self.index);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::TAG as u8).extend_object_id(self.object).extend_index_id(self.index.prev());
		Some(serializer.to_encoded_key())
	}

	fn decode(range: &EncodedKeyRange) -> (Option<Self>, Option<Self>)
	where
		Self: Sized,
	{
		let start_key = match &range.start {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		let end_key = match &range.end {
			Bound::Included(key) | Bound::Excluded(key) => Self::decode_key(key),
			Bound::Unbounded => None,
		};

		(start_key, end_key)
	}
}

impl IndexEntryKey {
	pub const TAG: KeyTag = KeyTag::IndexEntry;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.key.len());
		serializer
			.extend_u8(Self::TAG as u8)
			.extend_object_id(self.object)
			.extend_index_id(self.index)
			.extend_raw(self.key.as_slice());
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::TAG {
			return None;
		}

		let object = de.read_object_id().ok()?;
		let index = de.read_index_id().ok()?;

		let remaining = de.remaining();
		let remaining_bytes = de.read_raw(remaining).ok()?;
		Some(Self {
			object,
			index,
			key: EncodedIndexKey::new(remaining_bytes),
		})
	}
}

impl IndexEntryKey {
	pub fn index_range(object: impl Into<ObjectId>, index: IndexId) -> TaggedKeyBoundRange {
		let object = object.into();
		TaggedKeyBoundRange::prefix(
			<IndexEntryKeyRange as KeyRangeCodec>::TAG,
			object_fields(object)
				.into_iter()
				.chain([Field::UAsc(Width::U8, 1), Field::UDesc(Width::U64, index.as_u64() as u128)]),
		)
	}

	pub fn object_range(object: impl Into<ObjectId>) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(KeyTag::IndexEntry, object_fields(object.into()))
	}

	pub fn key_prefix_range(object: impl Into<ObjectId>, index: IndexId, key_prefix: &[u8]) -> TaggedKeyBoundRange {
		let object = object.into();
		TaggedKeyBoundRange::prefix(
			KeyTag::IndexEntry,
			object_fields(object).into_iter().chain([
				Field::UAsc(Width::U8, 1),
				Field::UDesc(Width::U64, index.as_u64() as u128),
				Field::RawAsc(RawEncoding::Verbatim, Cow::Owned(key_prefix.to_vec())),
			]),
		)
	}

	pub fn key_range(
		object: impl Into<ObjectId>,
		index: IndexId,
		index_range: EncodedIndexKeyRange,
	) -> TaggedKeyBoundRange {
		let object = object.into();
		let head = || {
			object_fields(object)
				.into_iter()
				.chain([Field::UAsc(Width::U8, 1), Field::UDesc(Width::U64, index.as_u64() as u128)])
		};
		let at = |key: &EncodedIndexKey| {
			TaggedKeyBound::prefix(
				KeyTag::IndexEntry,
				head().chain([Field::RawAsc(
					RawEncoding::Verbatim,
					Cow::Owned(key.as_slice().to_vec()),
				)]),
			)
		};

		let start = match &index_range.start {
			Bound::Included(key) => Bound::Included(at(key)),
			Bound::Excluded(key) => Bound::Excluded(at(key)),
			Bound::Unbounded => Bound::Included(TaggedKeyBound::prefix(KeyTag::IndexEntry, head())),
		};

		let end = match &index_range.end {
			Bound::Included(key) => Bound::Included(at(key)),
			Bound::Excluded(key) => Bound::Excluded(at(key)),
			Bound::Unbounded => Bound::Excluded(TaggedKeyBound::prefix_end(KeyTag::IndexEntry, head())),
		};

		TaggedKeyBoundRange {
			start,
			end,
		}
	}
}

#[cfg(test)]
pub mod index_entry_key_tests_2 {
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::{sort::SortDirection, value::index::shape::IndexShape};

	#[test]
	fn test_encode_decode() {
		let layout = IndexShape::new(
			&[ValueType::Uint8, ValueType::Uint8],
			&[SortDirection::Asc, SortDirection::Asc],
		)
		.unwrap();

		let mut index_key = layout.allocate_key();
		layout.set_u64(&mut index_key, 0, 100u64);
		layout.set_row_number(&mut index_key, 1, 1u64);

		let entry = IndexEntryKey {
			object: ObjectId::table(42),
			index: IndexId::primary(7),
			key: index_key.clone(),
		};

		let encoded = entry.encode();
		let decoded = IndexEntryKey::decode(&encoded).unwrap();

		assert_eq!(decoded.object, ObjectId::table(42));
		assert_eq!(decoded.index, IndexId::primary(7));
		assert_eq!(decoded.key.as_slice(), index_key.as_slice());
	}

	#[test]
	fn test_ordering() {
		let layout = IndexShape::new(&[ValueType::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key1 = layout.allocate_key();
		layout.set_u64(&mut key1, 0, 100u64);

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);

		let entry1 = IndexEntryKey {
			object: ObjectId::table(1),
			index: IndexId::primary(1),
			key: key1,
		};

		let entry2 = IndexEntryKey {
			object: ObjectId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded1 = entry1.encode();
		let encoded2 = entry2.encode();

		assert!(encoded1.as_slice() < encoded2.as_slice());
	}

	#[test]
	fn test_index_range() {
		let range = IndexEntryKey::index_range(ObjectId::table(10), IndexId::primary(5)).encode();

		let layout = IndexShape::new(&[ValueType::Uint8], &[SortDirection::Asc]).unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 50u64);

		let entry = IndexEntryKey {
			object: ObjectId::table(10),
			index: IndexId::primary(5),
			key,
		};

		let encoded = entry.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		} else {
			panic!("Expected Included/Excluded bounds");
		}

		let entry2 = IndexEntryKey {
			object: ObjectId::table(10),
			index: IndexId::primary(6),
			key: layout.allocate_key(),
		};

		let encoded2 = entry2.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded2.as_slice() < start.as_slice() || encoded2.as_slice() >= end.as_slice());
		}
	}

	#[test]
	fn test_key_prefix_range() {
		let layout = IndexShape::new(
			&[ValueType::Uint8, ValueType::Uint8],
			&[SortDirection::Asc, SortDirection::Asc],
		)
		.unwrap();

		let mut key = layout.allocate_key();
		layout.set_u64(&mut key, 0, 100u64);
		layout.set_row_number(&mut key, 1, 0u64);

		let prefix = &key.as_slice()[..layout.fields[1].offset];
		let range = IndexEntryKey::key_prefix_range(ObjectId::table(1), IndexId::primary(1), prefix).encode();

		layout.set_row_number(&mut key, 1, 999u64);
		let entry = IndexEntryKey {
			object: ObjectId::table(1),
			index: IndexId::primary(1),
			key: key.clone(),
		};

		let encoded = entry.encode();

		if let (Bound::Included(start), Bound::Excluded(end)) = (&range.start, &range.end) {
			assert!(encoded.as_slice() >= start.as_slice());
			assert!(encoded.as_slice() < end.as_slice());
		}

		let mut key2 = layout.allocate_key();
		layout.set_u64(&mut key2, 0, 200u64);
		layout.set_row_number(&mut key2, 1, 1u64);

		let entry2 = IndexEntryKey {
			object: ObjectId::table(1),
			index: IndexId::primary(1),
			key: key2,
		};

		let encoded2 = entry2.encode();

		if let Bound::Excluded(end) = &range.end {
			assert!(encoded2.as_slice() >= end.as_slice());
		}
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SumTypeKey {
	pub sumtype: SumTypeId,
}

impl SumTypeKey {
	pub fn new(sumtype: SumTypeId) -> Self {
		Self {
			sumtype,
		}
	}

	pub fn encoded(sumtype: impl Into<SumTypeId>) -> EncodedKey {
		Self::new(sumtype.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

impl SumTypeKey {
	pub const TAG: KeyTag = KeyTag::SumType;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(SumTypeKey::TAG as u8).extend_u64(self.sumtype);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != SumTypeKey::TAG {
			return None;
		}

		let sumtype = de.read_u64().ok()?;

		Some(Self {
			sumtype: SumTypeId(sumtype),
		})
	}
}

#[cfg(test)]
mod sum_type_key_tests {
	use reifydb_value::value::sumtype::SumTypeId;

	use super::SumTypeKey;

	#[test]
	fn test_encode_decode() {
		let key = SumTypeKey {
			sumtype: SumTypeId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = SumTypeKey::decode(&encoded).unwrap();
		assert_eq!(decoded.sumtype, SumTypeId(0xABCD));
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ViewKey {
	pub view: ViewId,
}

impl ViewKey {
	pub const TAG: KeyTag = KeyTag::View;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(ViewKey::TAG as u8).extend_u64(self.view);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != ViewKey::TAG {
			return None;
		}

		let view = de.read_u64().ok()?;

		Some(Self {
			view: ViewId(view),
		})
	}
}

impl ViewKey {
	pub fn new(view: impl Into<ViewId>) -> Self {
		Self {
			view: view.into(),
		}
	}

	pub fn encoded(view: impl Into<ViewId>) -> EncodedKey {
		Self::new(view).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod view_key_tests {
	use super::ViewKey;
	use crate::interface::catalog::id::ViewId;

	#[test]
	fn test_encode_decode() {
		let key = ViewKey {
			view: ViewId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = ViewKey::decode(&encoded).unwrap();
		assert_eq!(key.view, 0xABCD);
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Table)]
pub struct TableKey {
	pub table: TableId,
}

impl TableKey {
	pub fn new(table: impl Into<TableId>) -> Self {
		Self {
			table: table.into(),
		}
	}

	pub fn encoded(table: impl Into<TableId>) -> EncodedKey {
		Self::new(table).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod table_key_tests {
	use super::TableKey;
	use crate::interface::catalog::id::TableId;

	#[test]
	fn test_encode_decode() {
		let key = TableKey {
			table: TableId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = TableKey::decode(&encoded).unwrap();
		assert_eq!(key.table, 0xABCD);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = TableKey {
			table: TableId(1),
		};
		let key2 = TableKey {
			table: TableId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SourceKey {
	pub source: SourceId,
}

impl SourceKey {
	pub const TAG: KeyTag = KeyTag::Source;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(SourceKey::TAG as u8).extend_u64(self.source);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != SourceKey::TAG {
			return None;
		}

		let source = de.read_u64().ok()?;

		Some(Self {
			source: SourceId(source),
		})
	}
}

impl SourceKey {
	pub fn new(source: impl Into<SourceId>) -> Self {
		Self {
			source: source.into(),
		}
	}

	pub fn encoded(source: impl Into<SourceId>) -> EncodedKey {
		Self::new(source).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod source_key_tests {
	use super::SourceKey;
	use crate::interface::catalog::id::SourceId;

	#[test]
	fn test_encode_decode() {
		let key = SourceKey {
			source: SourceId(0x1234),
		};
		let encoded = key.encode();
		let decoded = SourceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.source, SourceId(0x1234));
		assert_eq!(key, decoded);
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SinkKey {
	pub sink: SinkId,
}

impl SinkKey {
	pub const TAG: KeyTag = KeyTag::Sink;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(SinkKey::TAG as u8).extend_u64(self.sink);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != SinkKey::TAG {
			return None;
		}

		let sink = de.read_u64().ok()?;

		Some(Self {
			sink: SinkId(sink),
		})
	}
}

impl SinkKey {
	pub fn new(sink: impl Into<SinkId>) -> Self {
		Self {
			sink: sink.into(),
		}
	}

	pub fn encoded(sink: impl Into<SinkId>) -> EncodedKey {
		Self::new(sink).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod sink_key_tests {
	use super::SinkKey;
	use crate::interface::catalog::id::SinkId;

	#[test]
	fn test_encode_decode() {
		let key = SinkKey {
			sink: SinkId(0x1234),
		};
		let encoded = key.encode();
		let decoded = SinkKey::decode(&encoded).unwrap();
		assert_eq!(decoded.sink, SinkId(0x1234));
		assert_eq!(key, decoded);
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Relationship)]
pub struct RelationshipKey {
	pub relationship: RelationshipId,
}

impl RelationshipKey {
	pub fn new(relationship: impl Into<RelationshipId>) -> Self {
		Self {
			relationship: relationship.into(),
		}
	}

	pub fn encoded(relationship: impl Into<RelationshipId>) -> EncodedKey {
		Self::new(relationship).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod relationship_key_tests {
	use super::RelationshipKey;
	use crate::interface::catalog::id::RelationshipId;

	#[test]
	fn test_encode_decode() {
		let key = RelationshipKey {
			relationship: RelationshipId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = RelationshipKey::decode(&encoded).unwrap();
		assert_eq!(decoded.relationship, RelationshipId(0xABCD));
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = ColumnProperty)]
pub struct ColumnPropertyKey {
	pub column: ColumnId,
	pub property: ColumnPropertyId,
}

impl ColumnPropertyKey {
	pub fn new(column: impl Into<ColumnId>, property: impl Into<ColumnPropertyId>) -> Self {
		Self {
			column: column.into(),
			property: property.into(),
		}
	}

	pub fn encoded(column: impl Into<ColumnId>, property: impl Into<ColumnPropertyId>) -> EncodedKey {
		Self::new(column, property).encode()
	}

	pub fn full_scan(column: ColumnId) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(Self::TAG, [Field::UDesc(Width::U64, column.0 as u128)])
	}
}

#[cfg(test)]
pub mod column_property_key_tests {
	use super::ColumnPropertyKey;
	use crate::interface::catalog::id::{ColumnId, ColumnPropertyId};

	#[test]
	fn test_encode_decode() {
		let key = ColumnPropertyKey {
			column: ColumnId(0xABCD),
			property: ColumnPropertyId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xF6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = ColumnPropertyKey::decode(&encoded).unwrap();
		assert_eq!(key.column, 0xABCD);
		assert_eq!(key.property, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(100),
		};
		let key2 = ColumnPropertyKey {
			column: ColumnId(1),
			property: ColumnPropertyId(200),
		};
		let key3 = ColumnPropertyKey {
			column: ColumnId(2),
			property: ColumnPropertyId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = Handler)]
pub struct HandlerKey {
	pub handler: HandlerId,
}

impl HandlerKey {
	pub fn new(handler: HandlerId) -> Self {
		Self {
			handler,
		}
	}

	pub fn encoded(handler: impl Into<HandlerId>) -> EncodedKey {
		Self::new(handler.into()).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod handler_key_tests {
	use super::HandlerKey;
	use crate::interface::catalog::id::HandlerId;

	#[test]
	fn test_encode_decode() {
		let key = HandlerKey {
			handler: HandlerId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xD4, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = HandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.handler, HandlerId(0xABCD));
	}

	#[test]
	fn test_order_preserving() {
		let key1 = HandlerKey {
			handler: HandlerId(1),
		};
		let key2 = HandlerKey {
			handler: HandlerId(2),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[cfg(test)]
mod verify_byte_identical_handler_key {
	use reifydb_codec::key::serializer::KeySerializer;

	use super::HandlerKey;
	use crate::interface::catalog::id::HandlerId;

	fn legacy_encode(key: &HandlerKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(HandlerKey::TAG as u8).extend_u64(key.handler);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for handler in [0u64, 1, 42, 0xABCD, u64::MAX] {
			let key = HandlerKey {
				handler: HandlerId(handler),
			};
			assert_eq!(legacy_encode(&key), key.encode().as_slice().to_vec(), "handler={handler:#x}");
		}
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = VariantHandler)]
pub struct VariantHandlerKey {
	pub namespace: NamespaceId,
	pub sumtype: SumTypeId,
	pub variant_tag: u8,
	pub handler: HandlerId,
}

impl VariantHandlerKey {
	pub fn new(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8, handler: HandlerId) -> Self {
		Self {
			namespace,
			sumtype,
			variant_tag,
			handler,
		}
	}

	pub fn encoded(
		namespace: impl Into<NamespaceId>,
		sumtype: impl Into<SumTypeId>,
		variant_tag: u8,
		handler: impl Into<HandlerId>,
	) -> EncodedKey {
		Self::new(namespace.into(), sumtype.into(), variant_tag, handler.into()).encode()
	}

	pub fn variant_scan(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::prefix(
			Self::TAG,
			[
				Field::UDesc(Width::U64, namespace.0 as u128),
				Field::UDesc(Width::U64, sumtype.0 as u128),
				Field::UDesc(Width::U8, variant_tag as u128),
			],
		)
	}
}

#[cfg(test)]
pub mod variant_handler_key_tests {
	use std::ops::Bound;

	use reifydb_value::value::sumtype::SumTypeId;

	use super::VariantHandlerKey;
	use crate::interface::catalog::id::{HandlerId, NamespaceId};

	#[test]
	fn test_encode_decode() {
		let key = VariantHandlerKey {
			namespace: NamespaceId(0xABCD),
			sumtype: SumTypeId(0x1234),
			variant_tag: 5,
			handler: HandlerId(0x6789),
		};
		let encoded = key.encode();
		let expected: Vec<u8> = vec![
			0xD2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xED,
			0xCB, 0xFA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x98, 0x76,
		];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = VariantHandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.sumtype, SumTypeId(0x1234));
		assert_eq!(decoded.variant_tag, 5);
		assert_eq!(decoded.handler, HandlerId(0x6789));
	}

	#[test]
	fn test_order_preserving() {
		let key1 = VariantHandlerKey {
			namespace: NamespaceId::SYSTEM,
			sumtype: SumTypeId(5),
			variant_tag: 3,
			handler: HandlerId(100),
		};
		let key2 = VariantHandlerKey {
			namespace: NamespaceId::SYSTEM,
			sumtype: SumTypeId(5),
			variant_tag: 3,
			handler: HandlerId(200),
		};
		let key3 = VariantHandlerKey {
			namespace: NamespaceId::SYSTEM,
			sumtype: SumTypeId(5),
			variant_tag: 4,
			handler: HandlerId(1),
		};
		let key4 = VariantHandlerKey {
			namespace: NamespaceId::DEFAULT,
			sumtype: SumTypeId(1),
			variant_tag: 0,
			handler: HandlerId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();
		let encoded4 = key4.encode();

		assert!(encoded4 < encoded3, "ordering not preserved");
		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}

	#[test]
	fn test_variant_scan() {
		let ns = NamespaceId::SYSTEM;
		let st = SumTypeId(10);
		let tag = 5u8;

		let range = VariantHandlerKey::variant_scan(ns, st, tag).encode();
		let start = match &range.start {
			Bound::Included(k) | Bound::Excluded(k) => k,
			Bound::Unbounded => panic!("expected bounded start"),
		};
		let end = match &range.end {
			Bound::Included(k) | Bound::Excluded(k) => k,
			Bound::Unbounded => panic!("expected bounded end"),
		};

		let key = VariantHandlerKey {
			namespace: ns,
			sumtype: st,
			variant_tag: tag,
			handler: HandlerId(42),
		};
		let encoded = key.encode();
		assert!(encoded.as_slice() >= start.as_slice());
		assert!(encoded.as_slice() <= end.as_slice());

		let other = VariantHandlerKey {
			namespace: ns,
			sumtype: st,
			variant_tag: tag + 1,
			handler: HandlerId(42),
		};
		let other_encoded = other.encode();
		assert!(other_encoded.as_slice() < start.as_slice());
	}
}

#[cfg(test)]
mod verify_byte_identical_variant_handler_key {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_value::value::sumtype::SumTypeId;

	use super::VariantHandlerKey;
	use crate::interface::catalog::id::{HandlerId, NamespaceId};

	fn legacy_encode(key: &VariantHandlerKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VariantHandlerKey::TAG as u8)
			.extend_u64(key.namespace)
			.extend_u64(key.sumtype)
			.extend_u8(key.variant_tag)
			.extend_u64(key.handler);
		serializer.to_encoded_key().as_slice().to_vec()
	}

	#[test]
	fn matches_legacy_byte_layout() {
		for (namespace, sumtype, variant_tag, handler) in [
			(0u64, 0u64, 0u8, 0u64),
			(1, 2, 3, 4),
			(0xABCD, 0x1234, 5, 0x6789),
			(u64::MAX, u64::MAX, u8::MAX, u64::MAX),
		] {
			let key = VariantHandlerKey {
				namespace: NamespaceId(namespace),
				sumtype: SumTypeId(sumtype),
				variant_tag,
				handler: HandlerId(handler),
			};
			assert_eq!(
				legacy_encode(&key),
				key.encode().as_slice().to_vec(),
				"namespace={namespace:#x} sumtype={sumtype:#x} variant_tag={variant_tag:#x} handler={handler:#x}"
			);
		}
	}
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BindingKey {
	pub binding: BindingId,
}

impl BindingKey {
	pub const TAG: KeyTag = KeyTag::Binding;

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(BindingKey::TAG as u8).extend_u64(self.binding);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyTag = de.read_u8().ok()?.try_into().ok()?;
		if kind != BindingKey::TAG {
			return None;
		}

		let binding = de.read_u64().ok()?;

		Some(Self {
			binding: BindingId(binding),
		})
	}
}

impl BindingKey {
	pub fn new(binding: impl Into<BindingId>) -> Self {
		Self {
			binding: binding.into(),
		}
	}

	pub fn encoded(binding: impl Into<BindingId>) -> EncodedKey {
		Self::new(binding).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
pub mod binding_key_tests {
	use super::BindingKey;
	use crate::interface::catalog::id::BindingId;

	#[test]
	fn test_encode_decode() {
		let key = BindingKey {
			binding: BindingId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = BindingKey::decode(&encoded).unwrap();
		assert_eq!(decoded.binding, 0xABCD);
	}
}

#[derive(Debug, Clone, PartialEq, KeyCodec, Hash)]
#[key(tag = PrimaryKey)]
pub struct PrimaryKeyKey {
	pub primary_key: PrimaryKeyId,
}

impl PrimaryKeyKey {
	pub fn new(primary_key: impl Into<PrimaryKeyId>) -> Self {
		Self {
			primary_key: primary_key.into(),
		}
	}

	pub fn encoded(primary_key: impl Into<PrimaryKeyId>) -> EncodedKey {
		Self::new(primary_key).encode()
	}

	pub fn full_scan() -> TaggedKeyBoundRange {
		TaggedKeyBoundRange::kind(Self::TAG)
	}
}

#[cfg(test)]
mod primary_key_key_tests {
	use super::PrimaryKeyKey;
	use crate::interface::catalog::id::PrimaryKeyId;

	#[test]
	fn test_encode_decode() {
		let key = PrimaryKeyKey {
			primary_key: PrimaryKeyId(0xABCD),
		};
		let encoded = key.encode();
		let decoded = PrimaryKeyKey::decode(&encoded).unwrap();
		assert_eq!(decoded.primary_key, PrimaryKeyId(0xABCD));
	}
}

impl KeyFields for DictionaryEntryIndexKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.dictionary.0 as u128), Field::UDesc(Width::Varint, self.id)]
	}
}

impl KeyFields for IndexEntryKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![
			Field::UAsc(Width::U8, self.object.type_tag() as u128),
			Field::UDesc(Width::U64, self.object.as_u64() as u128),
			Field::UAsc(Width::U8, index_tag(&self.index) as u128),
			Field::UDesc(Width::U64, self.index.as_u64() as u128),
			Field::RawAsc(RawEncoding::Verbatim, Cow::Borrowed(self.key.as_slice())),
		]
	}
}

impl KeyFields for BindingKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.binding.0 as u128)]
	}
}

impl KeyFields for SinkKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.sink.0 as u128)]
	}
}

impl KeyFields for SourceKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.source.0 as u128)]
	}
}

impl KeyFields for ViewKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.view.0 as u128)]
	}
}

impl KeyFields for SumTypeKey {
	fn fields(&self) -> SmallVec<[Field<'_>; 6]> {
		smallvec![Field::UDesc(Width::U64, self.sumtype.0 as u128)]
	}
}
