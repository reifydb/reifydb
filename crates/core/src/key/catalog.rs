// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	ByteSink, decode_u64_from,
	deserializer::KeyDeserializer,
	encode_u64,
	encoded::{EncodedKey, EncodedKeyBuilder, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_value::{
	Result,
	value::{dictionary::DictionaryId, sumtype::SumTypeId},
};

use super::{EncodableKey, EncodableKeyRange, KeyKind, typed::key::Key};
use crate::{
	interface::catalog::{
		id::{
			BindingId, ColumnId, ColumnPropertyId, HandlerId, IndexId, NamespaceId, PrimaryKeyId,
			RelationshipId, SinkId, SourceId, TableId, ViewId,
		},
		object::ObjectId,
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Dictionary)]
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
		Key::encode(&Self::new(dictionary.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::dictionary_start()), Some(Self::dictionary_end()))
	}

	fn dictionary_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<DictionaryKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn dictionary_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<DictionaryKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[derive(Debug, Clone, PartialEq)]
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
		EncodableKey::encode(&Self::new(dictionary.into(), hash))
	}

	pub fn full_scan(dictionary: DictionaryId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::entry_start(dictionary)), Some(Self::entry_end(dictionary)))
	}

	fn entry_start(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<Self as EncodableKey>::KIND as u8).extend_u64(dictionary);
		serializer.to_encoded_key()
	}

	fn entry_end(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<Self as EncodableKey>::KIND as u8).extend_u64(*dictionary - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for DictionaryEntryKey {
	const KIND: KeyKind = KeyKind::DictionaryEntry;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.dictionary).extend_bytes(self.hash);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let dictionary = de.read_u64().ok()?;
		let hash_bytes = de.read_raw(16).ok()?;
		let mut hash = [0u8; 16];
		hash.copy_from_slice(hash_bytes);

		Some(Self {
			dictionary: DictionaryId(dictionary),
			hash,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
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
		EncodableKey::encode(&Self::new(dictionary.into(), id))
	}

	pub fn full_scan(dictionary: DictionaryId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::index_start(dictionary)), Some(Self::index_end(dictionary)))
	}

	fn index_start(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<Self as EncodableKey>::KIND as u8).extend_u64(dictionary);
		serializer.to_encoded_key()
	}

	fn index_end(dictionary: DictionaryId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<Self as EncodableKey>::KIND as u8).extend_u64(*dictionary - 1);
		serializer.to_encoded_key()
	}
}

impl EncodableKey for DictionaryEntryIndexKey {
	const KIND: KeyKind = KeyKind::DictionaryEntryIndex;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.dictionary).extend_u128_varint(self.id);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
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

impl EncodableKeyRange for DictionaryEntryIndexKeyRange {
	const KIND: KeyKind = KeyKind::DictionaryEntryIndex;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(25);
		serializer.extend_u8(Self::KIND as u8).extend_u64(self.dictionary);
		if let Some(id) = self.start_id {
			serializer.extend_u128_varint(id);
		}
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		if let Some(id) = self.end_id {
			let mut serializer = KeySerializer::with_capacity(25);
			serializer.extend_u8(Self::KIND as u8).extend_u64(self.dictionary).extend_u128_varint(id - 1);
			Some(serializer.to_encoded_key())
		} else {
			let mut serializer = KeySerializer::with_capacity(9);
			serializer.extend_u8(Self::KIND as u8).extend_u64(*self.dictionary - 1);
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
		let encoded = Key::encode(&key);
		let decoded = <DictionaryKey as Key>::decode(&encoded).unwrap();
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
		let encoded = EncodableKey::encode(&key);
		let decoded = <DictionaryEntryKey as EncodableKey>::decode(&encoded).unwrap();
		assert_eq!(decoded.dictionary, key.dictionary);
		assert_eq!(decoded.hash, key.hash);
	}

	#[test]
	fn test_dictionary_entry_index_key_encode_decode() {
		let key = DictionaryEntryIndexKey {
			dictionary: DictionaryId(99),
			id: 12345,
		};
		let encoded = EncodableKey::encode(&key);
		let decoded = <DictionaryEntryIndexKey as EncodableKey>::decode(&encoded).unwrap();
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
		let range = DictionaryEntryKey::full_scan(DictionaryId(42));
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}

	#[test]
	fn test_dictionary_entry_index_key_full_scan() {
		let range = DictionaryEntryIndexKey::full_scan(DictionaryId(42));
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Index)]
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

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;

		Some(ObjectIndexKeyRange {
			object,
		})
	}
}

impl EncodableKeyRange for ObjectIndexKeyRange {
	const KIND: KeyKind = KeyKind::Index;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object.prev());
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
	pub fn encoded(object: impl Into<ObjectId>, index: impl Into<IndexId>) -> EncodedKey {
		Key::encode(&Self {
			object: object.into(),
			index: index.into(),
		})
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> EncodedKeyRange {
		let object = object.into();
		EncodedKeyRange::start_end(Some(Self::object_start(object)), Some(Self::object_end(object)))
	}

	pub fn object_start(object: impl Into<ObjectId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(<IndexKey as Key>::KIND as u8).extend_object_id(object);
		serializer.to_encoded_key()
	}

	pub fn object_end(object: impl Into<ObjectId>) -> EncodedKey {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(10);
		serializer.extend_u8(<IndexKey as Key>::KIND as u8).extend_object_id(object.prev());
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod index_key_tests {
	use super::IndexKey;
	use crate::{
		interface::catalog::{id::IndexId, object::ObjectId},
		key::typed::key::Key,
	};

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

#[derive(Debug, Clone, PartialEq)]
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

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
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

impl EncodableKeyRange for IndexEntryKeyRange {
	const KIND: KeyKind = KeyKind::IndexEntry;

	fn start(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object).extend_index_id(self.index);
		Some(serializer.to_encoded_key())
	}

	fn end(&self) -> Option<EncodedKey> {
		let mut serializer = KeySerializer::with_capacity(19);
		serializer.extend_u8(Self::KIND as u8).extend_object_id(self.object).extend_index_id(self.index.prev());
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

impl EncodableKey for IndexEntryKey {
	const KIND: KeyKind = KeyKind::IndexEntry;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.key.len());
		serializer
			.extend_u8(Self::KIND as u8)
			.extend_object_id(self.object)
			.extend_index_id(self.index)
			.extend_raw(self.key.as_slice());
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let object = de.read_object_id().ok()?;
		let index = de.read_index_id().ok()?;

		let remaining = de.remaining();
		if remaining > 0 {
			let remaining_bytes = de.read_raw(remaining).ok()?;
			let index_key = EncodedIndexKey::new(remaining_bytes);
			Some(Self {
				object,
				index,
				key: index_key,
			})
		} else {
			None
		}
	}
}

impl IndexEntryKey {
	pub fn index_range(object: impl Into<ObjectId>, index: IndexId) -> EncodedKeyRange {
		let range = IndexEntryKeyRange {
			object: object.into(),
			index,
		};
		EncodedKeyRange::new(Bound::Included(range.start().unwrap()), Bound::Excluded(range.end().unwrap()))
	}

	pub fn object_range(object: impl Into<ObjectId>) -> EncodedKeyRange {
		let object = object.into();
		let mut start_serializer = KeySerializer::with_capacity(10);
		start_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_object_id(object);

		let next_object = object.next();
		let mut end_serializer = KeySerializer::with_capacity(10);
		end_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_object_id(next_object);

		EncodedKeyRange {
			start: Bound::Included(start_serializer.to_encoded_key()),
			end: Bound::Excluded(end_serializer.to_encoded_key()),
		}
	}

	pub fn key_prefix_range(object: impl Into<ObjectId>, index: IndexId, key_prefix: &[u8]) -> EncodedKeyRange {
		let object = object.into();
		let mut serializer = KeySerializer::with_capacity(20 + key_prefix.len());
		serializer
			.extend_u8(KeyKind::IndexEntry as u8)
			.extend_object_id(object)
			.extend_index_id(index)
			.extend_raw(key_prefix);
		EncodedKeyRange::prefix(serializer.to_encoded_key().as_slice())
	}

	pub fn key_range(
		object: impl Into<ObjectId>,
		index: IndexId,
		index_range: EncodedIndexKeyRange,
	) -> EncodedKeyRange {
		let object = object.into();

		let mut prefix_serializer = KeySerializer::with_capacity(19);
		prefix_serializer.extend_u8(KeyKind::IndexEntry as u8).extend_object_id(object).extend_index_id(index);
		let prefix = prefix_serializer.to_encoded_key().to_vec();

		let start = match index_range.start {
			Bound::Included(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Included(EncodedKey::new(bytes))
			}
			Bound::Excluded(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Excluded(EncodedKey::new(bytes))
			}
			Bound::Unbounded => Bound::Included(EncodedKey::new(prefix.clone())),
		};

		let end = match index_range.end {
			Bound::Included(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Included(EncodedKey::new(bytes))
			}
			Bound::Excluded(key) => {
				let mut bytes = prefix.clone();
				bytes.extend_from_slice(key.as_slice());
				Bound::Excluded(EncodedKey::new(bytes))
			}
			Bound::Unbounded => {
				let mut serializer = KeySerializer::with_capacity(19);
				serializer
					.extend_u8(KeyKind::IndexEntry as u8)
					.extend_object_id(object)
					.extend_index_id(index.prev());
				Bound::Excluded(serializer.to_encoded_key())
			}
		};

		EncodedKeyRange {
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
		let range = IndexEntryKey::index_range(ObjectId::table(10), IndexId::primary(5));

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
		let range = IndexEntryKey::key_prefix_range(ObjectId::table(1), IndexId::primary(1), prefix);

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

#[derive(Debug, Clone, PartialEq)]
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
		Key::encode(&Self::new(sumtype.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SumTypeKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SumTypeKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

impl Key for SumTypeKey {
	const KIND: KeyKind = KeyKind::SumType;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<SumTypeKey as Key>::KIND as u8).extend_u64(self.sumtype);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <SumTypeKey as Key>::KIND {
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

	use super::{Key, SumTypeKey};

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

#[derive(Debug, Clone, PartialEq)]
pub struct ViewKey {
	pub view: ViewId,
}

impl Key for ViewKey {
	const KIND: KeyKind = KeyKind::View;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<ViewKey as Key>::KIND as u8).extend_u64(self.view);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <ViewKey as Key>::KIND {
			return None;
		}

		let view = de.read_u64().ok()?;

		Some(Self {
			view: ViewId(view),
		})
	}
}

impl ViewKey {
	pub fn encoded(view: impl Into<ViewId>) -> EncodedKey {
		Key::encode(&Self {
			view: view.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::view_start()), Some(Self::view_end()))
	}

	fn view_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<ViewKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn view_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<ViewKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod view_key_tests {
	use super::{Key, ViewKey};
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Table)]
pub struct TableKey {
	pub table: TableId,
}

impl TableKey {
	pub fn encoded(table: impl Into<TableId>) -> EncodedKey {
		Key::encode(&Self {
			table: table.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::table_start()), Some(Self::table_end()))
	}

	fn table_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<TableKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn table_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<TableKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod table_key_tests {
	use super::TableKey;
	use crate::{interface::catalog::id::TableId, key::typed::key::Key};

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

#[derive(Debug, Clone, PartialEq)]
pub struct SourceKey {
	pub source: SourceId,
}

impl Key for SourceKey {
	const KIND: KeyKind = KeyKind::Source;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<SourceKey as Key>::KIND as u8).extend_u64(self.source);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <SourceKey as Key>::KIND {
			return None;
		}

		let source = de.read_u64().ok()?;

		Some(Self {
			source: SourceId(source),
		})
	}
}

impl SourceKey {
	pub fn encoded(source: impl Into<SourceId>) -> EncodedKey {
		Key::encode(&Self {
			source: source.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::source_start()), Some(Self::source_end()))
	}

	fn source_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SourceKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn source_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SourceKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod source_key_tests {
	use super::{Key, SourceKey};
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

#[derive(Debug, Clone, PartialEq)]
pub struct SinkKey {
	pub sink: SinkId,
}

impl Key for SinkKey {
	const KIND: KeyKind = KeyKind::Sink;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<SinkKey as Key>::KIND as u8).extend_u64(self.sink);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <SinkKey as Key>::KIND {
			return None;
		}

		let sink = de.read_u64().ok()?;

		Some(Self {
			sink: SinkId(sink),
		})
	}
}

impl SinkKey {
	pub fn encoded(sink: impl Into<SinkId>) -> EncodedKey {
		Key::encode(&Self {
			sink: sink.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::sink_start()), Some(Self::sink_end()))
	}

	fn sink_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SinkKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn sink_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<SinkKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod sink_key_tests {
	use super::{Key, SinkKey};
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Relationship)]
pub struct RelationshipKey {
	pub relationship: RelationshipId,
}

impl RelationshipKey {
	pub fn encoded(relationship: impl Into<RelationshipId>) -> EncodedKey {
		Key::encode(&Self {
			relationship: relationship.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::relationship_start()), Some(Self::relationship_end()))
	}

	fn relationship_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RelationshipKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn relationship_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<RelationshipKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod relationship_key_tests {
	use super::{Key, RelationshipKey};
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = ColumnProperty)]
pub struct ColumnPropertyKey {
	pub column: ColumnId,
	pub property: ColumnPropertyId,
}

impl ColumnPropertyKey {
	pub fn encoded(column: impl Into<ColumnId>, property: impl Into<ColumnPropertyId>) -> EncodedKey {
		Key::encode(&Self {
			column: column.into(),
			property: property.into(),
		})
	}

	pub fn full_scan(column: ColumnId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(column)), Some(Self::link_end(column)))
	}

	fn link_start(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<ColumnPropertyKey as Key>::KIND as u8).extend_u64(column);
		serializer.to_encoded_key()
	}

	fn link_end(column: ColumnId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<ColumnPropertyKey as Key>::KIND as u8).extend_u64(*column - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod column_property_key_tests {
	use super::ColumnPropertyKey;
	use crate::{
		interface::catalog::id::{ColumnId, ColumnPropertyId},
		key::typed::key::Key,
	};

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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Handler)]
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
		Key::encode(&Self::new(handler.into()))
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<HandlerKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<HandlerKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod handler_key_tests {
	use super::{HandlerKey, Key};
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

	use super::{HandlerKey, Key};
	use crate::interface::catalog::id::HandlerId;

	fn legacy_encode(key: &HandlerKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(HandlerKey::KIND as u8).extend_u64(key.handler);
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = VariantHandler)]
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
		Key::encode(&Self::new(namespace.into(), sumtype.into(), variant_tag, handler.into()))
	}

	pub fn variant_scan(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKeyRange {
		EncodedKeyRange::start_end(
			Some(Self::variant_start(namespace, sumtype, variant_tag)),
			Some(Self::variant_end(namespace, sumtype, variant_tag)),
		)
	}

	fn variant_start(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(<VariantHandlerKey as Key>::KIND as u8)
			.extend_u64(namespace)
			.extend_u64(sumtype)
			.extend_u8(variant_tag);
		serializer.to_encoded_key()
	}

	fn variant_end(namespace: NamespaceId, sumtype: SumTypeId, variant_tag: u8) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(18);
		serializer
			.extend_u8(<VariantHandlerKey as Key>::KIND as u8)
			.extend_u64(namespace)
			.extend_u64(sumtype)
			.extend_u8(variant_tag.wrapping_sub(1));
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod variant_handler_key_tests {
	use std::ops::Bound;

	use reifydb_value::value::sumtype::SumTypeId;

	use super::{Key, VariantHandlerKey};
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

		let range = VariantHandlerKey::variant_scan(ns, st, tag);
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

	use super::{Key, VariantHandlerKey};
	use crate::interface::catalog::id::{HandlerId, NamespaceId};

	fn legacy_encode(key: &VariantHandlerKey) -> Vec<u8> {
		let mut serializer = KeySerializer::with_capacity(26);
		serializer
			.extend_u8(VariantHandlerKey::KIND as u8)
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

#[derive(Debug, Clone, PartialEq)]
pub struct BindingKey {
	pub binding: BindingId,
}

impl Key for BindingKey {
	const KIND: KeyKind = KeyKind::Binding;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(<BindingKey as Key>::KIND as u8).extend_u64(self.binding);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != <BindingKey as Key>::KIND {
			return None;
		}

		let binding = de.read_u64().ok()?;

		Some(Self {
			binding: BindingId(binding),
		})
	}
}

impl BindingKey {
	pub fn encoded(binding: impl Into<BindingId>) -> EncodedKey {
		Key::encode(&Self {
			binding: binding.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::start()), Some(Self::end()))
	}

	fn start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<BindingKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<BindingKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod binding_key_tests {
	use super::{BindingKey, Key};
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

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = PrimaryKey)]
pub struct PrimaryKeyKey {
	pub primary_key: PrimaryKeyId,
}

impl PrimaryKeyKey {
	pub fn encoded(primary_key: impl Into<PrimaryKeyId>) -> EncodedKey {
		Key::encode(&Self {
			primary_key: primary_key.into(),
		})
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::primary_key_start()), Some(Self::primary_key_end()))
	}

	fn primary_key_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<PrimaryKeyKey as Key>::KIND as u8);
		serializer.to_encoded_key()
	}

	fn primary_key_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(<PrimaryKeyKey as Key>::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod primary_key_key_tests {
	use super::{Key, PrimaryKeyKey};
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
