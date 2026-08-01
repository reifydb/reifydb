// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	ByteSink, decode_u64_varint, deserializer::KeyDeserializer, encode_u64_varint, encoded::EncodedKeyBuilder,
	serializer::KeySerializer,
};
use reifydb_value::Result;

use crate::{
	interface::catalog::{
		id::{IndexId, PrimaryKeyId},
		object::ObjectId,
	},
	return_internal_error,
};

pub fn serialize_object_id<B: ByteSink>(object: &ObjectId, out: &mut B) {
	out.push(object.type_tag());
	encode_u64_varint(object.as_u64(), out);
}

pub fn deserialize_object_id(input: &mut &[u8]) -> Result<ObjectId> {
	if input.is_empty() {
		return_internal_error!("Invalid ObjectId encoding: empty input");
	}

	let type_byte = input[0];
	*input = &input[1..];
	let id = decode_u64_varint(input)?;

	match ObjectId::from_type_tag(type_byte, id) {
		Some(object) => Ok(object),
		None => return_internal_error!("Invalid ObjectId type byte: 0x{:02x}.", type_byte),
	}
}

pub fn serialize_index_id<B: ByteSink>(index: &IndexId, out: &mut B) {
	match index {
		IndexId::Primary(PrimaryKeyId(id)) => {
			out.push(0x01);
			encode_u64_varint(*id, out);
		}
	}
}

pub fn deserialize_index_id(input: &mut &[u8]) -> Result<IndexId> {
	if input.is_empty() {
		return_internal_error!("Invalid IndexId encoding: empty input");
	}

	let type_byte = input[0];
	*input = &input[1..];
	let id = decode_u64_varint(input)?;

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
pub mod tests {
	use reifydb_codec::key::serialize;

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
		key1.extend(&serialize(&100u64));

		let mut key2 = row_key_10_100.clone();
		key2.extend(&bytes10);
		key2.extend(&serialize(&200u64));

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

		assert_eq!(bytes10.len(), 2, "IndexId(10) should be 2 bytes");
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

		assert_eq!(object_bytes.len(), 2, "ObjectId(42) should be 2 bytes");
		assert_eq!(index_bytes.len(), 2, "IndexId(7) should be 2 bytes");

		assert_eq!(object_bytes[0], 0x01, "Table object should have type byte 0x01");
		assert_eq!(index_bytes[0], 0x01, "Primary index should have type byte 0x01");

		let total_prefix_size = 1 + 1 + object_bytes.len() + index_bytes.len();
		assert_eq!(total_prefix_size, 6, "Total IndexEntryKey prefix should be 6 bytes");
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

		// IndexId Primary uses 1 byte prefix + u64 varint
		assert_eq!(result.len(), 5);
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

		// ObjectId Table uses 1 byte prefix + u64 varint
		assert_eq!(result.len(), 6);
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
