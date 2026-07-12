// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	ByteSink, decode_u64_varint, deserializer::KeyDeserializer, encode_u64_varint, encoded::EncodedKeyBuilder,
	serializer::KeySerializer,
};
use reifydb_value::{Result, value::dictionary::DictionaryId};

use crate::{
	interface::catalog::{
		id::{IndexId, PrimaryKeyId, RingBufferId, SegmentTreeId, SeriesId, TableId, ViewId},
		shape::ShapeId,
		vtable::VTableId,
	},
	return_internal_error,
};

pub fn serialize_shape_id<B: ByteSink>(shape: &ShapeId, out: &mut B) {
	match shape {
		ShapeId::Table(TableId(id)) => {
			out.push(0x01);
			encode_u64_varint(*id, out);
		}
		ShapeId::View(ViewId(id)) => {
			out.push(0x02);
			encode_u64_varint(*id, out);
		}
		ShapeId::TableVirtual(VTableId(id)) => {
			out.push(0x03);
			encode_u64_varint(*id, out);
		}
		ShapeId::RingBuffer(RingBufferId(id)) => {
			out.push(0x04);
			encode_u64_varint(*id, out);
		}
		ShapeId::Dictionary(DictionaryId(id)) => {
			out.push(0x06);
			encode_u64_varint(*id, out);
		}
		ShapeId::Series(SeriesId(id)) => {
			out.push(0x07);
			encode_u64_varint(*id, out);
		}
		ShapeId::SegmentTree(SegmentTreeId(id)) => {
			out.push(0x08);
			encode_u64_varint(*id, out);
		}
	}
}

pub fn deserialize_shape_id(input: &mut &[u8]) -> Result<ShapeId> {
	if input.is_empty() {
		return_internal_error!("Invalid ShapeId encoding: empty input");
	}

	let type_byte = input[0];
	*input = &input[1..];
	let id = decode_u64_varint(input)?;

	match type_byte {
		0x01 => Ok(ShapeId::Table(TableId(id))),
		0x02 => Ok(ShapeId::View(ViewId(id))),
		0x03 => Ok(ShapeId::TableVirtual(VTableId(id))),
		0x04 => Ok(ShapeId::RingBuffer(RingBufferId(id))),
		0x06 => Ok(ShapeId::Dictionary(DictionaryId(id))),
		0x07 => Ok(ShapeId::Series(SeriesId(id))),
		0x08 => Ok(ShapeId::SegmentTree(SegmentTreeId(id))),
		_ => return_internal_error!("Invalid ShapeId type byte: 0x{:02x}.", type_byte),
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
	fn extend_shape_id(&mut self, shape: impl Into<ShapeId>) -> &mut Self;
	fn extend_index_id(&mut self, index: impl Into<IndexId>) -> &mut Self;
}

impl KeySerializerCatalogExt for KeySerializer {
	fn extend_shape_id(&mut self, shape: impl Into<ShapeId>) -> &mut Self {
		let mut buf = Vec::new();
		serialize_shape_id(&shape.into(), &mut buf);
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
	fn read_shape_id(&mut self) -> Result<ShapeId>;
	fn read_index_id(&mut self) -> Result<IndexId>;
}

impl KeyDeserializerCatalogExt for KeyDeserializer<'_> {
	fn read_shape_id(&mut self) -> Result<ShapeId> {
		let mut slice = self.remaining_bytes();
		let before = slice.len();
		let shape_id = deserialize_shape_id(&mut slice)?;
		self.read_raw(before - slice.len())?;
		Ok(shape_id)
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
	fn shape_id(self, shape: impl Into<ShapeId>) -> Self;
	fn index_id(self, index: impl Into<IndexId>) -> Self;
}

impl EncodedKeyBuilderCatalogExt for EncodedKeyBuilder {
	fn shape_id(self, shape: impl Into<ShapeId>) -> Self {
		let mut buf = Vec::new();
		serialize_shape_id(&shape.into(), &mut buf);
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
		serialize_index_id as serialize_index_id_inner, serialize_shape_id as serialize_shape_id_inner, *,
	};

	fn serialize_shape_id(shape: &ShapeId) -> Vec<u8> {
		let mut out = Vec::new();
		serialize_shape_id_inner(shape, &mut out);
		out
	}

	fn serialize_index_id(index: &IndexId) -> Vec<u8> {
		let mut out = Vec::new();
		serialize_index_id_inner(index, &mut out);
		out
	}

	#[test]
	fn test_shape_id_ordering() {
		let primitive1 = ShapeId::table(1);
		let primitive2 = ShapeId::table(2);
		let primitive100 = ShapeId::table(100);
		let primitive200 = ShapeId::table(200);

		let bytes1 = serialize_shape_id(&primitive1);
		let bytes2 = serialize_shape_id(&primitive2);
		let bytes100 = serialize_shape_id(&primitive100);
		let bytes200 = serialize_shape_id(&primitive200);

		assert!(bytes2 < bytes1, "shape(2) should be < shape(1) in bytes");
		assert!(bytes200 < bytes100, "shape(200) should be < shape(100) in bytes");
		assert!(bytes100 < bytes2, "shape(100) should be < shape(2) in bytes");
	}

	#[test]
	fn test_range_boundaries() {
		let primitive10 = ShapeId::table(10);
		let primitive9 = primitive10.prev();

		let bytes10 = serialize_shape_id(&primitive10);
		let bytes9 = serialize_shape_id(&primitive9);

		assert!(bytes9 > bytes10, "shape(9) should be > shape(10) in bytes");

		let view10 = ShapeId::view(10);
		let view9 = view10.prev();

		let vbytes10 = serialize_shape_id(&view10);
		let vbytes9 = serialize_shape_id(&view9);

		assert!(vbytes9 > vbytes10, "view(9) should be > view(10) in bytes");

		let virtual10 = ShapeId::vtable(10);
		let virtual9 = virtual10.prev();

		let tvbytes10 = serialize_shape_id(&virtual10);
		let tvbytes9 = serialize_shape_id(&virtual9);

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

		assert!(key1 >= bytes10, "key1 should be >= start(primitive10)");
		assert!(key1 < end_key, "key1 should be < end(primitive9)");
		assert!(key2 >= bytes10, "key2 should be >= start(primitive10)");
		assert!(key2 < end_key, "key2 should be < end(primitive9)");
	}

	#[test]
	fn test_vtable_serialization() {
		let virtual_primitive = ShapeId::vtable(42);
		let bytes = serialize_shape_id(&virtual_primitive);
		let mut slice = &bytes[..];
		let deserialized = deserialize_shape_id(&mut slice).unwrap();
		assert_eq!(virtual_primitive, deserialized);
		assert!(slice.is_empty());

		assert_eq!(bytes[0], 0x03);

		let virtual_id = VTableId(123);
		let primitive_from_id = ShapeId::from(virtual_id);
		let bytes_from_id = serialize_shape_id(&primitive_from_id);
		let mut slice = &bytes_from_id[..];
		let deserialized_id = deserialize_shape_id(&mut slice).unwrap();
		assert_eq!(primitive_from_id, deserialized_id);
		assert!(slice.is_empty());

		let virtual1 = ShapeId::vtable(1);
		let virtual2 = ShapeId::vtable(2);
		let bytes1 = serialize_shape_id(&virtual1);
		let bytes2 = serialize_shape_id(&virtual2);

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
		let shape = ShapeId::table(42);
		let index = IndexId::primary(7);

		let primitive_bytes = serialize_shape_id(&shape);
		let index_bytes = serialize_index_id(&index);

		assert_eq!(primitive_bytes.len(), 2, "ShapeId(42) should be 2 bytes");
		assert_eq!(index_bytes.len(), 2, "IndexId(7) should be 2 bytes");

		assert_eq!(primitive_bytes[0], 0x01, "Table shape should have type byte 0x01");
		assert_eq!(index_bytes[0], 0x01, "Primary index should have type byte 0x01");

		let total_prefix_size = 1 + 1 + primitive_bytes.len() + index_bytes.len();
		assert_eq!(total_prefix_size, 6, "Total IndexEntryKey prefix should be 6 bytes");
	}
}

#[cfg(test)]
mod moved_catalog_key_tests {
	use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer};

	use super::{KeyDeserializerCatalogExt, KeySerializerCatalogExt};
	use crate::interface::catalog::{
		id::{IndexId, PrimaryKeyId, TableId},
		shape::ShapeId,
	};

	#[test]
	fn test_index_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_index_id(IndexId::Primary(PrimaryKeyId(123456789)));
		let result = serializer.finish();

		// IndexId Primary uses 1 byte prefix + u64 varint
		assert_eq!(result.len(), 5);
		assert_eq!(result[0], 0x01); // Primary variant prefix

		// Verify it's using bitwise NOT (smaller values produce larger encoded values)
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_index_id(IndexId::Primary(PrimaryKeyId(1)));
		let result2 = serializer2.finish();

		// result2 (for IndexId(1)) should be > result (for IndexId(123456789))
		// Compare from byte 1 onwards (after the variant prefix)
		assert!(result2[1..] > result[1..]);
	}

	#[test]
	fn test_object_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_shape_id(ShapeId::Table(TableId(987654321)));
		let result = serializer.finish();

		// ShapeId Table uses 1 byte prefix + u64 varint
		assert_eq!(result.len(), 6);
		assert_eq!(result[0], 0x01); // Table variant prefix

		// Verify ordering
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_shape_id(ShapeId::Table(TableId(987654322)));
		let result2 = serializer2.finish();

		// result2 (for larger ShapeId) should be < result (inverted ordering)
		// Compare from byte 1 onwards (after the variant prefix)
		assert!(result2[1..] < result[1..]);
	}

	#[test]
	fn test_read_shape_id() {
		let mut ser = KeySerializer::new();
		let primitive = ShapeId::table(42);
		ser.extend_shape_id(primitive);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_shape_id().unwrap(), primitive);
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
