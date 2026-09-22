// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Borrow, result::Result as StdResult};

use arrow_array::{Array, LargeBinaryArray, builder::LargeBinaryBuilder};
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::Error as DeError,
	ser::{SerializeSeq, SerializeStruct},
};
use serde_bytes::{ByteBuf, Bytes};

use crate::value::{Value, container::varlen_array, digest::Digest};

fn decode(row: &[u8]) -> Digest {
	Digest::decode(row).unwrap_or_else(|error| panic!("corrupt Digest row {row:02x?}: {error}"))
}

pub fn digest_array<B: Borrow<Digest>>(values: impl IntoIterator<Item = Option<B>>) -> LargeBinaryArray {
	let mut builder = LargeBinaryBuilder::new();
	for value in values {
		match value {
			Some(digest) => push_digest(&mut builder, digest.borrow()),
			None => push_none_slot(&mut builder),
		}
	}
	builder.finish()
}

pub fn push_digest(builder: &mut LargeBinaryBuilder, digest: &Digest) {
	builder.append_value(digest.encode());
}

pub fn push_none_slot(builder: &mut LargeBinaryBuilder) {
	builder.append_value(b"");
}

pub fn get(array: &LargeBinaryArray, index: usize) -> Option<Digest> {
	match varlen_array::get(array, index) {
		Some(row) if !row.is_empty() => Some(decode(row)),
		_ => None,
	}
}

pub fn iter(array: &LargeBinaryArray) -> impl Iterator<Item = Option<Digest>> + '_ {
	(0..array.len()).map(move |index| get(array, index))
}

pub fn is_defined(array: &LargeBinaryArray, index: usize) -> bool {
	varlen_array::get(array, index).is_some_and(|row| !row.is_empty())
}

pub fn get_value(array: &LargeBinaryArray, index: usize) -> Value {
	match get(array, index) {
		Some(digest) => Value::Digest(Box::new(digest)),
		None => Value::none(),
	}
}

pub fn as_string(array: &LargeBinaryArray, index: usize) -> String {
	get_value(array, index).to_string()
}

struct Rows<'a>(&'a LargeBinaryArray);

impl Serialize for Rows<'_> {
	fn serialize<Ser: Serializer>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
		let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
		for index in 0..self.0.len() {
			let row = self.0.value(index);
			if row.is_empty() {
				seq.serialize_element(&None::<&Bytes>)?;
			} else {
				seq.serialize_element(&Some(Bytes::new(row)))?;
			}
		}
		seq.end()
	}
}

pub fn serialize<Ser: Serializer>(array: &LargeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	let mut state = serializer.serialize_struct("Helper", 1)?;
	state.serialize_field("data", &Rows(array))?;
	state.end()
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	#[derive(Deserialize)]
	struct Helper {
		data: Vec<Option<ByteBuf>>,
	}
	let rows = Helper::deserialize(deserializer)?.data;
	let mut builder =
		LargeBinaryBuilder::with_capacity(rows.len(), rows.iter().flatten().map(|row| row.len()).sum());
	for row in rows {
		match row {
			Some(bytes) => push_digest(&mut builder, &Digest::decode(&bytes).map_err(DeError::custom)?),
			None => push_none_slot(&mut builder),
		}
	}
	Ok(builder.finish())
}

#[cfg(test)]
mod tests {
	use postcard::{from_bytes, to_allocvec};
	use serde_json::{from_str, to_string};

	use super::*;
	use crate::value::digest::tests::built;

	#[derive(Serialize, Deserialize)]
	struct DigestColumn(#[serde(serialize_with = "serialize", deserialize_with = "deserialize")] LargeBinaryArray);

	#[test]
	fn an_empty_row_is_the_none_slot() {
		// Aggregates skip undefined digests; an empty row read as a digest would crash or count twice.
		let digest = built(10_000, &[1.0, 2.5, -4.0]);
		let array = digest_array([Some(&digest), None]);
		assert!(array.nulls().is_none());
		assert_eq!(array.value(1), b"");
		assert!(is_defined(&array, 0));
		assert!(!is_defined(&array, 1));
		assert!(!is_defined(&array, 2));
		assert_eq!(get(&array, 0), Some(digest.clone()));
		assert_eq!(get(&array, 1), None);
		assert_eq!(get_value(&array, 0), Value::Digest(Box::new(digest)));
		assert_eq!(get_value(&array, 1), Value::none());
		assert_eq!(as_string(&array, 1), Value::none().to_string());
		assert_eq!(iter(&array).filter(Option::is_some).count(), 1);
	}

	#[test]
	fn reorder_past_the_end_gives_the_none_slot() {
		// Out of range rows must be the none slot, never a zero digest.
		let array = varlen_array::reorder(&digest_array([Some(built(10_000, &[1.0]))]), &[4, 0]);
		assert!(!is_defined(&array, 0));
		assert!(is_defined(&array, 1));
	}

	#[test]
	fn deserialize_re_encodes_so_rows_stay_the_canonical_bytes() {
		// Rows must equal the encoding, or byte equality and the wire copy stop meaning digest equality.
		let digest = built(10_000, &[1.0, 2.5, -4.0, 1e9]);
		let array = digest_array([None, Some(&digest)]);
		let bytes = to_allocvec(&DigestColumn(array.clone())).unwrap();
		let back: DigestColumn = from_bytes(&bytes).unwrap();
		assert!(varlen_array::equals(&back.0, &array));
		assert_eq!(back.0.value(1), digest.encode().as_slice());
		let json: DigestColumn = from_str(&to_string(&DigestColumn(array.clone())).unwrap()).unwrap();
		assert!(varlen_array::equals(&json.0, &array));
	}

	#[test]
	fn deserialize_rejects_bytes_that_are_not_a_digest() {
		// Storing an invalid row would move the failure from load time to a later panic on read.
		assert!(from_str::<DigestColumn>(r#"{"data":[[9,9,9]]}"#).is_err());
		assert!(from_str::<DigestColumn>(r#"{"data":[[]]}"#).is_err());
	}

	#[test]
	fn serde_round_trip_keeps_the_none_slot() {
		// A round trip must give back exactly the digest and the none slot, never a zero digest.
		let digest = built(10_000, &[1.0, 2.5, -4.0]);
		let array = digest_array([Some(&digest), None]);
		let back: DigestColumn = from_bytes(&to_allocvec(&DigestColumn(array.clone())).unwrap()).unwrap();
		assert_eq!(iter(&back.0).collect::<Vec<_>>(), vec![Some(digest.clone()), None]);
		let json: DigestColumn = from_str(&to_string(&DigestColumn(array)).unwrap()).unwrap();
		assert_eq!(iter(&json.0).collect::<Vec<_>>(), vec![Some(digest), None]);
	}
}
