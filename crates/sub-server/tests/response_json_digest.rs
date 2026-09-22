// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_sub_server::response::{resolve_change_json, resolve_response_json};
use reifydb_value::{
	util::hex::encode,
	value::{
		Value,
		container::digest::DigestContainer,
		digest::Digest,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
		value_type::ValueType,
	},
};
use serde_json::{Value as JsonValue, from_str};

const ACCURACY: u32 = 10_000;

fn float_digest(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, ACCURACY).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

fn hex_of(digest: &Digest) -> JsonValue {
	JsonValue::String(format!("0x{}", encode(&digest.encode())))
}

fn digest_frame(rows: Vec<Option<Digest>>) -> Frame {
	let defined: Vec<bool> = rows.iter().map(Option::is_some).collect();
	let mut container = DigestContainer::with_capacity(rows.len());
	for row in rows {
		match row {
			Some(digest) => container.push(Box::new(digest)),
			None => container.push_default(),
		}
	}
	let data = FrameColumnData::Digest {
		container,
		inner: ValueType::Float8,
		accuracy: ACCURACY,
	};
	Frame::new(vec![FrameColumn {
		name: "d".to_string(),
		data: FrameColumnData::Option {
			inner: Box::new(data),
			bitvec: BooleanBuffer::from(defined),
		},
	}])
}

#[test]
fn a_json_response_writes_a_digest_cell_as_the_hex_of_its_bytes() {
	// The envelope types say Digest, so a client can decode the cell only if it holds the canonical bytes.
	let first = float_digest(&[1.0, 2.0, 100.0]);
	let second = float_digest(&[7.0]);
	let expected = [hex_of(&first), JsonValue::Null, hex_of(&second)];

	let body =
		resolve_response_json(vec![digest_frame(vec![Some(first), None, Some(second)])], false).unwrap().body;
	let parsed: JsonValue = from_str(&body).unwrap();

	let cells: Vec<JsonValue> = parsed[0]["rows"].as_array().unwrap().iter().map(|row| row["d"].clone()).collect();
	assert_eq!(cells, expected);
	assert_eq!(parsed[0]["types"]["d"]["underlying"]["id"], "Digest");
}

#[test]
fn a_json_change_writes_a_digest_cell_as_the_hex_of_its_bytes() {
	// Subscription changes over json share the row conversion, so a live digest must arrive as bytes too.
	let digest = float_digest(&[3.0, 30.0]);
	let expected = hex_of(&digest);

	let body = resolve_change_json(vec![digest_frame(vec![Some(digest)])]).unwrap().body;
	let parsed: JsonValue = from_str(&body).unwrap();

	assert_eq!(parsed[0]["rows"][0]["d"], expected);
}
