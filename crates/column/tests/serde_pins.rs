// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use postcard::{from_bytes, to_stdvec};
use reifydb_column::{
	encoding::{Encoding, canonical::CanonicalEncoding},
	persist::{PersistedArray, deserialize_block, serialize_block},
	snapshot::{ColumnBlock, ColumnChunks},
};
use reifydb_core::value::column::{
	builder::ColumnBuilder,
	data::{Column, canonical::Canonical},
	factory,
};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	identity::IdentityId,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};
use uuid::Uuid;

struct Pin {
	name: &'static str,
	chunk_postcard: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "bool",
		chunk_postcard: "0001000000000001050300",
	},
	Pin {
		name: "float4",
		chunk_postcard: "0001010000000104ffff7fff000000800000c03fffff7f7f00",
	},
	Pin {
		name: "float8",
		chunk_postcard: "0001020000000204ffffffffffffefff00000000000000800000000000000240ffffffffffffef7f00",
	},
	Pin {
		name: "int1",
		chunk_postcard: "000103000000030380007f00",
	},
	Pin {
		name: "int2",
		chunk_postcard: "0001040000000403ffff0300feff0300",
	},
	Pin {
		name: "int4",
		chunk_postcard: "0001050000000503ffffffff0f00feffffff0f00",
	},
	Pin {
		name: "int8",
		chunk_postcard: "0001060000000603ffffffffffffffffff0100feffffffffffffffff0100",
	},
	Pin {
		name: "int16",
		chunk_postcard: "0001070000000703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff0300",
	},
	Pin {
		name: "uint1",
		chunk_postcard: "00010900000008030001ff00",
	},
	Pin {
		name: "uint2",
		chunk_postcard: "00010a00000009030001ffff0300",
	},
	Pin {
		name: "uint4",
		chunk_postcard: "00010b0000000a030001ffffffff0f00",
	},
	Pin {
		name: "uint8",
		chunk_postcard: "00010c0000000b030001ffffffffffffffffff0100",
	},
	Pin {
		name: "uint16",
		chunk_postcard: "00010d0000000c030001ffffffffffffffffffffffffffffffffffff0300",
	},
	Pin {
		name: "utf8",
		chunk_postcard: "0001080000000d030161000668c3a96c6c6f00",
	},
	Pin {
		name: "date",
		chunk_postcard: "00010e0000000e0300c98e03dcc30200",
	},
	Pin {
		name: "datetime",
		chunk_postcard: "00010f0000000f0200aab490cedc9bb9e73000",
	},
	Pin {
		name: "time",
		chunk_postcard: "000110000000100200ffffbb8ac9d21300",
	},
	Pin {
		name: "duration",
		chunk_postcard: "00011100000011030000000204061b3d0000",
	},
	Pin {
		name: "identity_id",
		chunk_postcard: "00011200000012021000000000000170008000000000000000100000000000027000800000000000000000",
	},
	Pin {
		name: "uuid4",
		chunk_postcard: "0001130000001301100123456789ab4cde8f0123456789abcd00",
	},
	Pin {
		name: "uuid7",
		chunk_postcard: "00011400000014021000000000000370008000000000000000100000000000047000800000000000000000",
	},
	Pin {
		name: "blob",
		chunk_postcard: "0001150000001502000300ff0700",
	},
	Pin {
		name: "decimal",
		chunk_postcard: "0001164c0600000016014c06030000a0b9a4ffffffffffffffffffffffffffffff030181b1eb9697ae90f6eabdbfb8fdf7b8e3170000",
	},
	Pin {
		name: "any",
		chunk_postcard: "00011718000000170301060e010901780001010303",
	},
	Pin {
		name: "any_typed_list",
		chunk_postcard: "00011a050000011a051702011b0206020604011b0000",
	},
	Pin {
		name: "dictionary_id",
		chunk_postcard: "0001190000001802020102ffffffff0f00",
	},
	Pin {
		name: "dictionary_id_u16",
		chunk_postcard: "000119000000180104ffffffffffffffffffffffffffffffffffff0300",
	},
	Pin {
		name: "option_int4",
		chunk_postcard: "00011705000000050302000501010503",
	},
	Pin {
		name: "option_utf8",
		chunk_postcard: "000117080000000d0201610001010102",
	},
	Pin {
		name: "none_typed_int8",
		chunk_postcard: "000117060000000602000001010002",
	},
	Pin {
		name: "digest",
		chunk_postcard: "0001171d02904e000000190201100103904e000000018c01010200012e010001010102",
	},
	Pin {
		name: "sliced_int4",
		chunk_postcard: "000105000000050304060800",
	},
	Pin {
		name: "sliced_utf8",
		chunk_postcard: "0001080000000d020262620363636300",
	},
	Pin {
		name: "sliced_bool",
		chunk_postcard: "0001000000000001290600",
	},
	Pin {
		name: "sliced_option_int4",
		chunk_postcard: "00011705000000050300060001010203",
	},
];

fn uuid7_bits(i: u128) -> Uuid {
	Uuid::from_u128(((i + 1) << 80) | (0x7 << 76) | (0x2 << 62))
}

fn sliced(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	(column.0, column.1.slice(start, end - start))
}

fn digest_column() -> (FieldRef, ArrayRef) {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in [1.0, 2.5, -4.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	let ty = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	};
	let mut builder = ColumnBuilder::with_capacity(ty, 2);
	builder.push_value(Value::Digest(Box::new(digest)));
	builder.push_none();
	builder.finish("c")
}

fn fixtures() -> Vec<(&'static str, (FieldRef, ArrayRef))> {
	vec![
		("bool", factory::bool("c", [true, false, true])),
		("float4", factory::float4("c", [f32::MIN, -0.0, 1.5, f32::MAX])),
		("float8", factory::float8("c", [f64::MIN, -0.0, 2.25, f64::MAX])),
		("int1", factory::int1("c", [i8::MIN, 0, i8::MAX])),
		("int2", factory::int2("c", [i16::MIN, 0, i16::MAX])),
		("int4", factory::int4("c", [i32::MIN, 0, i32::MAX])),
		("int8", factory::int8("c", [i64::MIN, 0, i64::MAX])),
		("int16", factory::int16("c", [i128::MIN, 0, i128::MAX])),
		("uint1", factory::uint1("c", [0, 1, u8::MAX])),
		("uint2", factory::uint2("c", [0, 1, u16::MAX])),
		("uint4", factory::uint4("c", [0, 1, u32::MAX])),
		("uint8", factory::uint8("c", [0, 1, u64::MAX])),
		("uint16", factory::uint16("c", [0, 1, u128::MAX])),
		("utf8", factory::utf8("c", ["a", "", "h\u{e9}llo"])),
		(
			"date",
			factory::date(
				"c",
				[
					Date::from_ymd(1970, 1, 1).unwrap(),
					Date::from_ymd(1900, 2, 28).unwrap(),
					Date::from_ymd(2026, 9, 22).unwrap(),
				],
			),
		),
		(
			"datetime",
			factory::datetime(
				"c",
				[DateTime::from_nanos(0), DateTime::from_nanos(1_758_500_000_123_456_789)],
			),
		),
		(
			"time",
			factory::time(
				"c",
				[
					Time::from_hms_nano(0, 0, 0, 0).unwrap(),
					Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
				],
			),
		),
		(
			"duration",
			factory::duration(
				"c",
				[
					Duration::new(0, 0, 0).unwrap(),
					Duration::new(1, 2, 3).unwrap(),
					Duration::new(-14, -30, -86_400_000_000_000).unwrap(),
				],
			),
		),
		(
			"identity_id",
			factory::identity_id("c", [IdentityId(Uuid7(uuid7_bits(0))), IdentityId(Uuid7(uuid7_bits(1)))]),
		),
		("uuid4", factory::uuid4("c", [Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd))])),
		("uuid7", factory::uuid7("c", [Uuid7(uuid7_bits(2)), Uuid7(uuid7_bits(3))])),
		("blob", factory::blob("c", [Blob::new(vec![]), Blob::new(vec![0, 255, 7])])),
		(
			"decimal",
			factory::decimal(
				"c",
				Precision::MAX,
				Scale::new(6),
				["0", "-1.5", "123456789012345678901234567890.000001"]
					.map(|s| s.parse::<Decimal>().unwrap()),
			),
		),
		("any", factory::any_optional("c", [Some(Value::Int4(7)), Some(Value::Utf8("x".to_string())), None])),
		(
			"any_typed_list",
			factory::any_typed(
				"c",
				[Value::List(vec![Value::Int4(1), Value::Int4(2)]), Value::List(vec![])],
				ValueType::List(Box::new(ValueType::Int4)),
			),
		),
		(
			"dictionary_id",
			factory::dictionary_id("c", [DictionaryEntryId::U4(1), DictionaryEntryId::U4(u32::MAX)]),
		),
		("dictionary_id_u16", factory::dictionary_id("c", [DictionaryEntryId::U16(u128::MAX)])),
		("option_int4", factory::int4_optional("c", [Some(1), None, Some(-3)])),
		("option_utf8", factory::utf8_with_bitvec("c", ["a".to_string(), String::new()], vec![true, false])),
		("none_typed_int8", factory::none_typed("c", ValueType::Int8, 2)),
		("digest", digest_column()),
		("sliced_int4", sliced(factory::int4("c", [1, 2, 3, 4, 5]), 1, 4)),
		("sliced_utf8", sliced(factory::utf8("c", ["a", "bb", "ccc", "dddd"]), 1, 3)),
		(
			"sliced_bool",
			sliced(
				factory::bool("c", [true, false, true, true, false, false, true, false, true, true]),
				3,
				9,
			),
		),
		(
			"sliced_option_int4",
			sliced(factory::int4_optional("c", [Some(1), None, Some(3), None, Some(5)]), 1, 4),
		),
	]
}

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn unhex(text: &str) -> Vec<u8> {
	(0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn chunk_hex(column: &(FieldRef, ArrayRef)) -> String {
	let canonical = Canonical::from_column(column).unwrap();
	let persisted = CanonicalEncoding::FIXED.persist(&Column::from_canonical(canonical)).unwrap();
	hex(&to_stdvec(&persisted).unwrap())
}

fn decode_chunk(pinned: &str, ty: &ValueType) -> Result<Arc<Canonical>, String> {
	let persisted: PersistedArray = from_bytes(&unhex(pinned)).map_err(|err| err.to_string())?;
	let column = CanonicalEncoding::FIXED.load(persisted, ty).map_err(|err| err.to_string())?;
	column.to_canonical().map_err(|err| err.to_string())
}

fn check_decoded(label: String, pinned: &str, expected: &(FieldRef, ArrayRef)) -> Option<String> {
	let expected = Canonical::from_column(expected).unwrap();
	match decode_chunk(pinned, &expected.view().base_type()) {
		Ok(decoded)
			if decoded.field_type() == expected.field_type() && decoded.buffer() == expected.buffer() =>
		{
			None
		}
		Ok(decoded) => Some(format!("{label}: decoded {decoded:?}, expected {expected:?}")),
		Err(err) => Some(format!("{label}: decode failed: {err}")),
	}
}

fn pin(name: &str) -> &'static Pin {
	PINS.iter().find(|pin| pin.name == name).unwrap_or_else(|| panic!("no pin for fixture {name}"))
}

fn report(mismatches: Vec<String>) {
	assert!(mismatches.is_empty(), "persisted chunk bytes drifted from the pins:\n{}", mismatches.join("\n"));
}

#[test]
fn every_fixture_has_exactly_one_pin() {
	// A variant that loses its pin would stop guarding its wire format without any test going red.
	let fixture_names: Vec<&str> = fixtures().iter().map(|(name, _)| *name).collect();
	let pin_names: Vec<&str> = PINS.iter().map(|pin| pin.name).collect();
	assert_eq!(fixture_names, pin_names);
}

#[test]
fn chunk_postcard_matches_pins() {
	// Column snapshot blocks persist each chunk with postcard, so any byte drift makes stored blocks unreadable.
	let mismatches = fixtures()
		.iter()
		.filter_map(|(name, column)| {
			let actual = chunk_hex(column);
			(PINS.iter().find(|pin| pin.name == *name).map(|pin| pin.chunk_postcard)
				!= Some(actual.as_str()))
			.then(|| format!("{name} chunk_postcard: \"{actual}\""))
		})
		.collect();
	report(mismatches);
}

#[test]
fn pinned_chunk_bytes_decode_to_the_fixture() {
	// Pinned chunk bytes must read back as the same field type and values, otherwise a reload changes the column.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, column)| {
			check_decoded(format!("{name} postcard"), pin(name).chunk_postcard, &column)
		})
		.collect();
	report(mismatches);
}

#[test]
fn untyped_any_column_round_trips_through_postcard() {
	// Skipping an absent declared type drops a field postcard cannot detect, so the reader runs past the column.
	let columns = [factory::any_optional("value", [Some(Value::Int4(1)), None]), factory::int4("after", [9, 10])];
	let canonicals: Vec<Canonical> = columns.iter().map(|column| Canonical::from_column(column).unwrap()).collect();
	let schema = Arc::new(
		columns.iter()
			.zip(&canonicals)
			.map(|((field, _), canonical)| {
				(field.name().to_string(), canonical.view().base_type(), field.is_nullable())
			})
			.collect::<Vec<_>>(),
	);
	let chunks = schema
		.iter()
		.zip(&canonicals)
		.map(|((_, ty, nullable), canonical)| {
			ColumnChunks::single(ty.clone(), *nullable, Column::from_canonical(canonical.clone()))
		})
		.collect();
	let block = ColumnBlock::new(schema, chunks);
	let decoded = deserialize_block(&serialize_block(&block).unwrap()).unwrap();
	assert_eq!(*decoded.schema, *block.schema);
	for (restored, canonical) in decoded.columns.iter().zip(&canonicals) {
		let restored = restored.chunks[0].to_canonical().unwrap();
		assert_eq!(restored.field_type(), canonical.field_type());
		assert_eq!(restored.buffer(), canonical.buffer());
	}
}
