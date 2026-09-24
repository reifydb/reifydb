// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Debug, Write as _};

use num_bigint::BigInt;
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns};
use reifydb_value::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	digest::Digest,
	duration::Duration,
	frame::{data::FrameColumnData, frame::Frame},
	identity::IdentityId,
	int::Int,
	partition::Partition,
	row_number::RowNumber,
	system_columns::SystemColumns,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

struct Pin {
	name: &'static str,
	column_postcard: &'static str,
	column_json: &'static str,
	frame_postcard: &'static str,
	frame_json: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "bool",
		column_postcard: "00010503",
		column_json: "{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "00010503",
		frame_json: "{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "float4",
		column_postcard: "0104ffff7fff000000800000c03fffff7f7f",
		column_json: "{\"Float4\":{\"data\":[-3.4028235e+38,-0.0,1.5,3.4028235e+38]}}",
		frame_postcard: "0104ffff7fff000000800000c03fffff7f7f",
		frame_json: "{\"Float4\":{\"data\":[-3.4028235e+38,-0.0,1.5,3.4028235e+38]}}",
	},
	Pin {
		name: "float8",
		column_postcard: "0204ffffffffffffefff00000000000000800000000000000240ffffffffffffef7f",
		column_json: "{\"Float8\":{\"data\":[-1.7976931348623157e+308,-0.0,2.25,1.7976931348623157e+308]}}",
		frame_postcard: "0204ffffffffffffefff00000000000000800000000000000240ffffffffffffef7f",
		frame_json: "{\"Float8\":{\"data\":[-1.7976931348623157e+308,-0.0,2.25,1.7976931348623157e+308]}}",
	},
	Pin {
		name: "int1",
		column_postcard: "030380007f",
		column_json: "{\"Int1\":{\"data\":[-128,0,127]}}",
		frame_postcard: "030380007f",
		frame_json: "{\"Int1\":{\"data\":[-128,0,127]}}",
	},
	Pin {
		name: "int2",
		column_postcard: "0403ffff0300feff03",
		column_json: "{\"Int2\":{\"data\":[-32768,0,32767]}}",
		frame_postcard: "0403ffff0300feff03",
		frame_json: "{\"Int2\":{\"data\":[-32768,0,32767]}}",
	},
	Pin {
		name: "int4",
		column_postcard: "0503ffffffff0f00feffffff0f",
		column_json: "{\"Int4\":{\"data\":[-2147483648,0,2147483647]}}",
		frame_postcard: "0503ffffffff0f00feffffff0f",
		frame_json: "{\"Int4\":{\"data\":[-2147483648,0,2147483647]}}",
	},
	Pin {
		name: "int8",
		column_postcard: "0603ffffffffffffffffff0100feffffffffffffffff01",
		column_json: "{\"Int8\":{\"data\":[-9223372036854775808,0,9223372036854775807]}}",
		frame_postcard: "0603ffffffffffffffffff0100feffffffffffffffff01",
		frame_json: "{\"Int8\":{\"data\":[-9223372036854775808,0,9223372036854775807]}}",
	},
	Pin {
		name: "int16",
		column_postcard: "0703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff03",
		column_json: "{\"Int16\":{\"data\":[-170141183460469231731687303715884105728,0,170141183460469231731687303715884105727]}}",
		frame_postcard: "0703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff03",
		frame_json: "{\"Int16\":{\"data\":[-170141183460469231731687303715884105728,0,170141183460469231731687303715884105727]}}",
	},
	Pin {
		name: "uint1",
		column_postcard: "08030001ff",
		column_json: "{\"Uint1\":{\"data\":[0,1,255]}}",
		frame_postcard: "08030001ff",
		frame_json: "{\"Uint1\":{\"data\":[0,1,255]}}",
	},
	Pin {
		name: "uint2",
		column_postcard: "09030001ffff03",
		column_json: "{\"Uint2\":{\"data\":[0,1,65535]}}",
		frame_postcard: "09030001ffff03",
		frame_json: "{\"Uint2\":{\"data\":[0,1,65535]}}",
	},
	Pin {
		name: "uint4",
		column_postcard: "0a030001ffffffff0f",
		column_json: "{\"Uint4\":{\"data\":[0,1,4294967295]}}",
		frame_postcard: "0a030001ffffffff0f",
		frame_json: "{\"Uint4\":{\"data\":[0,1,4294967295]}}",
	},
	Pin {
		name: "uint8",
		column_postcard: "0b030001ffffffffffffffffff01",
		column_json: "{\"Uint8\":{\"data\":[0,1,18446744073709551615]}}",
		frame_postcard: "0b030001ffffffffffffffffff01",
		frame_json: "{\"Uint8\":{\"data\":[0,1,18446744073709551615]}}",
	},
	Pin {
		name: "uint16",
		column_postcard: "0c030001ffffffffffffffffffffffffffffffffffff03",
		column_json: "{\"Uint16\":{\"data\":[0,1,340282366920938463463374607431768211455]}}",
		frame_postcard: "0c030001ffffffffffffffffffffffffffffffffffff03",
		frame_json: "{\"Uint16\":{\"data\":[0,1,340282366920938463463374607431768211455]}}",
	},
	Pin {
		name: "utf8",
		column_postcard: "0d030161000668c3a96c6c6fffffffff0f",
		column_json: "{\"Utf8\":{\"container\":[[97],[],[104,195,169,108,108,111]],\"max_bytes\":4294967295}}",
		frame_postcard: "0d030161000668c3a96c6c6f",
		frame_json: "{\"Utf8\":[[97],[],[104,195,169,108,108,111]]}",
	},
	Pin {
		name: "date",
		column_postcard: "0e0300c98e03dcc302",
		column_json: "{\"Date\":{\"data\":[0,-25509,20718]}}",
		frame_postcard: "0e0300c98e03dcc302",
		frame_json: "{\"Date\":{\"data\":[0,-25509,20718]}}",
	},
	Pin {
		name: "datetime",
		column_postcard: "0f0200959a88a7eecddcb318",
		column_json: "{\"DateTime\":{\"data\":[0,1758500000123456789]}}",
		frame_postcard: "0f0200959a88a7eecddcb318",
		frame_json: "{\"DateTime\":{\"data\":[0,1758500000123456789]}}",
	},
	Pin {
		name: "time",
		column_postcard: "100200ffffbb8ac9d213",
		column_json: "{\"Time\":{\"data\":[0,86399999999999]}}",
		frame_postcard: "100200ffffbb8ac9d213",
		frame_json: "{\"Time\":{\"data\":[0,86399999999999]}}",
	},
	Pin {
		name: "duration",
		column_postcard: "11030000000204061b3d00",
		column_json: "{\"Duration\":{\"data\":[{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":1,\"days\":2,\"nanos\":3},{\"months\":-14,\"days\":-31,\"nanos\":0}]}}",
		frame_postcard: "11030000000204061b3d00",
		frame_json: "{\"Duration\":{\"data\":[{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":1,\"days\":2,\"nanos\":3},{\"months\":-14,\"days\":-31,\"nanos\":0}]}}",
	},
	Pin {
		name: "identity_id",
		column_postcard: "120210000000000001700080000000000000001000000000000270008000000000000000",
		column_json: "{\"IdentityId\":{\"data\":[\"00000000-0001-7000-8000-000000000000\",\"00000000-0002-7000-8000-000000000000\"]}}",
		frame_postcard: "120210000000000001700080000000000000001000000000000270008000000000000000",
		frame_json: "{\"IdentityId\":{\"data\":[\"00000000-0001-7000-8000-000000000000\",\"00000000-0002-7000-8000-000000000000\"]}}",
	},
	Pin {
		name: "uuid4",
		column_postcard: "1301100123456789ab4cde8f0123456789abcd",
		column_json: "{\"Uuid4\":{\"data\":[\"01234567-89ab-4cde-8f01-23456789abcd\"]}}",
		frame_postcard: "1301100123456789ab4cde8f0123456789abcd",
		frame_json: "{\"Uuid4\":{\"data\":[\"01234567-89ab-4cde-8f01-23456789abcd\"]}}",
	},
	Pin {
		name: "uuid7",
		column_postcard: "140210000000000003700080000000000000001000000000000470008000000000000000",
		column_json: "{\"Uuid7\":{\"data\":[\"00000000-0003-7000-8000-000000000000\",\"00000000-0004-7000-8000-000000000000\"]}}",
		frame_postcard: "140210000000000003700080000000000000001000000000000470008000000000000000",
		frame_json: "{\"Uuid7\":{\"data\":[\"00000000-0003-7000-8000-000000000000\",\"00000000-0004-7000-8000-000000000000\"]}}",
	},
	Pin {
		name: "blob",
		column_postcard: "1502000300ff07ffffffff0f",
		column_json: "{\"Blob\":{\"container\":[[],[0,255,7]],\"max_bytes\":4294967295}}",
		frame_postcard: "1502000300ff07",
		frame_json: "{\"Blob\":[[],[0,255,7]]}",
	},
	Pin {
		name: "int",
		column_postcard: "16030000ff040000008080808008ff05cb8989b20a95cad5fe0be292c0d905f9979b8d01ac1cffffffff0f",
		column_json: "{\"Int\":{\"container\":{\"data\":[[0,[]],[-1,[0,0,0,2147483648]],[-1,[2789360843,3218433301,1529874786,296143865,3628]]]},\"max_bytes\":4294967295}}",
		frame_postcard: "16030000ff040000008080808008ff05cb8989b20a95cad5fe0be292c0d905f9979b8d01ac1c",
		frame_json: "{\"Int\":{\"data\":[[0,[]],[-1,[0,0,0,2147483648]],[-1,[2789360843,3218433301,1529874786,296143865,3628]]]}}",
	},
	Pin {
		name: "uint",
		column_postcard: "170300000104ffffffff0fffffffff0fffffffff0fffffffff0f0105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db11ffffffff0f",
		column_json: "{\"Uint\":{\"container\":{\"data\":[[0,[]],[1,[4294967295,4294967295,4294967295,4294967295]],[1,[1505606452,1095215850,2286885549,2228032345,290245]]]},\"max_bytes\":4294967295}}",
		frame_postcard: "170300000104ffffffff0fffffffff0fffffffff0fffffffff0f0105b4f6f6cd05ead59e8a04adb5bcc208d9a6b4a608c5db11",
		frame_json: "{\"Uint\":{\"data\":[[0,[]],[1,[4294967295,4294967295,4294967295,4294967295]],[1,[1505606452,1095215850,2286885549,2228032345,290245]]]}}",
	},
	Pin {
		name: "decimal",
		column_postcard: "180303304530062d3135452d3127313233343536373839303132333435363738393031323334353637383930303030303031452d36ff00",
		column_json: "{\"Decimal\":{\"container\":{\"data\":[\"0E0\",\"-15E-1\",\"123456789012345678901234567890000001E-6\"]},\"precision\":255,\"scale\":0}}",
		frame_postcard: "180303304530062d3135452d3127313233343536373839303132333435363738393031323334353637383930303030303031452d36",
		frame_json: "{\"Decimal\":{\"data\":[\"0E0\",\"-15E-1\",\"123456789012345678901234567890000001E-6\"]}}",
	},
	Pin {
		name: "any",
		column_postcard: "1903060e090178001a00",
		column_json: "{\"Any\":{\"data\":[{\"Int4\":7},{\"Utf8\":\"x\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}}",
		frame_postcard: "1903060e090178001a00",
		frame_json: "{\"Any\":{\"data\":[{\"Int4\":7},{\"Utf8\":\"x\"},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}}",
	},
	Pin {
		name: "any_typed_list",
		column_postcard: "19021d02060206041d00011c05",
		column_json: "{\"Any\":{\"data\":[{\"List\":[{\"Int4\":1},{\"Int4\":2}]},{\"List\":[]}],\"declared_type\":{\"List\":\"Int4\"}}}",
		frame_postcard: "19021d02060206041d00011c05",
		frame_json: "{\"Any\":{\"data\":[{\"List\":[{\"Int4\":1},{\"Int4\":2}]},{\"List\":[]}],\"declared_type\":{\"List\":\"Int4\"}}}",
	},
	Pin {
		name: "dictionary_id",
		column_postcard: "1a02020102ffffffff0f00",
		column_json: "{\"DictionaryId\":{\"data\":[{\"U4\":1},{\"U4\":4294967295}],\"dictionary_id\":null}}",
		frame_postcard: "1a02020102ffffffff0f00",
		frame_json: "{\"DictionaryId\":{\"data\":[{\"U4\":1},{\"U4\":4294967295}],\"dictionary_id\":null}}",
	},
	Pin {
		name: "dictionary_id_u16",
		column_postcard: "1a0104ffffffffffffffffffffffffffffffffffff0300",
		column_json: "{\"DictionaryId\":{\"data\":[{\"U16\":340282366920938463463374607431768211455}],\"dictionary_id\":null}}",
		frame_postcard: "1a0104ffffffffffffffffffffffffffffffffffff0300",
		frame_json: "{\"DictionaryId\":{\"data\":[{\"U16\":340282366920938463463374607431768211455}],\"dictionary_id\":null}}",
	},
	Pin {
		name: "option_int4",
		column_postcard: "1b0503020005010503",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,0,-3]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0503020005010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,0,-3]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_utf8",
		column_postcard: "1b0d02016100ffffffff0f010102",
		column_json: "{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[97],[]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b0d02016100010102",
		frame_json: "{\"Option\":{\"inner\":{\"Utf8\":[[97],[]]},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "none_typed_int8",
		column_postcard: "1b06020000010002",
		column_json: "{\"Option\":{\"inner\":{\"Int8\":{\"data\":[0,0]}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
		frame_postcard: "1b06020000010002",
		frame_json: "{\"Option\":{\"inner\":{\"Int8\":{\"data\":[0,0]}},\"bitvec\":{\"bits\":[0],\"len\":2}}}",
	},
	Pin {
		name: "digest",
		column_postcard: "1b1c0201100103904e000000018c01010200012e010002904e010102",
		column_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b1c0201100103904e000000018c01010200012e010002904e010102",
		frame_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "sliced_int4",
		column_postcard: "0503040608",
		column_json: "{\"Int4\":{\"data\":[2,3,4]}}",
		frame_postcard: "0503040608",
		frame_json: "{\"Int4\":{\"data\":[2,3,4]}}",
	},
	Pin {
		name: "sliced_utf8",
		column_postcard: "0d0202626203636363ffffffff0f",
		column_json: "{\"Utf8\":{\"container\":[[98,98],[99,99,99]],\"max_bytes\":4294967295}}",
		frame_postcard: "0d0202626203636363",
		frame_json: "{\"Utf8\":[[98,98],[99,99,99]]}",
	},
	Pin {
		name: "sliced_bool",
		column_postcard: "00012906",
		column_json: "{\"Bool\":{\"data\":{\"bits\":[41],\"len\":6}}}",
		frame_postcard: "00012906",
		frame_json: "{\"Bool\":{\"data\":{\"bits\":[41],\"len\":6}}}",
	},
	Pin {
		name: "sliced_option_int4",
		column_postcard: "1b0503000600010203",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[0,3,0]}},\"bitvec\":{\"bits\":[2],\"len\":3}}}",
		frame_postcard: "1b0503000600010203",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[0,3,0]}},\"bitvec\":{\"bits\":[2],\"len\":3}}}",
	},
];

const COLUMNS_POSTCARD: &str = "020102010200ffffffffffffffffffffffffffffffffffff03020a14021e2802323c02070802050202041b0d02017800ffffffff0f010102020202696402046e616d65";
const COLUMNS_JSON: &str = "{\"system\":{\"row_numbers\":[1,2],\"has_row_numbers\":true,\"partitions\":[0,340282366920938463463374607431768211455],\"created_at\":[10,20],\"updated_at\":[30,40],\"time\":[50,60],\"commit_versions\":[7,8]},\"columns\":[{\"Int4\":{\"data\":[1,2]}},{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[120],[]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[1],\"len\":2}}}],\"names\":[{\"Internal\":{\"text\":\"id\"}},{\"Internal\":{\"text\":\"name\"}}]}";
const FRAME_POSTCARD: &str = "020102010200ffffffffffffffffffffffffffffffffffff03020a14021e2802323c0207080202696405020204046e616d651b0d0201780001010200";
const FRAME_JSON: &str = "{\"system\":{\"row_numbers\":[1,2],\"has_row_numbers\":true,\"partitions\":[0,340282366920938463463374607431768211455],\"created_at\":[10,20],\"updated_at\":[30,40],\"time\":[50,60],\"commit_versions\":[7,8]},\"columns\":[{\"name\":\"id\",\"data\":{\"Int4\":{\"data\":[1,2]}}},{\"name\":\"name\",\"data\":{\"Option\":{\"inner\":{\"Utf8\":[[120],[]]},\"bitvec\":{\"bits\":[1],\"len\":2}}}}],\"op\":null}";

fn uuid7_bits(i: u128) -> Uuid {
	Uuid::from_u128(((i + 1) << 80) | (0x7 << 76) | (0x2 << 62))
}

fn digest_buffer() -> ColumnBuffer {
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
	builder.finish()
}

fn fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	let big = "-1234567890123456789012345678901234567890123".parse::<BigInt>().unwrap();
	let big_unsigned = "98765432109876543210987654321098765432109876".parse::<BigInt>().unwrap();
	vec![
		("bool", ColumnBuffer::bool([true, false, true])),
		("float4", ColumnBuffer::float4([f32::MIN, -0.0, 1.5, f32::MAX])),
		("float8", ColumnBuffer::float8([f64::MIN, -0.0, 2.25, f64::MAX])),
		("int1", ColumnBuffer::int1([i8::MIN, 0, i8::MAX])),
		("int2", ColumnBuffer::int2([i16::MIN, 0, i16::MAX])),
		("int4", ColumnBuffer::int4([i32::MIN, 0, i32::MAX])),
		("int8", ColumnBuffer::int8([i64::MIN, 0, i64::MAX])),
		("int16", ColumnBuffer::int16([i128::MIN, 0, i128::MAX])),
		("uint1", ColumnBuffer::uint1([0, 1, u8::MAX])),
		("uint2", ColumnBuffer::uint2([0, 1, u16::MAX])),
		("uint4", ColumnBuffer::uint4([0, 1, u32::MAX])),
		("uint8", ColumnBuffer::uint8([0, 1, u64::MAX])),
		("uint16", ColumnBuffer::uint16([0, 1, u128::MAX])),
		("utf8", ColumnBuffer::utf8(["a", "", "h\u{e9}llo"])),
		(
			"date",
			ColumnBuffer::date([
				Date::from_ymd(1970, 1, 1).unwrap(),
				Date::from_ymd(1900, 2, 28).unwrap(),
				Date::from_ymd(2026, 9, 22).unwrap(),
			]),
		),
		(
			"datetime",
			ColumnBuffer::datetime([
				DateTime::from_nanos(0),
				DateTime::from_nanos(1_758_500_000_123_456_789),
			]),
		),
		(
			"time",
			ColumnBuffer::time([
				Time::from_hms_nano(0, 0, 0, 0).unwrap(),
				Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
			]),
		),
		(
			"duration",
			ColumnBuffer::duration([
				Duration::new(0, 0, 0).unwrap(),
				Duration::new(1, 2, 3).unwrap(),
				Duration::new(-14, -30, -86_400_000_000_000).unwrap(),
			]),
		),
		(
			"identity_id",
			ColumnBuffer::identity_id([IdentityId(Uuid7(uuid7_bits(0))), IdentityId(Uuid7(uuid7_bits(1)))]),
		),
		("uuid4", ColumnBuffer::uuid4([Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd))])),
		("uuid7", ColumnBuffer::uuid7([Uuid7(uuid7_bits(2)), Uuid7(uuid7_bits(3))])),
		("blob", ColumnBuffer::blob([Blob::new(vec![]), Blob::new(vec![0, 255, 7])])),
		("int", ColumnBuffer::int([Int::from(0i64), Int::from(i128::MIN), Int(big)])),
		("uint", ColumnBuffer::uint([Uint::from(0u64), Uint::from(u128::MAX), Uint(big_unsigned)])),
		(
			"decimal",
			ColumnBuffer::decimal(
				["0", "-1.5", "123456789012345678901234567890.000001"]
					.map(|s| s.parse::<Decimal>().unwrap()),
			),
		),
		("any", ColumnBuffer::any([Value::Int4(7), Value::Utf8("x".to_string()), Value::none()])),
		(
			"any_typed_list",
			ColumnBuffer::any_typed(
				[Value::List(vec![Value::Int4(1), Value::Int4(2)]), Value::List(vec![])],
				ValueType::List(Box::new(ValueType::Int4)),
			),
		),
		(
			"dictionary_id",
			ColumnBuffer::dictionary_id([DictionaryEntryId::U4(1), DictionaryEntryId::U4(u32::MAX)]),
		),
		("dictionary_id_u16", ColumnBuffer::dictionary_id([DictionaryEntryId::U16(u128::MAX)])),
		("option_int4", ColumnBuffer::int4_optional([Some(1), None, Some(-3)])),
		("option_utf8", ColumnBuffer::utf8_with_bitvec(["a".to_string(), String::new()], vec![true, false])),
		("none_typed_int8", ColumnBuffer::none_typed(ValueType::Int8, 2)),
		("digest", digest_buffer()),
		("sliced_int4", ColumnBuffer::int4([1, 2, 3, 4, 5]).slice(1, 4)),
		("sliced_utf8", ColumnBuffer::utf8(["a", "bb", "ccc", "dddd"]).slice(1, 3)),
		(
			"sliced_bool",
			ColumnBuffer::bool([true, false, true, true, false, false, true, false, true, true])
				.slice(3, 9),
		),
		(
			"sliced_option_int4",
			ColumnBuffer::int4_optional([Some(1), None, Some(3), None, Some(5)]).slice(1, 4),
		),
	]
}

fn columns_fixture() -> Columns {
	let system = SystemColumns::new(
		vec![RowNumber(1), RowNumber(2)],
		vec![Partition(0), Partition(u128::MAX)],
		vec![DateTime::from_nanos(10), DateTime::from_nanos(20)],
		vec![DateTime::from_nanos(30), DateTime::from_nanos(40)],
		vec![DateTime::from_nanos(50), DateTime::from_nanos(60)],
		vec![7, 8],
	);
	Columns::with_system(
		vec![
			ColumnWithName::new("id", ColumnBuffer::int4([1, 2])),
			ColumnWithName::new(
				"name",
				ColumnBuffer::utf8_with_bitvec(["x".to_string(), String::new()], vec![true, false]),
			),
		],
		system,
	)
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

fn postcard_hex<T: Serialize>(value: &T) -> String {
	hex(&to_stdvec(value).unwrap())
}

fn json<T: Serialize>(value: &T) -> String {
	serde_json::to_string(value).unwrap()
}

fn decode_postcard<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	from_bytes(&unhex(pinned)).map_err(|err| err.to_string())
}

fn decode_json<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	serde_json::from_str(pinned).map_err(|err| err.to_string())
}

fn check_decoded<T: PartialEq + Debug>(label: String, decoded: Result<T, String>, expected: &T) -> Option<String> {
	match decoded {
		Ok(value) if value == *expected => None,
		Ok(value) => Some(format!("{label}: decoded {value:?}, expected {expected:?}")),
		Err(err) => Some(format!("{label}: decode failed: {err}")),
	}
}

fn pin(name: &str) -> &'static Pin {
	PINS.iter().find(|pin| pin.name == name).unwrap_or_else(|| panic!("no pin for fixture {name}"))
}

fn report(mismatches: Vec<String>) {
	assert!(mismatches.is_empty(), "serde output drifted from the pins:\n{}", mismatches.join("\n"));
}

#[test]
fn every_fixture_has_exactly_one_pin() {
	// A variant that loses its pin would stop guarding its wire format without any test going red.
	let fixture_names: Vec<&str> = fixtures().iter().map(|(name, _)| *name).collect();
	let pin_names: Vec<&str> = PINS.iter().map(|pin| pin.name).collect();
	assert_eq!(fixture_names, pin_names);
}

#[test]
fn column_buffer_postcard_matches_pins() {
	// Column snapshot blocks persist ColumnBuffer with postcard, so any byte drift makes stored blocks unreadable.
	let mismatches = fixtures()
		.iter()
		.filter_map(|(name, buffer)| {
			let actual = postcard_hex(buffer);
			(PINS.iter().find(|pin| pin.name == *name).map(|pin| pin.column_postcard)
				!= Some(actual.as_str()))
			.then(|| format!("{name} column_postcard: \"{actual}\""))
		})
		.collect();
	report(mismatches);
}

#[test]
fn column_buffer_json_matches_pins() {
	// A renamed variant or field changes the self-describing form even when postcard bytes stay the same.
	let mismatches = fixtures()
		.iter()
		.filter_map(|(name, buffer)| {
			let actual = json(buffer);
			(PINS.iter().find(|pin| pin.name == *name).map(|pin| pin.column_json) != Some(actual.as_str()))
				.then(|| format!("{name} column_json: {actual:?}"))
		})
		.collect();
	report(mismatches);
}

#[test]
fn frame_column_data_postcard_matches_pins() {
	// Frames cross process boundaries as postcard, so byte drift breaks peers running the previous build.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, buffer)| {
			let actual = postcard_hex(&FrameColumnData::from(buffer));
			(PINS.iter().find(|pin| pin.name == name).map(|pin| pin.frame_postcard)
				!= Some(actual.as_str()))
			.then(|| format!("{name} frame_postcard: \"{actual}\""))
		})
		.collect();
	report(mismatches);
}

#[test]
fn frame_column_data_json_matches_pins() {
	// A renamed variant or field changes the self-describing form even when postcard bytes stay the same.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, buffer)| {
			let actual = json(&FrameColumnData::from(buffer));
			(PINS.iter().find(|pin| pin.name == name).map(|pin| pin.frame_json) != Some(actual.as_str()))
				.then(|| format!("{name} frame_json: {actual:?}"))
		})
		.collect();
	report(mismatches);
}

#[test]
fn pinned_column_buffer_bytes_decode_to_the_fixture() {
	// Blocks written before the Arrow swap must still read back as the same values after it.
	let mismatches = fixtures()
		.into_iter()
		.flat_map(|(name, buffer)| {
			let pin = pin(name);
			[
				check_decoded(
					format!("{name} postcard"),
					decode_postcard(pin.column_postcard),
					&buffer,
				),
				check_decoded(format!("{name} json"), decode_json(pin.column_json), &buffer),
			]
		})
		.flatten()
		.collect();
	report(mismatches);
}

#[test]
fn pinned_frame_column_data_decodes_to_the_fixture() {
	// A peer on the previous build must decode to the same frame values this build produced.
	let mismatches = fixtures()
		.into_iter()
		.flat_map(|(name, buffer)| {
			let pin = pin(name);
			let expected = FrameColumnData::from(buffer);
			[
				check_decoded(
					format!("{name} postcard"),
					decode_postcard(pin.frame_postcard),
					&expected,
				),
				check_decoded(format!("{name} json"), decode_json(pin.frame_json), &expected),
			]
		})
		.flatten()
		.collect();
	report(mismatches);
}

#[test]
fn columns_serde_matches_pins_and_decodes() {
	// Columns carry system columns next to the buffers; both halves must keep their wire form.
	let columns = columns_fixture();
	report([
		(postcard_hex(&columns) != COLUMNS_POSTCARD)
			.then(|| format!("COLUMNS_POSTCARD: \"{}\"", postcard_hex(&columns))),
		(json(&columns) != COLUMNS_JSON).then(|| format!("COLUMNS_JSON: {:?}", json(&columns))),
	]
	.into_iter()
	.flatten()
	.collect());
	for decoded in [decode_postcard::<Columns>(COLUMNS_POSTCARD), decode_json::<Columns>(COLUMNS_JSON)] {
		let decoded = decoded.unwrap();
		assert_eq!(decoded.system, columns.system);
		assert_eq!(decoded.names, columns.names);
		assert_eq!(decoded.columns, columns.columns);
	}
}

#[test]
fn frame_serde_matches_pins_and_decodes() {
	// A frame is what clients receive, so its encoded form must survive the container swap unchanged.
	let frame = Frame::from(columns_fixture());
	report([
		(postcard_hex(&frame) != FRAME_POSTCARD)
			.then(|| format!("FRAME_POSTCARD: \"{}\"", postcard_hex(&frame))),
		(json(&frame) != FRAME_JSON).then(|| format!("FRAME_JSON: {:?}", json(&frame))),
	]
	.into_iter()
	.flatten()
	.collect());
	assert_eq!(decode_postcard::<Frame>(FRAME_POSTCARD).unwrap(), frame);
	assert_eq!(decode_json::<Frame>(FRAME_JSON).unwrap(), frame);
}

#[test]
fn untyped_any_column_round_trips_through_postcard() {
	// Skipping an absent declared type drops a field postcard cannot detect, so the reader runs past the column.
	let columns = Columns::new(vec![
		ColumnWithName::new("value", ColumnBuffer::any([Value::Int4(1), Value::none()])),
		ColumnWithName::new("after", ColumnBuffer::int4([9, 10])),
	]);
	let decoded: Columns = from_bytes(&to_stdvec(&columns).unwrap()).unwrap();
	assert_eq!(decoded.columns, columns.columns);
	assert_eq!(decoded.names, columns.names);
}
