// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Debug, Write as _};

use arrow_buffer::{BooleanBuffer, NullBuffer};
use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	builder::ColumnBuilder,
	cast::{cast_column_data, convert::TargetConvert},
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		container::digest_array::digest_array,
		date::Date,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
		digest::Digest,
		duration::Duration,
		frame::data::FrameColumnData,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
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

struct FramePin {
	name: &'static str,
	postcard: &'static str,
	json: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "zero_nones_int4_from_cast",
		column_postcard: "1b0503080a0c010703",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[4,5,6]}},\"bitvec\":{\"bits\":[7],\"len\":3}}}",
		frame_postcard: "1b0503080a0c010703",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[4,5,6]}},\"bitvec\":{\"bits\":[7],\"len\":3}}}",
	},
	Pin {
		name: "zero_nones_utf8_from_cast",
		column_postcard: "1b0d020161026263ffffffff0f010302",
		column_json: "{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[97],[98,99]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
		frame_postcard: "1b0d020161026263010302",
		frame_json: "{\"Option\":{\"inner\":{\"Utf8\":[[97],[98,99]]},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
	},
	Pin {
		name: "zero_nones_bool_from_cast",
		column_postcard: "1b00010503010703",
		column_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[7],\"len\":3}}}",
		frame_postcard: "1b00010503010703",
		frame_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[7],\"len\":3}}}",
	},
	Pin {
		name: "zero_nones_int4_from_filter",
		column_postcard: "1b05020206010302",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,3]}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
		frame_postcard: "1b05020206010302",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,3]}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
	},
	Pin {
		name: "zero_nones_int4_from_slice",
		column_postcard: "1b05020608010302",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[3,4]}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
		frame_postcard: "1b05020608010302",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[3,4]}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
	},
	Pin {
		name: "zero_rows_int4_from_builder",
		column_postcard: "1b05000000",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[]}},\"bitvec\":{\"bits\":[],\"len\":0}}}",
		frame_postcard: "1b05000000",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[]}},\"bitvec\":{\"bits\":[],\"len\":0}}}",
	},
	Pin {
		name: "all_none_bool",
		column_postcard: "1b00010003010003",
		column_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[0],\"len\":3}}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b00010003010003",
		frame_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[0],\"len\":3}}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_utf8",
		column_postcard: "1b0d03000000ffffffff0f010003",
		column_json: "{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[],[],[]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b0d03000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Utf8\":[[],[],[]]},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_uuid4",
		column_postcard: "1b1303100000000000000000000000000000000010000000000000000000000000000000001000000000000000000000000000000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Uuid4\":{\"data\":[\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b1303100000000000000000000000000000000010000000000000000000000000000000001000000000000000000000000000000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Uuid4\":{\"data\":[\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_uuid7",
		column_postcard: "1b1403100000000000000000000000000000000010000000000000000000000000000000001000000000000000000000000000000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Uuid7\":{\"data\":[\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b1403100000000000000000000000000000000010000000000000000000000000000000001000000000000000000000000000000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Uuid7\":{\"data\":[\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_date",
		column_postcard: "1b0e03000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Date\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b0e03000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Date\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_datetime",
		column_postcard: "1b0f03000000010003",
		column_json: "{\"Option\":{\"inner\":{\"DateTime\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b0f03000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"DateTime\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_time",
		column_postcard: "1b1003000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Time\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b1003000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Time\":{\"data\":[0,0,0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_duration",
		column_postcard: "1b1103000000000000000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Duration\":{\"data\":[{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":0,\"days\":0,\"nanos\":0}]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b1103000000000000000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Duration\":{\"data\":[{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":0,\"days\":0,\"nanos\":0}]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_float8",
		column_postcard: "1b0203000000000000000000000000000000000000000000000000010003",
		column_json: "{\"Option\":{\"inner\":{\"Float8\":{\"data\":[0.0,0.0,0.0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b0203000000000000000000000000000000000000000000000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Float8\":{\"data\":[0.0,0.0,0.0]}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "all_none_blob",
		column_postcard: "1b1503000000ffffffff0f010003",
		column_json: "{\"Option\":{\"inner\":{\"Blob\":{\"container\":[[],[],[]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
		frame_postcard: "1b1503000000010003",
		frame_json: "{\"Option\":{\"inner\":{\"Blob\":[[],[],[]]},\"bitvec\":{\"bits\":[0],\"len\":3}}}",
	},
	Pin {
		name: "option_bool",
		column_postcard: "1b00010503010503",
		column_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b00010503010503",
		frame_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_float4",
		column_postcard: "1b01030000c03f00000000ffff7fff010503",
		column_json: "{\"Option\":{\"inner\":{\"Float4\":{\"data\":[1.5,0.0,-3.4028235e+38]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b01030000c03f00000000ffff7fff010503",
		frame_json: "{\"Option\":{\"inner\":{\"Float4\":{\"data\":[1.5,0.0,-3.4028235e+38]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_float8",
		column_postcard: "1b020300000000000002c00000000000000000ffffffffffffef7f010503",
		column_json: "{\"Option\":{\"inner\":{\"Float8\":{\"data\":[-2.25,0.0,1.7976931348623157e+308]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b020300000000000002c00000000000000000ffffffffffffef7f010503",
		frame_json: "{\"Option\":{\"inner\":{\"Float8\":{\"data\":[-2.25,0.0,1.7976931348623157e+308]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_int1",
		column_postcard: "1b030380007f010503",
		column_json: "{\"Option\":{\"inner\":{\"Int1\":{\"data\":[-128,0,127]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b030380007f010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int1\":{\"data\":[-128,0,127]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_int2",
		column_postcard: "1b0403ffff0300feff03010503",
		column_json: "{\"Option\":{\"inner\":{\"Int2\":{\"data\":[-32768,0,32767]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0403ffff0300feff03010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int2\":{\"data\":[-32768,0,32767]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_int8",
		column_postcard: "1b0603ffffffffffffffffff0100feffffffffffffffff01010503",
		column_json: "{\"Option\":{\"inner\":{\"Int8\":{\"data\":[-9223372036854775808,0,9223372036854775807]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0603ffffffffffffffffff0100feffffffffffffffff01010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int8\":{\"data\":[-9223372036854775808,0,9223372036854775807]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_uint1",
		column_postcard: "1b08030100ff010503",
		column_json: "{\"Option\":{\"inner\":{\"Uint1\":{\"data\":[1,0,255]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b08030100ff010503",
		frame_json: "{\"Option\":{\"inner\":{\"Uint1\":{\"data\":[1,0,255]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_uint2",
		column_postcard: "1b09030100ffff03010503",
		column_json: "{\"Option\":{\"inner\":{\"Uint2\":{\"data\":[1,0,65535]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b09030100ffff03010503",
		frame_json: "{\"Option\":{\"inner\":{\"Uint2\":{\"data\":[1,0,65535]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_uint4",
		column_postcard: "1b0a030100ffffffff0f010503",
		column_json: "{\"Option\":{\"inner\":{\"Uint4\":{\"data\":[1,0,4294967295]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0a030100ffffffff0f010503",
		frame_json: "{\"Option\":{\"inner\":{\"Uint4\":{\"data\":[1,0,4294967295]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_uint8",
		column_postcard: "1b0b030100ffffffffffffffffff01010503",
		column_json: "{\"Option\":{\"inner\":{\"Uint8\":{\"data\":[1,0,18446744073709551615]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0b030100ffffffffffffffffff01010503",
		frame_json: "{\"Option\":{\"inner\":{\"Uint8\":{\"data\":[1,0,18446744073709551615]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_date",
		column_postcard: "1b0e03c98e0300dcc302010503",
		column_json: "{\"Option\":{\"inner\":{\"Date\":{\"data\":[-25509,0,20718]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0e03c98e0300dcc302010503",
		frame_json: "{\"Option\":{\"inner\":{\"Date\":{\"data\":[-25509,0,20718]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_datetime",
		column_postcard: "1b0f030200aab490cedc9bb9e730010503",
		column_json: "{\"Option\":{\"inner\":{\"DateTime\":{\"data\":[1,0,1758500000123456789]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0f030200aab490cedc9bb9e730010503",
		frame_json: "{\"Option\":{\"inner\":{\"DateTime\":{\"data\":[1,0,1758500000123456789]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_time",
		column_postcard: "1b100384dcdea0ad6c00ffffbb8ac9d213010503",
		column_json: "{\"Option\":{\"inner\":{\"Time\":{\"data\":[3723000000004,0,86399999999999]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b100384dcdea0ad6c00ffffbb8ac9d213010503",
		frame_json: "{\"Option\":{\"inner\":{\"Time\":{\"data\":[3723000000004,0,86399999999999]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_duration",
		column_postcard: "1b11030204060000001b3d00010503",
		column_json: "{\"Option\":{\"inner\":{\"Duration\":{\"data\":[{\"months\":1,\"days\":2,\"nanos\":3},{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":-14,\"days\":-31,\"nanos\":0}]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b11030204060000001b3d00010503",
		frame_json: "{\"Option\":{\"inner\":{\"Duration\":{\"data\":[{\"months\":1,\"days\":2,\"nanos\":3},{\"months\":0,\"days\":0,\"nanos\":0},{\"months\":-14,\"days\":-31,\"nanos\":0}]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_uuid4",
		column_postcard: "1b1302100123456789ab4cde8f0123456789abcd1000000000000000000000000000000000010102",
		column_json: "{\"Option\":{\"inner\":{\"Uuid4\":{\"data\":[\"01234567-89ab-4cde-8f01-23456789abcd\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b1302100123456789ab4cde8f0123456789abcd1000000000000000000000000000000000010102",
		frame_json: "{\"Option\":{\"inner\":{\"Uuid4\":{\"data\":[\"01234567-89ab-4cde-8f01-23456789abcd\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "option_uuid7",
		column_postcard: "1b140210000000000006700080000000000000001000000000000000000000000000000000010102",
		column_json: "{\"Option\":{\"inner\":{\"Uuid7\":{\"data\":[\"00000000-0006-7000-8000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b140210000000000006700080000000000000001000000000000000000000000000000000010102",
		frame_json: "{\"Option\":{\"inner\":{\"Uuid7\":{\"data\":[\"00000000-0006-7000-8000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "option_blob",
		column_postcard: "1b15030300ff070000ffffffff0f010503",
		column_json: "{\"Option\":{\"inner\":{\"Blob\":{\"container\":[[0,255,7],[],[]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b15030300ff070000010503",
		frame_json: "{\"Option\":{\"inner\":{\"Blob\":[[0,255,7],[],[]]},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "option_tuple",
		column_postcard: "1b19021f020602090179001a00010102",
		column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Tuple\":[{\"Int4\":1},{\"Utf8\":\"y\"}]},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b19021f020602090179001a00010102",
		frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"Tuple\":[{\"Int4\":1},{\"Utf8\":\"y\"}]},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":null}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "option_typed_list",
		column_postcard: "1b19021d0206020604001a011c05010102",
		column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"List\":[{\"Int4\":1},{\"Int4\":2}]},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":{\"List\":\"Int4\"}}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b19021d0206020604001a011c05010102",
		frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"List\":[{\"Int4\":1},{\"Int4\":2}]},{\"None\":{\"inner\":\"Any\"}}],\"declared_type\":{\"List\":\"Int4\"}}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
	Pin {
		name: "option_dictionary_id_with_dictionary",
		column_postcard: "1b1a03020700000309012a010503",
		column_json: "{\"Option\":{\"inner\":{\"DictionaryId\":{\"data\":[{\"U4\":7},{\"U1\":0},{\"U8\":9}],\"dictionary_id\":42}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b1a03020700000309012a010503",
		frame_json: "{\"Option\":{\"inner\":{\"DictionaryId\":{\"data\":[{\"U4\":7},{\"U1\":0},{\"U8\":9}],\"dictionary_id\":42}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "sliced_option_int4_at_offset_8",
		column_postcard: "1b0505a001b40100dc01f001011b05",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[80,90,0,110,120]}},\"bitvec\":{\"bits\":[27],\"len\":5}}}",
		frame_postcard: "1b0505a001b40100dc01f001011b05",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[80,90,0,110,120]}},\"bitvec\":{\"bits\":[27],\"len\":5}}}",
	},
	Pin {
		name: "sliced_option_int4_at_offset_3",
		column_postcard: "1b05043c006478010d04",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[30,0,50,60]}},\"bitvec\":{\"bits\":[13],\"len\":4}}}",
		frame_postcard: "1b05043c006478010d04",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[30,0,50,60]}},\"bitvec\":{\"bits\":[13],\"len\":4}}}",
	},
	Pin {
		name: "sliced_option_utf8_at_offset_3",
		column_postcard: "1b0d03026464000166ffffffff0f010503",
		column_json: "{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[100,100],[],[102]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0d03026464000166010503",
		frame_json: "{\"Option\":{\"inner\":{\"Utf8\":[[100,100],[],[102]]},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "sliced_option_bool_at_offset_3",
		column_postcard: "1b00010603010603",
		column_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[6],\"len\":3}}},\"bitvec\":{\"bits\":[6],\"len\":3}}}",
		frame_postcard: "1b00010603010603",
		frame_json: "{\"Option\":{\"inner\":{\"Bool\":{\"data\":{\"bits\":[6],\"len\":3}}},\"bitvec\":{\"bits\":[6],\"len\":3}}}",
	},
	Pin {
		name: "sliced_option_any_at_offset_3",
		column_postcard: "1b1903001a0101070c00010603",
		column_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Any\"}},{\"Boolean\":true},{\"Int8\":6}],\"declared_type\":null}},\"bitvec\":{\"bits\":[6],\"len\":3}}}",
		frame_postcard: "1b1903001a0101070c00010603",
		frame_json: "{\"Option\":{\"inner\":{\"Any\":{\"data\":[{\"None\":{\"inner\":\"Any\"}},{\"Boolean\":true},{\"Int8\":6}],\"declared_type\":null}},\"bitvec\":{\"bits\":[6],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4",
		column_postcard: "1b050402c6010608010d04",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3,4]}},\"bitvec\":{\"bits\":[13],\"len\":4}}}",
		frame_postcard: "1b050402c6010608010d04",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3,4]}},\"bitvec\":{\"bits\":[13],\"len\":4}}}",
	},
	Pin {
		name: "placeholder_utf8",
		column_postcard: "1b0d030161027a7a0163ffffffff0f010503",
		column_json: "{\"Option\":{\"inner\":{\"Utf8\":{\"container\":[[97],[122,122],[99]],\"max_bytes\":4294967295}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b0d030161027a7a0163010503",
		frame_json: "{\"Option\":{\"inner\":{\"Utf8\":[[97],[122,122],[99]]},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4_take",
		column_postcard: "1b050302c60106010503",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b050302c60106010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4_filter",
		column_postcard: "1b050302c60108010503",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,4]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b050302c60108010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,4]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4_reorder",
		column_postcard: "1b050308c60102010503",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[4,99,1]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		frame_postcard: "1b050308c60102010503",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[4,99,1]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4_reorder_out_of_range",
		column_postcard: "1b05030200c601010103",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,0,99]}},\"bitvec\":{\"bits\":[1],\"len\":3}}}",
		frame_postcard: "1b05030200c601010103",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,0,99]}},\"bitvec\":{\"bits\":[1],\"len\":3}}}",
	},
	Pin {
		name: "placeholder_int4_extend_by_bare",
		column_postcard: "1b050602c60106080a0c013d06",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3,4,5,6]}},\"bitvec\":{\"bits\":[61],\"len\":6}}}",
		frame_postcard: "1b050602c60106080a0c013d06",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[1,99,3,4,5,6]}},\"bitvec\":{\"bits\":[61],\"len\":6}}}",
	},
	Pin {
		name: "bare_int4_extend_by_placeholder",
		column_postcard: "1b05060a0c02c6010608013706",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[5,6,1,99,3,4]}},\"bitvec\":{\"bits\":[55],\"len\":6}}}",
		frame_postcard: "1b05060a0c02c6010608013706",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[5,6,1,99,3,4]}},\"bitvec\":{\"bits\":[55],\"len\":6}}}",
	},
	Pin {
		name: "all_none_utf8_extend_by_int4",
		column_postcard: "1b050400000a0c010c04",
		column_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[0,0,5,6]}},\"bitvec\":{\"bits\":[12],\"len\":4}}}",
		frame_postcard: "1b050400000a0c010c04",
		frame_json: "{\"Option\":{\"inner\":{\"Int4\":{\"data\":[0,0,5,6]}},\"bitvec\":{\"bits\":[12],\"len\":4}}}",
	},
	Pin {
		name: "option_digest_defined_bit_over_empty_slot",
		column_postcard: "1b1c0201100103904e000000018c01010200012e010002904e010302",
		column_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
		frame_postcard: "1b1c0201100103904e000000018c01010200012e010002904e010302",
		frame_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],null]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[3],\"len\":2}}}",
	},
	Pin {
		name: "option_digest_cleared_bit_over_real_digest",
		column_postcard: "1b1c0201100103904e000000018c01010200012e01010c0103904e0000000001d0010102904e010102",
		column_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],[1,3,144,78,0,0,0,0,1,208,1,1]]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
		frame_postcard: "1b1c0201100103904e000000018c01010200012e01010c0103904e0000000001d0010102904e010102",
		frame_json: "{\"Option\":{\"inner\":{\"Digest\":{\"container\":{\"data\":[[1,3,144,78,0,0,0,1,140,1,1,2,0,1,46,1],[1,3,144,78,0,0,0,0,1,208,1,1]]},\"inner\":\"Float8\",\"accuracy\":10000}},\"bitvec\":{\"bits\":[1],\"len\":2}}}",
	},
];

const FRAME_PINS: &[FramePin] = &[
	FramePin {
		name: "nested_option_int4_depth_2",
		postcard: "1b1b05030e0012010503010303",
		json: "{\"Option\":{\"inner\":{\"Option\":{\"inner\":{\"Int4\":{\"data\":[7,0,9]}},\"bitvec\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[3],\"len\":3}}}",
	},
	FramePin {
		name: "nested_option_int4_depth_3",
		postcard: "1b1b1b05030e0012010503010303010603",
		json: "{\"Option\":{\"inner\":{\"Option\":{\"inner\":{\"Option\":{\"inner\":{\"Int4\":{\"data\":[7,0,9]}},\"bitvec\":{\"bits\":[5],\"len\":3}}},\"bitvec\":{\"bits\":[3],\"len\":3}}},\"bitvec\":{\"bits\":[6],\"len\":3}}}",
	},
];

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

fn check_written(label: String, actual: String, pinned: &str) -> Option<String> {
	(actual != pinned).then(|| format!("{label}: {actual:?}"))
}

fn check_decoded<T: PartialEq + Debug>(label: String, decoded: Result<T, String>, expected: &T) -> Option<String> {
	match decoded {
		Ok(value) if value == *expected => None,
		Ok(value) => Some(format!("{label} decode: got {value:?}, expected {expected:?}")),
		Err(err) => Some(format!("{label} decode failed: {err}")),
	}
}

fn check_reserialized<T>(
	label: String,
	decoded: Result<T, String>,
	pinned: &str,
	write: fn(&T) -> String,
) -> Option<String> {
	match decoded {
		Ok(value) => {
			let again = write(&value);
			(again != pinned).then(|| format!("{label} re-serialized to {again:?}"))
		}
		Err(err) => Some(format!("{label} decode failed: {err}")),
	}
}

fn column_mismatches(name: &str, buffer: ColumnBuffer) -> Vec<String> {
	let Some(pin) = PINS.iter().find(|pin| pin.name == name) else {
		return vec![format!("{name}: no pin")];
	};
	let frame = FrameColumnData::from(buffer.clone());
	[
		check_written(format!("{name} column_postcard"), postcard_hex(&buffer), pin.column_postcard),
		check_written(format!("{name} column_json"), json(&buffer), pin.column_json),
		check_written(format!("{name} frame_postcard"), postcard_hex(&frame), pin.frame_postcard),
		check_written(format!("{name} frame_json"), json(&frame), pin.frame_json),
		check_decoded(format!("{name} column postcard"), decode_postcard(pin.column_postcard), &buffer),
		check_decoded(format!("{name} column json"), decode_json(pin.column_json), &buffer),
		check_decoded(format!("{name} frame postcard"), decode_postcard(pin.frame_postcard), &frame),
		check_decoded(format!("{name} frame json"), decode_json(pin.frame_json), &frame),
		check_reserialized::<ColumnBuffer>(
			format!("{name} column postcard"),
			decode_postcard(pin.column_postcard),
			pin.column_postcard,
			postcard_hex,
		),
		check_reserialized::<ColumnBuffer>(
			format!("{name} column json"),
			decode_json(pin.column_json),
			pin.column_json,
			json,
		),
		check_reserialized::<FrameColumnData>(
			format!("{name} frame postcard"),
			decode_postcard(pin.frame_postcard),
			pin.frame_postcard,
			postcard_hex,
		),
		check_reserialized::<FrameColumnData>(
			format!("{name} frame json"),
			decode_json(pin.frame_json),
			pin.frame_json,
			json,
		),
	]
	.into_iter()
	.flatten()
	.collect()
}

fn frame_mismatches(name: &str, frame: FrameColumnData) -> Vec<String> {
	let Some(pin) = FRAME_PINS.iter().find(|pin| pin.name == name) else {
		return vec![format!("{name}: no pin")];
	};
	[
		check_written(format!("{name} postcard"), postcard_hex(&frame), pin.postcard),
		check_written(format!("{name} json"), json(&frame), pin.json),
		check_decoded(format!("{name} postcard"), decode_postcard(pin.postcard), &frame),
		check_decoded(format!("{name} json"), decode_json(pin.json), &frame),
		check_reserialized::<FrameColumnData>(
			format!("{name} postcard"),
			decode_postcard(pin.postcard),
			pin.postcard,
			postcard_hex,
		),
		check_reserialized::<FrameColumnData>(format!("{name} json"), decode_json(pin.json), pin.json, json),
	]
	.into_iter()
	.flatten()
	.collect()
}

fn assert_columns_pinned(fixtures: Vec<(&'static str, ColumnBuffer)>) {
	let mismatches: Vec<String> =
		fixtures.into_iter().flat_map(|(name, buffer)| column_mismatches(name, buffer)).collect();
	assert!(mismatches.is_empty(), "serde output drifted from the pins:\n{}", mismatches.join("\n"));
}

fn assert_frames_pinned(fixtures: Vec<(&'static str, FrameColumnData)>) {
	let mismatches: Vec<String> =
		fixtures.into_iter().flat_map(|(name, frame)| frame_mismatches(name, frame)).collect();
	assert!(mismatches.is_empty(), "serde output drifted from the pins:\n{}", mismatches.join("\n"));
}

fn bits(values: &[bool]) -> BooleanBuffer {
	BooleanBuffer::from(values.to_vec())
}

fn uuid7_bits(i: u128) -> Uuid {
	Uuid::from_u128(((i + 1) << 80) | (0x7 << 76) | (0x2 << 62))
}

fn digest_of(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

fn built(ty: ValueType, values: Vec<Option<Value>>) -> ColumnBuffer {
	let mut builder = ColumnBuilder::with_capacity(ty, values.len());
	for value in values {
		match value {
			Some(value) => builder.push_value(value),
			None => builder.push_none(),
		}
	}
	builder.finish()
}

fn option_of(ty: ValueType) -> ValueType {
	ValueType::Option(Box::new(ty))
}

fn placeholder_int4() -> ColumnBuffer {
	ColumnBuffer::int4_with_bitvec([1, 99, 3, 4], bits(&[true, false, true, true]))
}

fn with_inner_dictionary_id(mut buffer: ColumnBuffer, id: DictionaryId) -> ColumnBuffer {
	assert!(buffer.nulls().is_some(), "fixture must be an Option buffer");
	let ColumnBuffer::DictionaryId {
		dictionary_id,
		..
	} = &mut buffer
	else {
		panic!("fixture must wrap a DictionaryId buffer");
	};
	*dictionary_id = Some(id);
	buffer
}

fn cast_to_option(buffer: ColumnBuffer) -> ColumnBuffer {
	let ty = option_of(buffer.get_type());
	cast_column_data(
		TargetConvert {
			target: None,
		},
		&buffer,
		ty,
		|| Fragment::internal("fixture"),
	)
	.unwrap()
}

fn zero_none_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	let filtered = {
		let mut buffer = ColumnBuffer::int4_optional([Some(1), None, Some(3)]);
		buffer.filter(&bits(&[true, false, true])).unwrap();
		buffer
	};
	vec![
		("zero_nones_int4_from_cast", cast_to_option(ColumnBuffer::int4([4, 5, 6]))),
		("zero_nones_utf8_from_cast", cast_to_option(ColumnBuffer::utf8(["a", "bc"]))),
		("zero_nones_bool_from_cast", cast_to_option(ColumnBuffer::bool([true, false, true]))),
		("zero_nones_int4_from_filter", filtered),
		(
			"zero_nones_int4_from_slice",
			ColumnBuffer::int4_optional([Some(1), None, Some(3), Some(4)]).slice(2, 4),
		),
		("zero_rows_int4_from_builder", built(option_of(ValueType::Int4), vec![])),
	]
}

fn all_none_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	vec![
		("all_none_bool", ColumnBuffer::none_typed(ValueType::Boolean, 3)),
		("all_none_utf8", ColumnBuffer::none_typed(ValueType::Utf8, 3)),
		("all_none_uuid4", ColumnBuffer::none_typed(ValueType::Uuid4, 3)),
		("all_none_uuid7", ColumnBuffer::none_typed(ValueType::Uuid7, 3)),
		("all_none_date", ColumnBuffer::none_typed(ValueType::Date, 3)),
		("all_none_datetime", ColumnBuffer::none_typed(ValueType::DateTime, 3)),
		("all_none_time", ColumnBuffer::none_typed(ValueType::Time, 3)),
		("all_none_duration", ColumnBuffer::none_typed(ValueType::Duration, 3)),
		("all_none_float8", ColumnBuffer::none_typed(ValueType::Float8, 3)),
		("all_none_blob", ColumnBuffer::none_typed(ValueType::Blob, 3)),
	]
}

fn option_kind_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	vec![
		("option_bool", ColumnBuffer::bool_with_bitvec([true, false, true], vec![true, false, true])),
		("option_float4", ColumnBuffer::float4_with_bitvec([1.5, 0.0, f32::MIN], vec![true, false, true])),
		("option_float8", ColumnBuffer::float8_with_bitvec([-2.25, 0.0, f64::MAX], vec![true, false, true])),
		("option_int1", ColumnBuffer::int1_with_bitvec([i8::MIN, 0, i8::MAX], vec![true, false, true])),
		("option_int2", ColumnBuffer::int2_with_bitvec([i16::MIN, 0, i16::MAX], vec![true, false, true])),
		("option_int8", ColumnBuffer::int8_with_bitvec([i64::MIN, 0, i64::MAX], vec![true, false, true])),
		("option_uint1", ColumnBuffer::uint1_with_bitvec([1, 0, u8::MAX], vec![true, false, true])),
		("option_uint2", ColumnBuffer::uint2_with_bitvec([1, 0, u16::MAX], vec![true, false, true])),
		("option_uint4", ColumnBuffer::uint4_with_bitvec([1, 0, u32::MAX], vec![true, false, true])),
		("option_uint8", ColumnBuffer::uint8_with_bitvec([1, 0, u64::MAX], vec![true, false, true])),
		(
			"option_date",
			ColumnBuffer::date_with_bitvec(
				[
					Date::from_ymd(1900, 2, 28).unwrap(),
					Date::default(),
					Date::from_ymd(2026, 9, 22).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_datetime",
			ColumnBuffer::datetime_with_bitvec(
				[
					DateTime::from_nanos(1),
					DateTime::default(),
					DateTime::from_nanos(1_758_500_000_123_456_789),
				],
				vec![true, false, true],
			),
		),
		(
			"option_time",
			ColumnBuffer::time_with_bitvec(
				[
					Time::from_hms_nano(1, 2, 3, 4).unwrap(),
					Time::default(),
					Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_duration",
			ColumnBuffer::duration_with_bitvec(
				[
					Duration::new(1, 2, 3).unwrap(),
					Duration::default(),
					Duration::new(-14, -30, -86_400_000_000_000).unwrap(),
				],
				vec![true, false, true],
			),
		),
		(
			"option_uuid4",
			ColumnBuffer::uuid4_with_bitvec(
				[Uuid4(Uuid::from_u128(0x0123_4567_89ab_4cde_8f01_2345_6789_abcd)), Uuid4::default()],
				vec![true, false],
			),
		),
		(
			"option_uuid7",
			ColumnBuffer::uuid7_with_bitvec([Uuid7(uuid7_bits(5)), Uuid7::default()], vec![true, false]),
		),
		(
			"option_blob",
			ColumnBuffer::blob_with_bitvec(
				[Blob::new(vec![0, 255, 7]), Blob::default(), Blob::new(vec![])],
				vec![true, false, true],
			),
		),
		(
			"option_tuple",
			built(
				ValueType::Tuple(vec![ValueType::Int4, ValueType::Utf8]),
				vec![Some(Value::Tuple(vec![Value::Int4(1), Value::Utf8("y".to_string())])), None],
			),
		),
		(
			"option_typed_list",
			built(
				ValueType::List(Box::new(ValueType::Int4)),
				vec![Some(Value::List(vec![Value::Int4(1), Value::Int4(2)])), None],
			),
		),
	]
}

fn dictionary_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	vec![(
		"option_dictionary_id_with_dictionary",
		with_inner_dictionary_id(
			ColumnBuffer::dictionary_id_with_bitvec(
				[DictionaryEntryId::U4(7), DictionaryEntryId::default(), DictionaryEntryId::U8(9)],
				vec![true, false, true],
			),
			DictionaryId(42),
		),
	)]
}

fn sliced_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	let long = || ColumnBuffer::int4_optional((0..16).map(|i| (i % 3 != 1).then_some(i * 10)));
	vec![
		("sliced_option_int4_at_offset_8", long().slice(8, 13)),
		("sliced_option_int4_at_offset_3", long().slice(3, 7)),
		(
			"sliced_option_utf8_at_offset_3",
			ColumnBuffer::utf8_with_bitvec(
				["a", "", "ccc", "dd", "", "f"].map(String::from),
				vec![true, false, true, true, false, true],
			)
			.slice(3, 6),
		),
		(
			"sliced_option_bool_at_offset_3",
			ColumnBuffer::bool_with_bitvec(
				[true, false, false, false, true, true],
				vec![true, false, true, false, true, true],
			)
			.slice(3, 6),
		),
		(
			"sliced_option_any_at_offset_3",
			ColumnBuffer::any_with_bitvec(
				[
					Value::Int4(1),
					Value::none(),
					Value::Utf8("c".to_string()),
					Value::none(),
					Value::Boolean(true),
					Value::Int8(6),
				],
				bits(&[true, false, true, false, true, true]),
			)
			.slice(3, 6),
		),
	]
}

fn placeholder_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	let filtered = {
		let mut buffer = placeholder_int4();
		buffer.filter(&bits(&[true, true, false, true])).unwrap();
		buffer
	};
	let reordered = {
		let mut buffer = placeholder_int4();
		buffer.reorder(&[3, 1, 0]);
		buffer
	};
	let reordered_out_of_range = {
		let mut buffer = placeholder_int4();
		buffer.reorder(&[0, 9, 1]);
		buffer
	};
	let extended_by_bare = {
		let mut buffer = placeholder_int4();
		buffer.extend(ColumnBuffer::int4([5, 6])).unwrap();
		buffer
	};
	let bare_extended = {
		let mut buffer = ColumnBuffer::int4([5, 6]);
		buffer.extend(placeholder_int4()).unwrap();
		buffer
	};
	let all_none_extended = {
		let mut buffer = ColumnBuffer::none_typed(ValueType::Utf8, 2);
		buffer.extend(ColumnBuffer::int4([5, 6])).unwrap();
		buffer
	};
	vec![
		("placeholder_int4", placeholder_int4()),
		("placeholder_utf8", ColumnBuffer::utf8_with_bitvec(["a", "zz", "c"], bits(&[true, false, true]))),
		("placeholder_int4_take", placeholder_int4().take(3)),
		("placeholder_int4_filter", filtered),
		("placeholder_int4_reorder", reordered),
		("placeholder_int4_reorder_out_of_range", reordered_out_of_range),
		("placeholder_int4_extend_by_bare", extended_by_bare),
		("bare_int4_extend_by_placeholder", bare_extended),
		("all_none_utf8_extend_by_int4", all_none_extended),
	]
}

fn digest_fixtures() -> Vec<(&'static str, ColumnBuffer)> {
	let first = digest_of(&[1.0, 2.5, -4.0]);
	let second = digest_of(&[8.0]);
	let digest_column = |slots: [Option<&Digest>; 2]| ColumnBuffer::Digest {
		container: digest_array(slots),
		inner: ValueType::Float8,
		accuracy: 10_000,
	};
	vec![
		(
			"option_digest_defined_bit_over_empty_slot",
			digest_column([Some(&first), None]).with_nulls(NullBuffer::new(bits(&[true, true]))),
		),
		(
			"option_digest_cleared_bit_over_real_digest",
			digest_column([Some(&first), Some(&second)]).with_nulls(NullBuffer::new(bits(&[true, false]))),
		),
	]
}

fn nested_frame_fixtures() -> Vec<(&'static str, FrameColumnData)> {
	let depth_two = || FrameColumnData::Option {
		inner: Box::new(FrameColumnData::from(ColumnBuffer::int4_with_bitvec(
			[7, 0, 9],
			bits(&[true, false, true]),
		))),
		bitvec: bits(&[true, true, false]),
	};
	vec![
		("nested_option_int4_depth_2", depth_two()),
		(
			"nested_option_int4_depth_3",
			FrameColumnData::Option {
				inner: Box::new(depth_two()),
				bitvec: bits(&[false, true, true]),
			},
		),
	]
}

#[test]
fn every_pin_has_exactly_one_fixture() {
	// A pin whose fixture is dropped stops guarding its bytes without any test going red.
	let column_names: Vec<&str> = [
		zero_none_fixtures(),
		all_none_fixtures(),
		option_kind_fixtures(),
		dictionary_fixtures(),
		sliced_fixtures(),
		placeholder_fixtures(),
		digest_fixtures(),
	]
	.into_iter()
	.flatten()
	.map(|(name, _)| name)
	.collect();
	assert_eq!(column_names, PINS.iter().map(|pin| pin.name).collect::<Vec<_>>());
	let frame_names: Vec<&str> = nested_frame_fixtures().into_iter().map(|(name, _)| name).collect();
	assert_eq!(frame_names, FRAME_PINS.iter().map(|pin| pin.name).collect::<Vec<_>>());
}

#[test]
fn nullable_columns_with_zero_nones_keep_the_option_wrapper_on_the_wire() {
	// A nullable column with no none row must still write the Option wrapper and an all-set bitmap.
	assert_columns_pinned(zero_none_fixtures());
}

#[test]
fn all_none_columns_are_pinned() {
	// Every none row must keep exactly today's placeholder and a cleared bit, per kind.
	assert_columns_pinned(all_none_fixtures());
}

#[test]
fn option_columns_of_every_remaining_kind_are_pinned() {
	// Each kind must keep its placeholder under the none row, otherwise stored optional columns drift.
	assert_columns_pinned(option_kind_fixtures());
}

#[test]
fn option_dictionary_id_keeps_its_dictionary_on_the_wire() {
	// The dictionary id must survive under the Option wrapper, never be dropped when the none bitmap moves.
	assert_columns_pinned(dictionary_fixtures());
}

#[test]
fn sliced_option_columns_repack_the_bitmap_from_the_slice_start() {
	// A slice at any offset must write the bitmap and values from the slice start, exactly like a fresh column.
	assert_columns_pinned(sliced_fixtures());
}

#[test]
fn values_under_none_rows_survive_serde_and_row_operations() {
	// A value stored under a none row must be written as stored, never replaced by a default.
	assert_columns_pinned(placeholder_fixtures());
}

#[test]
fn option_digest_bits_and_slots_are_pinned_independently() {
	// The Option bit and the Digest empty slot are separate nones and must both be written exactly.
	assert_columns_pinned(digest_fixtures());
}

#[test]
fn nested_option_frames_keep_every_layer_on_the_wire() {
	// Frames keep nesting, so every layer's bitmap must be written outer first and read back exactly.
	assert_frames_pinned(nested_frame_fixtures());
}
