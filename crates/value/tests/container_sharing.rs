// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use arrow_array::{
	Array, BooleanArray, Date32Array, FixedSizeBinaryArray, GenericByteArray, IntervalMonthDayNanoArray,
	LargeBinaryArray, LargeStringArray, Time64NanosecondArray, UInt64Array, types::ByteArrayType,
};
use arrow_buffer::BooleanBuffer;
use postcard::{from_bytes, to_allocvec};
use reifydb_value::{
	util::bitmap,
	value::{
		Value,
		blob::Blob,
		container::{
			any::AnyContainer,
			bool_array,
			number::NumberContainer,
			primitive,
			row::RowNumberContainer,
			temporal_array, uuid_array,
			varlen_array::{self, blob_array, compact_parts, equals, get, slice},
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};
use serde::{Deserialize, Serialize};
use serde_json::{from_str as json_from_str, to_string as json_to_string};
use uuid::Uuid;

const ROWS: usize = 1000;

fn points_into<T>(parent: &[T], child: &[T]) -> bool {
	let start = parent.as_ptr() as usize;
	let end = start + parent.len() * size_of::<T>();
	let child_start = child.as_ptr() as usize;
	let child_end = child_start + child.len() * size_of::<T>();
	child_start >= start && child_end <= end
}

macro_rules! slice_backed_suite {
	($name:ident, $container:ty, $elem:ty, $gen:expr, $extra:expr) => {
		mod $name {
			use super::*;

			fn value(i: usize) -> $elem {
				let make: fn(usize) -> $elem = $gen;
				make(i)
			}

			fn values(n: usize) -> Vec<$elem> {
				(0..n).map(value).collect()
			}

			fn extra() -> $elem {
				$extra
			}

			fn frozen(n: usize) -> $container {
				let mut c = <$container>::from_vec(values(n));
				c.freeze();
				c
			}

			#[test]
			fn freeze_keeps_the_data_pointer() {
				// Freezing must wrap the existing allocation, never copy it.
				let mut c = <$container>::from_vec(values(ROWS));
				let before = c.data().as_ptr();
				c.freeze();
				assert_eq!(c.data().as_ptr(), before);
				assert_eq!(c.data(), &values(ROWS)[..]);
				assert_eq!(
					c.data_mut().as_ptr(),
					before,
					"a unique frozen container must thaw in place, never copy"
				);
			}

			#[test]
			fn clone_after_freeze_shares_the_data_pointer() {
				// A frozen clone must be zero copy, otherwise every batch clone is O(rows).
				let mut c = frozen(ROWS);
				let base = c.data().as_ptr();
				let d = c.clone();
				assert_eq!(d.data().as_ptr(), c.data().as_ptr());
				assert_eq!(d.len(), ROWS);
				assert_eq!(d.data(), &values(ROWS)[..]);
				drop(d);
				assert_eq!(
					c.data_mut().as_ptr(),
					base,
					"dropping the clone must release the allocation"
				);
			}

			#[test]
			fn slice_after_freeze_points_into_the_parent() {
				// A frozen slice must point exactly start elements into the parent allocation.
				let c = frozen(ROWS);
				let s = c.slice(100, 350);
				assert_eq!(s.len(), 250);
				assert_eq!(s.data().as_ptr(), c.data().as_ptr().wrapping_add(100));
				assert!(points_into(c.data(), s.data()));
				assert_eq!(s.data(), &values(ROWS)[100..350]);

				let t = c.take(10);
				assert_eq!(t.data().as_ptr(), c.data().as_ptr());
				assert_eq!(t.data(), &values(ROWS)[..10]);

				let clamped = c.slice(990, 5000);
				assert_eq!(clamped.data(), &values(ROWS)[990..], "slice must clamp to the length");
				assert!(c.slice(2000, 3000).is_empty());
			}

			#[test]
			fn slice_of_a_slice_offsets_from_the_view() {
				// A nested slice must add both offsets, never restart from the parent start.
				let c = frozen(ROWS);
				let s = c.slice(100, 600).slice(50, 60);
				assert_eq!(s.data().as_ptr(), c.data().as_ptr().wrapping_add(150));
				assert_eq!(s.data(), &values(ROWS)[150..160]);
			}

			#[test]
			fn push_on_a_clone_never_shows_through_the_original() {
				// A write through a clone must copy first, otherwise it corrupts the shared block.
				let c = frozen(ROWS);
				let mut d = c.clone();
				d.push(extra());
				assert_eq!(c.len(), ROWS);
				assert_eq!(c.data(), &values(ROWS)[..]);
				assert_eq!(d.len(), ROWS + 1);
				assert_eq!(d.data()[ROWS], extra());
				assert_eq!(&d.data()[..ROWS], &values(ROWS)[..]);
				assert_ne!(d.data().as_ptr(), c.data().as_ptr());
			}

			#[test]
			fn push_on_the_original_never_shows_through_a_clone() {
				// A write through the original must never change what an earlier clone reads.
				let mut c = frozen(ROWS);
				let d = c.clone();
				c.push(extra());
				assert_eq!(d.len(), ROWS);
				assert_eq!(d.data(), &values(ROWS)[..]);
				assert_eq!(c.len(), ROWS + 1);
				assert_eq!(c.data()[ROWS], extra());
			}

			#[test]
			fn push_on_a_slice_never_overwrites_the_parent() {
				// Pushing onto a head slice must never write the parent row right after the view.
				let c = frozen(ROWS);
				let mut s = c.slice(0, 10);
				s.push(extra());
				assert_eq!(s.len(), 11);
				assert_eq!(s.data()[10], extra());
				assert_eq!(&s.data()[..10], &values(ROWS)[..10]);
				assert_eq!(c.data()[10], value(10));
				assert_eq!(c.data(), &values(ROWS)[..]);

				let mut mid = c.slice(500, 510);
				mid.push(extra());
				assert_eq!(c.data()[510], value(510), "a mid slice push must never reach the parent");
				assert_eq!(&mid.data()[..10], &values(ROWS)[500..510]);
			}

			#[test]
			fn unfrozen_clone_is_an_equal_independent_copy() {
				// An owned container must still clone correctly, with writes isolated in both
				// directions.
				let mut c = <$container>::from_vec(values(ROWS));
				let mut d = c.clone();
				assert_eq!(d.data(), c.data());
				assert_ne!(d.data().as_ptr(), c.data().as_ptr());
				c.push(extra());
				assert_eq!(d.len(), ROWS);
				d.push(value(3));
				assert_eq!(c.data()[ROWS], extra());
				assert_eq!(d.data()[ROWS], value(3));
				let s = c.slice(3, 8);
				assert_eq!(s.data(), &values(ROWS)[3..8]);
			}

			#[test]
			fn serialize_of_a_slice_equals_a_fresh_container() {
				// A slice must serialize as exactly its rows, never the parent's rows.
				let c = frozen(ROWS);
				let s = c.slice(100, 350);
				let fresh = <$container>::from_vec(values(ROWS)[100..350].to_vec());
				assert_eq!(to_allocvec(&s).unwrap(), to_allocvec(&fresh).unwrap());
				let json = serde_json::to_string(&s).unwrap();
				assert_eq!(json, serde_json::to_string(&fresh).unwrap());
				let decoded: $container = serde_json::from_str(&json).unwrap();
				assert_eq!(decoded.data(), &values(ROWS)[100..350]);
				assert!(decoded == fresh, "the decoded slice must equal the fresh container");
			}

			#[test]
			fn equality_ignores_how_the_rows_are_stored() {
				// A frozen slice and an owned container with the same rows must compare equal.
				let c = frozen(ROWS);
				let fresh = <$container>::from_vec(values(ROWS)[20..40].to_vec());
				assert!(c.slice(20, 40) == fresh);
				assert!(c.slice(21, 41) != fresh);
			}
		}
	};
}

slice_backed_suite!(int1, NumberContainer<i8>, i8, |i| (i % 100) as i8, i8::MIN);
slice_backed_suite!(int2, NumberContainer<i16>, i16, |i| i as i16 * 3, i16::MIN);
slice_backed_suite!(int4, NumberContainer<i32>, i32, |i| i as i32 * 7, i32::MIN);
slice_backed_suite!(int8, NumberContainer<i64>, i64, |i| i as i64 * 11, i64::MIN);
slice_backed_suite!(int16, NumberContainer<i128>, i128, |i| i as i128 * 13, i128::MIN);
slice_backed_suite!(uint1, NumberContainer<u8>, u8, |i| (i % 200) as u8, u8::MAX);
slice_backed_suite!(uint2, NumberContainer<u16>, u16, |i| i as u16 * 3, u16::MAX);
slice_backed_suite!(uint4, NumberContainer<u32>, u32, |i| i as u32 * 7, u32::MAX);
slice_backed_suite!(uint8, NumberContainer<u64>, u64, |i| i as u64 * 11, u64::MAX);
slice_backed_suite!(uint16, NumberContainer<u128>, u128, |i| i as u128 * 13, u128::MAX);
slice_backed_suite!(float4, NumberContainer<f32>, f32, |i| i as f32 * 0.5, -1.0);
slice_backed_suite!(float8, NumberContainer<f64>, f64, |i| i as f64 * 0.25, -1.0);
slice_backed_suite!(int, NumberContainer<Int>, Int, |i| Int::from_i64(i as i64 * 1_000_003), Int::from_i64(-1));
slice_backed_suite!(uint, NumberContainer<Uint>, Uint, |i| Uint::from_u64(i as u64 * 7), Uint::from_u64(u64::MAX));
slice_backed_suite!(decimal, NumberContainer<Decimal>, Decimal, |i| Decimal::from_i64(i as i64), Decimal::from_i64(-1));
slice_backed_suite!(row_number, RowNumberContainer, RowNumber, |i| RowNumber(i as u64 + 1), RowNumber(u64::MAX));
slice_backed_suite!(
	any,
	AnyContainer,
	Value,
	|i| if i % 3 == 0 {
		Value::Utf8(format!("row-{i}"))
	} else {
		Value::Int8(i as i64)
	},
	Value::Utf8("extra".to_string())
);

macro_rules! array_suite {
	($name:ident, $array:ty, $elem:ty, $gen:expr, $build:path, $typed:path, $ops:ident, $ser:tt, $de:tt) => {
		mod $name {
			use super::*;

			#[derive(Serialize, Deserialize)]
			struct Column(#[serde(serialize_with = $ser, deserialize_with = $de)] $array);

			fn values(n: usize) -> Vec<$elem> {
				let make: fn(usize) -> $elem = $gen;
				(0..n).map(make).collect()
			}

			fn array(n: usize) -> $array {
				$build(values(n))
			}

			#[test]
			fn slice_after_freeze_points_into_the_parent() {
				// Typed and clamped slices must honour the array offset, never read the parent head.
				let c = array(ROWS);
				let s = $ops::slice(&c, 100, 350);
				assert_eq!(s.len(), 250);
				assert_eq!($typed(&s).as_ptr(), $typed(&c).as_ptr().wrapping_add(100));
				assert!(points_into($typed(&c), $typed(&s)));
				assert_eq!($typed(&s), &values(ROWS)[100..350]);

				let t = $ops::take(&c, 10);
				assert_eq!($typed(&t).as_ptr(), $typed(&c).as_ptr());
				assert_eq!($typed(&t), &values(ROWS)[..10]);

				let clamped = $ops::slice(&c, 990, 5000);
				assert_eq!($typed(&clamped), &values(ROWS)[990..], "slice must clamp to the length");
				assert!($ops::slice(&c, 2000, 3000).is_empty());
			}

			#[test]
			fn serialize_of_a_slice_equals_a_fresh_container() {
				// A slice must serialize as exactly its rows, never the parent's rows.
				let c = array(ROWS);
				let s = Column($ops::slice(&c, 100, 350));
				let fresh = Column($build(values(ROWS)[100..350].to_vec()));
				assert_eq!(to_allocvec(&s).unwrap(), to_allocvec(&fresh).unwrap());
				let json = json_to_string(&s).unwrap();
				assert_eq!(json, json_to_string(&fresh).unwrap());
				let decoded: Column = json_from_str(&json).unwrap();
				assert_eq!($typed(&decoded.0), &values(ROWS)[100..350]);
				assert!(
					$typed(&decoded.0) == $typed(&fresh.0),
					"the decoded slice must equal the fresh container"
				);
			}

			#[test]
			fn equality_ignores_how_the_rows_are_stored() {
				// Typed slices must honour the array offset, otherwise equality sees the parent rows.
				let c = array(ROWS);
				let fresh = $build(values(ROWS)[20..40].to_vec());
				assert!($typed(&$ops::slice(&c, 20, 40)) == $typed(&fresh));
				assert!($typed(&$ops::slice(&c, 21, 41)) != $typed(&fresh));
			}
		}
	};
}

array_suite!(
	date,
	Date32Array,
	Date,
	|i| Date::from_days_since_epoch(i as i32).unwrap(),
	temporal_array::date_array,
	temporal_array::dates,
	primitive,
	"temporal_array::serialize_dates",
	"temporal_array::deserialize_dates"
);
array_suite!(
	datetime,
	UInt64Array,
	DateTime,
	|i| DateTime::from_nanos(i as u64 * 1_000_000_007 + 7),
	temporal_array::datetime_array,
	temporal_array::datetimes,
	primitive,
	"temporal_array::serialize_datetimes",
	"temporal_array::deserialize_datetimes"
);
array_suite!(
	time,
	Time64NanosecondArray,
	Time,
	|i| Time::from_nanos_since_midnight(i as u64 * 1_000).unwrap(),
	temporal_array::time_array,
	temporal_array::times,
	primitive,
	"temporal_array::serialize_times",
	"temporal_array::deserialize_times"
);
array_suite!(
	duration,
	IntervalMonthDayNanoArray,
	Duration,
	|i| Duration::new((i % 12) as i32, (i % 28) as i32, i as i64 * 1_000).unwrap(),
	temporal_array::duration_array,
	temporal_array::durations,
	primitive,
	"temporal_array::serialize_durations",
	"temporal_array::deserialize_durations"
);
array_suite!(
	uuid4,
	FixedSizeBinaryArray,
	Uuid4,
	|i| Uuid4(Uuid::from_u128(i as u128 + 1)),
	uuid_array::uuid4_array,
	uuid_array::uuid4s,
	uuid_array,
	"uuid_array::serialize_uuid4s",
	"uuid_array::deserialize_uuid4s"
);
array_suite!(
	uuid7,
	FixedSizeBinaryArray,
	Uuid7,
	|i| Uuid7(Uuid::from_u128((i as u128 + 1) << 64)),
	uuid_array::uuid7_array,
	uuid_array::uuid7s,
	uuid_array,
	"uuid_array::serialize_uuid7s",
	"uuid_array::deserialize_uuid7s"
);
array_suite!(
	identity_id,
	FixedSizeBinaryArray,
	IdentityId,
	|i| IdentityId(Uuid7(Uuid::from_u128(((i as u128 + 1) << 80) | (0x7 << 76) | (0x2 << 62)))),
	uuid_array::identity_id_array,
	uuid_array::identity_ids,
	uuid_array,
	"uuid_array::serialize_identity_ids",
	"uuid_array::deserialize_identity_ids"
);

#[test]
fn a_small_slice_of_a_frozen_container_pins_the_parent_allocation() {
	// A one row slice must keep pointing into the parent allocation after the parent handle is dropped; this pins
	// the whole block.
	let mut parent = NumberContainer::from_vec((0..100_000i64).collect());
	parent.freeze();
	let base = parent.data().as_ptr();
	let mut s = parent.slice(50_000, 50_001);
	drop(parent);
	assert_eq!(s.data().as_ptr(), base.wrapping_add(50_000));
	assert_eq!(s.data(), &[50_000]);
	assert_eq!(
		s.data_mut().as_ptr(),
		base,
		"with the parent dropped the slice must be the sole owner and drain in place"
	);
}

fn bool_pattern(n: usize) -> Vec<bool> {
	(0..n).map(|i| i % 3 == 0 || i % 7 == 0).collect()
}

fn packed_bits_ptr(bits: &BooleanBuffer) -> *const u8 {
	match bitmap::packed_bytes(bits) {
		Cow::Borrowed(bytes) => bytes.as_ptr(),
		Cow::Owned(_) => panic!("a view at bit zero must borrow its packed bytes"),
	}
}

#[derive(Serialize, Deserialize)]
struct BoolColumn(
	#[serde(serialize_with = "bool_array::serialize", deserialize_with = "bool_array::deserialize")] BooleanArray,
);

#[test]
fn bool_clone_after_freeze_shares_the_bits() {
	// A bool array clone must share the packed bytes, never copy them.
	let c = BooleanArray::from(bool_pattern(ROWS));
	let d = c.clone();
	assert_eq!(packed_bits_ptr(d.values()), packed_bits_ptr(c.values()));
	assert_eq!(d.values().iter().collect::<Vec<_>>(), bool_pattern(ROWS));
}

#[test]
fn bool_slice_after_freeze_shares_the_bits_and_reads_at_the_offset() {
	// A bool slice at any bit offset must share the parent bits and read exactly its rows.
	let c = BooleanArray::from(bool_pattern(ROWS));
	for (start, end) in [(0usize, 10usize), (1, 9), (7, 300), (8, 72), (65, 999)] {
		let s = bool_array::slice(&c, start, end);
		assert_eq!(
			s.values().inner().as_ptr(),
			c.values().inner().as_ptr(),
			"slice {start}..{end} must share the parent bits"
		);
		assert_eq!(s.len(), end - start);
		assert_eq!(
			s.values().iter().collect::<Vec<_>>(),
			&bool_pattern(ROWS)[start..end],
			"slice {start}..{end}"
		);
		for i in 0..s.len() {
			assert_eq!(
				bool_array::get_value(&s, i),
				Value::Boolean(bool_pattern(ROWS)[start + i]),
				"slice {start}..{end} row {i}"
			);
		}
		assert_eq!(
			bool_array::get_value(&s, s.len()),
			Value::none_of(ValueType::Boolean),
			"reading past the slice must be none even when the parent has the row"
		);
	}
	let t = bool_array::take(&c, 100);
	assert_eq!(t.values().inner().as_ptr(), c.values().inner().as_ptr());
}

#[test]
fn bool_serialize_of_a_slice_equals_a_fresh_container() {
	// A bool slice at a non byte offset must serialize exactly like a fresh array of its rows.
	let c = BooleanArray::from(bool_pattern(ROWS));
	let s = BoolColumn(bool_array::slice(&c, 13, 400));
	let fresh = BoolColumn(BooleanArray::from(bool_pattern(ROWS)[13..400].to_vec()));
	assert_eq!(to_allocvec(&s).unwrap(), to_allocvec(&fresh).unwrap());
	let decoded: BoolColumn = from_bytes(&to_allocvec(&s).unwrap()).unwrap();
	assert_eq!(decoded.0.values().iter().collect::<Vec<_>>(), &bool_pattern(ROWS)[13..400]);
	assert!(s.0 == fresh.0);
}

fn strings(n: usize) -> Vec<String> {
	(0..n).map(|i| format!("row-{i}-{}", "x".repeat(i % 5))).collect()
}

fn utf8_array(n: usize) -> LargeStringArray {
	LargeStringArray::from(strings(n))
}

#[derive(Serialize, Deserialize)]
struct Utf8Column(
	#[serde(serialize_with = "varlen_array::serialize", deserialize_with = "varlen_array::deserialize_utf8")]
	LargeStringArray,
);

fn byte_start(rows: &[String], row: usize) -> usize {
	rows[..row].iter().map(|s| s.len()).sum()
}

#[track_caller]
fn assert_compact_parts_match<T: ByteArrayType<Offset = i64>>(array: &GenericByteArray<T>, rows: &[&[u8]]) {
	let (data, offsets) = compact_parts(array);
	assert_eq!(offsets.len(), rows.len() + 1);
	assert_eq!(offsets[0], 0, "compact offsets must be rebased to zero");
	assert_eq!(*offsets.last().unwrap() as usize, data.len(), "the last compact offset must equal the byte length");
	for (i, row) in rows.iter().enumerate() {
		assert_eq!(&data[offsets[i] as usize..offsets[i + 1] as usize], *row, "compact row {i}");
	}
}

#[test]
fn utf8_slice_after_freeze_references_the_parent_bytes() {
	// A frozen utf8 slice must reference the parent bytes at exactly its first row's byte, never copy them.
	let rows = strings(ROWS);
	let c = utf8_array(ROWS);
	let (parent_data, _) = compact_parts(&c);
	for (start, end) in [(0usize, 10usize), (1, 2), (100, 350), (999, 1000)] {
		let s = slice(&c, start, end);
		assert_eq!(s.len(), end - start);
		for i in 0..s.len() {
			assert_eq!(get(&s, i), Some(rows[start + i].as_str()), "slice {start}..{end} row {i}");
		}
		assert_eq!(get(&s, s.len()), None);
		let (data, offsets) = compact_parts(&s);
		assert_eq!(
			data.as_ptr(),
			parent_data.as_ptr().wrapping_add(byte_start(&rows, start)),
			"slice {start}..{end}"
		);
		assert!(points_into(parent_data, data), "slice {start}..{end} bytes must lie inside the parent");
		assert_eq!(data.len(), byte_start(&rows, end) - byte_start(&rows, start));
		if start == 0 {
			assert!(matches!(offsets, Cow::Borrowed(_)), "a head slice must borrow its offsets");
		} else {
			assert!(
				matches!(offsets, Cow::Owned(_)),
				"a slice past row zero keeps absolute offsets, so rebasing must copy"
			);
		}
		let expected: Vec<&[u8]> = rows[start..end].iter().map(|s| s.as_bytes()).collect();
		assert_compact_parts_match(&s, &expected);
	}
	assert!(slice(&c, 5, 5).is_empty());
	assert!(slice(&c, 2000, 3000).is_empty());
	assert_eq!(slice(&c, 990, 5000).len(), 10, "utf8 slice must clamp to the length");
}

#[test]
fn utf8_slice_of_a_slice_keeps_absolute_offsets() {
	// A nested utf8 slice must resolve rows against the shared buffer, never against the outer view start.
	let rows = strings(ROWS);
	let c = utf8_array(ROWS);
	let (parent_data, _) = compact_parts(&c);
	let s = slice(&slice(&c, 100, 600), 50, 60);
	assert_eq!((0..s.len()).map(|i| s.value(i)).collect::<Vec<_>>(), rows[150..160].to_vec());
	let (data, _) = compact_parts(&s);
	assert_eq!(data.as_ptr(), parent_data.as_ptr().wrapping_add(byte_start(&rows, 150)));
}

#[test]
fn utf8_serialize_of_a_slice_equals_a_fresh_container() {
	// A utf8 slice must serialize exactly its rows, never the whole shared buffer.
	let rows = strings(ROWS);
	let c = utf8_array(ROWS);
	let s = slice(&c, 100, 350);
	let fresh = LargeStringArray::from(rows[100..350].to_vec());
	let encoded = to_allocvec(&Utf8Column(s.clone())).unwrap();
	assert_eq!(encoded, to_allocvec(&Utf8Column(fresh.clone())).unwrap());
	let decoded: Utf8Column = from_bytes(&encoded).unwrap();
	assert_eq!((0..decoded.0.len()).map(|i| decoded.0.value(i)).collect::<Vec<_>>(), rows[100..350].to_vec());
	assert!(equals(&s, &fresh));
}

#[test]
fn a_small_utf8_slice_pins_the_whole_parent_buffer() {
	// A one row utf8 slice must keep referencing the parent buffer after the parent handle is dropped; this pins
	// the whole block.
	let rows = strings(100_000);
	let c = utf8_array(100_000);
	let base = compact_parts(&c).0.as_ptr();
	let s = slice(&c, 50_000, 50_001);
	drop(c);
	let (data, offsets) = compact_parts(&s);
	assert_eq!(data.as_ptr(), base.wrapping_add(byte_start(&rows, 50_000)));
	assert_eq!(data, rows[50_000].as_bytes());
	assert_eq!(&offsets[..], &[0, rows[50_000].len() as i64]);
	assert_eq!(get(&s, 0), Some(rows[50_000].as_str()));
}

fn blobs(n: usize) -> Vec<Vec<u8>> {
	(0..n).map(|i| (0..(i % 9) as u8).map(|b| b.wrapping_mul(i as u8)).collect()).collect()
}

#[derive(Serialize)]
struct BlobColumn(#[serde(serialize_with = "varlen_array::serialize")] LargeBinaryArray);

#[test]
fn blob_slice_after_freeze_references_the_parent_bytes() {
	// A frozen blob slice must share the parent bytes and keep empty rows empty.
	let rows = blobs(ROWS);
	let c = blob_array(&rows.iter().cloned().map(Blob::new).collect::<Vec<_>>());
	let (parent_data, _) = compact_parts(&c);
	let d = c.clone();
	assert_eq!(compact_parts(&d).0.as_ptr(), parent_data.as_ptr());
	let s = slice(&c, 100, 350);
	let start: usize = rows[..100].iter().map(|b| b.len()).sum();
	let (data, _) = compact_parts(&s);
	assert_eq!(data.as_ptr(), parent_data.as_ptr().wrapping_add(start));
	for i in 0..s.len() {
		assert_eq!(get(&s, i), Some(rows[100 + i].as_slice()), "blob row {i}");
	}
	let expected: Vec<&[u8]> = rows[100..350].iter().map(|b| b.as_slice()).collect();
	assert_compact_parts_match(&s, &expected);

	let fresh = blob_array(&rows[100..350].iter().cloned().map(Blob::new).collect::<Vec<_>>());
	assert_eq!(to_allocvec(&BlobColumn(s.clone())).unwrap(), to_allocvec(&BlobColumn(fresh.clone())).unwrap());
	assert!(equals(&s, &fresh));
}

#[test]
fn varlen_compact_parts_borrow_when_offsets_start_at_zero() {
	// compact_parts must borrow unsliced offsets and rebase sliced ones exactly to zero.
	let rows: Vec<Vec<u8>> = blobs(64);
	let v = LargeBinaryArray::from_iter_values(rows.iter().map(|r| r.as_slice()));
	let (data, offsets) = compact_parts(&v);
	assert!(matches!(offsets, Cow::Borrowed(_)));
	assert_eq!(data.len(), rows.iter().map(|r| r.len()).sum::<usize>());
	let base = compact_parts(&v).0.as_ptr();
	let s = slice(&v, 10, 20);
	let (data, offsets) = compact_parts(&s);
	assert!(matches!(offsets, Cow::Owned(_)));
	let start: usize = rows[..10].iter().map(|r| r.len()).sum();
	assert_eq!(data.as_ptr(), base.wrapping_add(start));
	let expected: Vec<&[u8]> = rows[10..20].iter().map(|r| r.as_slice()).collect();
	assert_compact_parts_match(&s, &expected);
	for (i, row) in expected.iter().enumerate() {
		assert_eq!(get(&s, i), Some(*row));
	}
	assert_eq!(get(&s, 10), None);

	let empty = slice(&v, 7, 7);
	let (data, offsets) = compact_parts(&empty);
	assert!(data.is_empty());
	assert_eq!(&offsets[..], &[0]);
}
