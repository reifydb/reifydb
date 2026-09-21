// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use postcard::to_allocvec;
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, row::RowNumberContainer,
			temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer, varlen::VarlenContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
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
slice_backed_suite!(
	date,
	TemporalContainer<Date>,
	Date,
	|i| Date::from_days_since_epoch(i as i32).unwrap(),
	Date::from_days_since_epoch(-1).unwrap()
);
slice_backed_suite!(
	datetime,
	TemporalContainer<DateTime>,
	DateTime,
	|i| DateTime::from_nanos(i as u64 * 1_000_000_007 + 7),
	DateTime::from_nanos(1)
);
slice_backed_suite!(
	time,
	TemporalContainer<Time>,
	Time,
	|i| Time::from_nanos_since_midnight(i as u64 * 1_000).unwrap(),
	Time::from_nanos_since_midnight(86_399_999_999_999).unwrap()
);
slice_backed_suite!(
	duration,
	TemporalContainer<Duration>,
	Duration,
	|i| Duration::new((i % 12) as i32, (i % 28) as i32, i as i64 * 1_000).unwrap(),
	Duration::new(-7, -3, -5).unwrap()
);
slice_backed_suite!(
	uuid4,
	UuidContainer<Uuid4>,
	Uuid4,
	|i| Uuid4(Uuid::from_u128(i as u128 + 1)),
	Uuid4(Uuid::from_u128(u128::MAX))
);
slice_backed_suite!(
	uuid7,
	UuidContainer<Uuid7>,
	Uuid7,
	|i| Uuid7(Uuid::from_u128((i as u128 + 1) << 64)),
	Uuid7(Uuid::from_u128(u128::MAX))
);
slice_backed_suite!(
	identity_id,
	IdentityIdContainer,
	IdentityId,
	|i| IdentityId(Uuid7(Uuid::from_u128(((i as u128 + 1) << 80) | (0x7 << 76) | (0x2 << 62)))),
	IdentityId::root()
);
slice_backed_suite!(row_number, RowNumberContainer, RowNumber, |i| RowNumber(i as u64 + 1), RowNumber(u64::MAX));
slice_backed_suite!(
	dictionary,
	DictionaryContainer,
	DictionaryEntryId,
	|i| DictionaryEntryId::U4(i as u32),
	DictionaryEntryId::U4(u32::MAX)
);
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

fn packed_ptr(bits: &BitVec) -> *const u8 {
	match bits.to_packed_bytes() {
		Cow::Borrowed(bytes) => bytes.as_ptr(),
		Cow::Owned(_) => panic!("a view at bit zero must borrow its packed bytes"),
	}
}

#[test]
fn bool_clone_after_freeze_shares_the_bits() {
	// A frozen bool clone must share the packed bytes, never copy them.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let d = c.clone();
	assert_eq!(packed_ptr(d.data()), packed_ptr(c.data()));
	assert_eq!(d.data().to_vec(), bool_pattern(ROWS));
}

#[test]
fn bool_slice_after_freeze_shares_the_bits_and_reads_at_the_offset() {
	// A bool slice at any bit offset must share the parent bits and read exactly its rows.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	for (start, end) in [(0usize, 10usize), (1, 9), (7, 300), (8, 72), (65, 999)] {
		let s = c.slice(start, end);
		assert_eq!(s.data().capacity(), c.data().capacity(), "slice {start}..{end} must share the parent bits");
		assert_eq!(s.len(), end - start);
		assert_eq!(s.data().to_vec(), &bool_pattern(ROWS)[start..end], "slice {start}..{end}");
		for i in 0..s.len() {
			assert_eq!(s.get(i), Some(bool_pattern(ROWS)[start + i]), "slice {start}..{end} row {i}");
		}
		assert_eq!(
			s.get(s.len()),
			None,
			"reading past the slice must be none even when the parent has the row"
		);
	}
	let t = c.take(100);
	assert_eq!(packed_ptr(t.data()), packed_ptr(c.data()));
}

#[test]
fn bool_writes_never_leak_between_handles() {
	// A bool write through any handle must never show through another handle.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let mut d = c.clone();
	d.push(true);
	assert_eq!(c.len(), ROWS);
	assert_eq!(d.len(), ROWS + 1);
	assert_eq!(c.data().to_vec(), bool_pattern(ROWS));

	let mut head = c.slice(0, 10);
	let next = !bool_pattern(ROWS)[10];
	head.push(next);
	assert_eq!(head.get(10), Some(next));
	assert_eq!(c.get(10), Some(bool_pattern(ROWS)[10]), "a head slice push must never write the parent bit");

	let mut mid = c.slice(9, 20);
	mid.push(!bool_pattern(ROWS)[20]);
	assert_eq!(c.data().to_vec(), bool_pattern(ROWS));

	c.push(false);
	assert_eq!(d.data().to_vec()[..ROWS], bool_pattern(ROWS)[..]);
	assert_eq!(d.get(ROWS), Some(true));
}

#[test]
fn bool_unfrozen_clone_is_an_equal_independent_copy() {
	// An owned bool container must still clone correctly with writes isolated.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	let d = c.clone();
	c.push(true);
	assert_eq!(d.len(), ROWS);
	assert_eq!(d.data().to_vec(), bool_pattern(ROWS));
}

#[test]
fn bool_serialize_of_a_slice_equals_a_fresh_container() {
	// A bool slice at a non byte offset must serialize exactly like a fresh container of its rows.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let s = c.slice(13, 400);
	let fresh = BoolContainer::from_vec(bool_pattern(ROWS)[13..400].to_vec());
	assert_eq!(to_allocvec(&s).unwrap(), to_allocvec(&fresh).unwrap());
	let decoded: BoolContainer = postcard::from_bytes(&to_allocvec(&s).unwrap()).unwrap();
	assert_eq!(decoded.data().to_vec(), &bool_pattern(ROWS)[13..400]);
	assert!(s == fresh);
}

fn strings(n: usize) -> Vec<String> {
	(0..n).map(|i| format!("row-{i}-{}", "x".repeat(i % 5))).collect()
}

fn frozen_utf8(n: usize) -> Utf8Container {
	let mut c = Utf8Container::from_vec(strings(n));
	c.freeze();
	c
}

fn byte_start(rows: &[String], row: usize) -> usize {
	rows[..row].iter().map(|s| s.len()).sum()
}

#[track_caller]
fn assert_compact_parts_match(inner: &VarlenContainer, rows: &[&[u8]]) {
	let (data, offsets) = inner.compact_parts();
	assert_eq!(offsets.len(), rows.len() + 1);
	assert_eq!(offsets[0], 0, "compact offsets must be rebased to zero");
	assert_eq!(*offsets.last().unwrap() as usize, data.len(), "the last compact offset must equal the byte length");
	for (i, row) in rows.iter().enumerate() {
		assert_eq!(&data[offsets[i] as usize..offsets[i + 1] as usize], *row, "compact row {i}");
	}
}

#[test]
fn utf8_clone_after_freeze_shares_bytes_and_offsets() {
	// A frozen utf8 clone must share both the byte buffer and the offsets.
	let c = frozen_utf8(ROWS);
	let d = c.clone();
	let (c_data, c_offsets) = c.inner().compact_parts();
	let (d_data, d_offsets) = d.inner().compact_parts();
	assert_eq!(d_data.as_ptr(), c_data.as_ptr());
	assert_eq!(d_data.len(), c_data.len());
	assert!(matches!(c_offsets, Cow::Borrowed(_)), "unsliced offsets must already start at zero");
	assert_eq!(d_offsets.as_ptr(), c_offsets.as_ptr());
	assert_eq!(d.iter_str().collect::<Vec<_>>(), strings(ROWS));
}

#[test]
fn utf8_slice_after_freeze_references_the_parent_bytes() {
	// A frozen utf8 slice must reference the parent bytes at exactly its first row's byte, never copy them.
	let rows = strings(ROWS);
	let c = frozen_utf8(ROWS);
	let (parent_data, _) = c.inner().compact_parts();
	for (start, end) in [(0usize, 10usize), (1, 2), (100, 350), (999, 1000)] {
		let s = c.slice(start, end);
		assert_eq!(s.len(), end - start);
		for i in 0..s.len() {
			assert_eq!(s.get(i), Some(rows[start + i].as_str()), "slice {start}..{end} row {i}");
		}
		assert_eq!(s.get(s.len()), None);
		let (data, offsets) = s.inner().compact_parts();
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
		assert_compact_parts_match(s.inner(), &expected);
	}
	assert!(c.slice(5, 5).is_empty());
	assert!(c.slice(2000, 3000).is_empty());
	assert_eq!(c.slice(990, 5000).len(), 10, "utf8 slice must clamp to the length");
}

#[test]
fn utf8_slice_of_a_slice_keeps_absolute_offsets() {
	// A nested utf8 slice must resolve rows against the shared buffer, never against the outer view start.
	let rows = strings(ROWS);
	let c = frozen_utf8(ROWS);
	let (parent_data, _) = c.inner().compact_parts();
	let s = c.slice(100, 600).slice(50, 60);
	assert_eq!(s.iter_str().collect::<Vec<_>>(), rows[150..160].to_vec());
	let (data, _) = s.inner().compact_parts();
	assert_eq!(data.as_ptr(), parent_data.as_ptr().wrapping_add(byte_start(&rows, 150)));
}

#[test]
fn utf8_writes_never_leak_between_handles() {
	// A utf8 push through a clone or a slice must never change the rows another handle reads.
	let rows = strings(ROWS);
	let c = frozen_utf8(ROWS);
	let mut d = c.clone();
	d.push_str("extra");
	assert_eq!(c.len(), ROWS);
	assert_eq!(d.len(), ROWS + 1);
	assert_eq!(d.get(ROWS), Some("extra"));
	assert_eq!(c.iter_str().collect::<Vec<_>>(), rows);

	let mut head = c.slice(0, 10);
	head.push_str("head");
	assert_eq!(head.get(10), Some("head"));
	assert_eq!(head.iter_str().take(10).collect::<Vec<_>>(), rows[..10].to_vec());
	assert_eq!(c.get(10), Some(rows[10].as_str()), "a head slice push must never write the parent row");

	let mut mid = c.slice(500, 505);
	mid.push_str("mid");
	assert_eq!(
		mid.iter_str().collect::<Vec<_>>(),
		vec![&rows[500], &rows[501], &rows[502], &rows[503], &rows[504], "mid"]
	);
	assert_compact_parts_match(
		mid.inner(),
		&[
			rows[500].as_bytes(),
			rows[501].as_bytes(),
			rows[502].as_bytes(),
			rows[503].as_bytes(),
			rows[504].as_bytes(),
			b"mid",
		],
	);
	assert_eq!(c.iter_str().collect::<Vec<_>>(), rows, "the parent must never see slice writes");

	let mut owner = frozen_utf8(ROWS);
	let snapshot = owner.clone();
	owner.push_str("tail");
	assert_eq!(snapshot.len(), ROWS);
	assert_eq!(snapshot.iter_str().collect::<Vec<_>>(), rows);
}

#[test]
fn utf8_unfrozen_clone_and_slice_are_correct() {
	// An owned utf8 container must still clone and slice correctly with writes isolated.
	let rows = strings(ROWS);
	let mut c = Utf8Container::from_vec(rows.clone());
	let d = c.clone();
	c.push_str("extra");
	assert_eq!(d.len(), ROWS);
	assert_eq!(d.iter_str().collect::<Vec<_>>(), rows);
	let s = c.slice(3, 8);
	assert_eq!(s.iter_str().collect::<Vec<_>>(), rows[3..8].to_vec());
	let expected: Vec<&[u8]> = rows[3..8].iter().map(|s| s.as_bytes()).collect();
	assert_compact_parts_match(s.inner(), &expected);
}

#[test]
fn utf8_serialize_of_a_slice_equals_a_fresh_container() {
	// A utf8 slice must serialize exactly its rows, never the whole shared buffer.
	let rows = strings(ROWS);
	let c = frozen_utf8(ROWS);
	let s = c.slice(100, 350);
	let fresh = Utf8Container::from_vec(rows[100..350].to_vec());
	let encoded = to_allocvec(&s).unwrap();
	assert_eq!(encoded, to_allocvec(&fresh).unwrap());
	let decoded: Utf8Container = postcard::from_bytes(&encoded).unwrap();
	assert_eq!(decoded.iter_str().collect::<Vec<_>>(), rows[100..350].to_vec());
	assert!(s == fresh);
}

#[test]
fn a_small_utf8_slice_pins_the_whole_parent_buffer() {
	// A one row utf8 slice must keep referencing the parent buffer after the parent handle is dropped; this pins
	// the whole block.
	let rows = strings(100_000);
	let c = frozen_utf8(100_000);
	let base = c.inner().compact_parts().0.as_ptr();
	let s = c.slice(50_000, 50_001);
	drop(c);
	let (data, offsets) = s.inner().compact_parts();
	assert_eq!(data.as_ptr(), base.wrapping_add(byte_start(&rows, 50_000)));
	assert_eq!(data, rows[50_000].as_bytes());
	assert_eq!(&offsets[..], &[0, rows[50_000].len() as u64]);
	assert_eq!(s.get(0), Some(rows[50_000].as_str()));
}

fn blobs(n: usize) -> Vec<Vec<u8>> {
	(0..n).map(|i| (0..(i % 9) as u8).map(|b| b.wrapping_mul(i as u8)).collect()).collect()
}

#[test]
fn blob_slice_after_freeze_references_the_parent_bytes() {
	// A frozen blob slice must share the parent bytes and keep empty rows empty.
	let rows = blobs(ROWS);
	let mut c = BlobContainer::from_vec(rows.iter().cloned().map(Blob::new).collect());
	c.freeze();
	let (parent_data, _) = c.inner().compact_parts();
	let d = c.clone();
	assert_eq!(d.inner().compact_parts().0.as_ptr(), parent_data.as_ptr());
	let s = c.slice(100, 350);
	let start: usize = rows[..100].iter().map(|b| b.len()).sum();
	let (data, _) = s.inner().compact_parts();
	assert_eq!(data.as_ptr(), parent_data.as_ptr().wrapping_add(start));
	for i in 0..s.len() {
		assert_eq!(s.get(i), Some(rows[100 + i].as_slice()), "blob row {i}");
	}
	let expected: Vec<&[u8]> = rows[100..350].iter().map(|b| b.as_slice()).collect();
	assert_compact_parts_match(s.inner(), &expected);

	let mut w = s.clone();
	w.push_bytes(&[0xAA, 0xBB]);
	assert_eq!(w.get(250), Some(&[0xAA, 0xBB][..]));
	assert_eq!(s.len(), 250, "a push through a clone of the slice must never change the slice");
	assert_eq!(c.get(350), Some(rows[350].as_slice()), "a slice push must never write the parent row");

	let fresh = BlobContainer::from_vec(rows[100..350].iter().cloned().map(Blob::new).collect());
	assert_eq!(to_allocvec(&s).unwrap(), to_allocvec(&fresh).unwrap());
	assert!(s == fresh);
}

#[test]
fn varlen_compact_parts_borrow_when_offsets_start_at_zero() {
	// compact_parts must borrow unsliced offsets and rebase sliced ones exactly to zero.
	let rows: Vec<Vec<u8>> = blobs(64);
	let mut v = VarlenContainer::from_byte_slices(rows.iter().map(|r| r.as_slice()));
	let (data, offsets) = v.compact_parts();
	assert!(matches!(offsets, Cow::Borrowed(_)));
	assert_eq!(data.len(), rows.iter().map(|r| r.len()).sum::<usize>());
	v.freeze();
	let base = v.compact_parts().0.as_ptr();
	let s = v.slice(10, 20);
	let (data, offsets) = s.compact_parts();
	assert!(matches!(offsets, Cow::Owned(_)));
	let start: usize = rows[..10].iter().map(|r| r.len()).sum();
	assert_eq!(data.as_ptr(), base.wrapping_add(start));
	let expected: Vec<&[u8]> = rows[10..20].iter().map(|r| r.as_slice()).collect();
	assert_compact_parts_match(&s, &expected);
	for (i, row) in expected.iter().enumerate() {
		assert_eq!(s.get_bytes(i), Some(*row));
	}
	assert_eq!(s.get_bytes(10), None);

	let empty = v.slice(7, 7);
	let (data, offsets) = empty.compact_parts();
	assert!(data.is_empty());
	assert_eq!(&offsets[..], &[0]);
}
