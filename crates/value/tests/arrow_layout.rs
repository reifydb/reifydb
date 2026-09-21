// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	mem::{align_of, size_of, size_of_val},
	ptr::NonNull,
	sync::Arc,
};

use arrow_buffer::{BooleanBuffer, Buffer, IntervalMonthDayNano, ScalarBuffer, alloc::Allocation};
use reifydb_value::value::{
	blob::Blob,
	container::{
		blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer, number::NumberContainer,
		row::RowNumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
	},
	date::Date,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	row_number::RowNumber,
	time::Time,
	uuid::{Uuid4, Uuid7},
};
use uuid::Uuid;

const ROWS: usize = 1000;

fn export<C, T>(container: C, data: fn(&C) -> &[T]) -> (Buffer, *const T)
where
	C: Allocation + 'static,
{
	let owner = Arc::new(container);
	let values = data(&owner);
	let ptr = NonNull::from(values).cast::<u8>();
	let len = size_of_val(values);
	// SAFETY: ptr addresses exactly len initialized bytes inside owner, and the Arc keeps that frozen allocation
	// alive and unmoved while the buffer exists.
	let buffer = unsafe { Buffer::from_custom_allocation(ptr, len, owner) };
	(buffer, ptr.as_ptr().cast::<T>().cast_const())
}

macro_rules! fixed_width_export {
	($name:ident, $container:ty, $elem:ty, $native:ty, $gen:expr, $to_native:expr) => {
		mod $name {
			use super::*;

			fn values(n: usize) -> Vec<$elem> {
				let make: fn(usize) -> $elem = $gen;
				(0..n).map(make).collect()
			}

			fn native(values: &[$elem]) -> Vec<$native> {
				let convert: fn(&$elem) -> $native = $to_native;
				values.iter().map(convert).collect()
			}

			#[test]
			fn export_aliases_the_frozen_data() {
				// The arrow buffer must alias the frozen rows and decode to exactly their native
				// values, otherwise the export is a copy or the layout drifted.
				let mut c = <$container>::from_vec(values(ROWS));
				c.freeze();
				let base = c.data().as_ptr();
				let (buffer, ptr) = export(c.clone(), <$container>::data);
				assert_eq!(ptr, base);
				assert_eq!(buffer.as_ptr(), base.cast::<u8>());
				assert_eq!(buffer.len(), ROWS * size_of::<$elem>());
				let scalars = ScalarBuffer::<$native>::from(buffer);
				assert_eq!(&scalars[..], &native(&values(ROWS))[..]);
				drop(scalars);
				assert_eq!(c.data_mut().as_ptr(), base, "dropping the buffer must release its handle");

				c.freeze();
				let (buffer, _) = export(c.clone(), <$container>::data);
				c.data_mut().reverse();
				assert_ne!(
					c.data().as_ptr(),
					base,
					"the exported buffer must hold a handle on the allocation"
				);
				let scalars = ScalarBuffer::<$native>::from(buffer);
				assert_eq!(
					&scalars[..],
					&native(&values(ROWS))[..],
					"a container write must never reach exported rows"
				);
			}

			#[test]
			fn export_of_a_slice_aliases_the_parent_and_outlives_every_handle() {
				// An exported slice must start exactly at its first row inside the parent and stay
				// readable after every container handle is gone.
				let mut c = <$container>::from_vec(values(ROWS));
				c.freeze();
				let base = c.data().as_ptr();
				let (buffer, ptr) = export(c.slice(100, 350), <$container>::data);
				drop(c);
				assert_eq!(ptr, base.wrapping_add(100));
				assert_eq!(buffer.len(), 250 * size_of::<$elem>());
				let scalars = ScalarBuffer::<$native>::from(buffer);
				assert_eq!(&scalars[..], &native(&values(ROWS)[100..350])[..]);
			}
		}
	};
}

fixed_width_export!(int1, NumberContainer<i8>, i8, i8, |i| (i % 100) as i8 - 50, |v| *v);
fixed_width_export!(int2, NumberContainer<i16>, i16, i16, |i| i as i16 * -3, |v| *v);
fixed_width_export!(int4, NumberContainer<i32>, i32, i32, |i| i as i32 * -7, |v| *v);
fixed_width_export!(int8, NumberContainer<i64>, i64, i64, |i| i as i64 * -11, |v| *v);
fixed_width_export!(int16, NumberContainer<i128>, i128, i128, |i| i as i128 * -13, |v| *v);
fixed_width_export!(uint1, NumberContainer<u8>, u8, u8, |i| (i % 256) as u8, |v| *v);
fixed_width_export!(uint2, NumberContainer<u16>, u16, u16, |i| i as u16 * 3, |v| *v);
fixed_width_export!(uint4, NumberContainer<u32>, u32, u32, |i| i as u32 * 7, |v| *v);
fixed_width_export!(uint8, NumberContainer<u64>, u64, u64, |i| u64::MAX - i as u64, |v| *v);
fixed_width_export!(uint16, NumberContainer<u128>, u128, u128, |i| u128::MAX - i as u128, |v| *v);
fixed_width_export!(float4, NumberContainer<f32>, f32, u32, |i| i as f32 * -0.5, |v| v.to_bits());
fixed_width_export!(float8, NumberContainer<f64>, f64, u64, |i| i as f64 * -0.25, |v| v.to_bits());
fixed_width_export!(
	date,
	TemporalContainer<Date>,
	Date,
	i32,
	|i| Date::from_days_since_epoch(i as i32 * 37 - 18_000).unwrap(),
	|d| d.to_days_since_epoch()
);
fixed_width_export!(
	time,
	TemporalContainer<Time>,
	Time,
	i64,
	|i| Time::from_nanos_since_midnight(i as u64 * 86_000_000_007).unwrap(),
	|t| t.to_nanos_since_midnight() as i64
);
fixed_width_export!(
	datetime,
	TemporalContainer<DateTime>,
	DateTime,
	i64,
	|i| DateTime::from_nanos(i as u64 * 1_000_000_000_007 + 3),
	|d| d.to_epoch_nanos().unwrap()
);
fixed_width_export!(
	duration,
	TemporalContainer<Duration>,
	Duration,
	IntervalMonthDayNano,
	|i| if i % 2 == 0 {
		Duration::new(i as i32, i as i32 * 3, i as i64 * 1_000_003).unwrap()
	} else {
		Duration::new(-(i as i32), -(i as i32), -(i as i64)).unwrap()
	},
	|d| IntervalMonthDayNano::new(d.get_months(), d.get_days(), d.get_nanos())
);
fixed_width_export!(row_number, RowNumberContainer, RowNumber, u64, |i| RowNumber(i as u64 * 5 + 1), |r| r.0);

macro_rules! id_export {
	($name:ident, $container:ty, $elem:ty, $gen:expr, $to_bytes:expr) => {
		mod $name {
			use super::*;

			fn values(n: usize) -> Vec<$elem> {
				let make: fn(usize) -> $elem = $gen;
				(0..n).map(make).collect()
			}

			fn bytes(values: &[$elem]) -> Vec<u8> {
				let convert: fn(&$elem) -> [u8; 16] = $to_bytes;
				values.iter().flat_map(convert).collect()
			}

			#[test]
			fn export_aliases_the_frozen_data_as_sixteen_byte_rows() {
				// Each id must occupy exactly its 16 uuid bytes in order, otherwise a fixed size binary
				// export reads the wrong rows.
				let mut c = <$container>::from_vec(values(ROWS));
				c.freeze();
				let base = c.data().as_ptr();
				let (buffer, ptr) = export(c.clone(), <$container>::data);
				assert_eq!(ptr, base);
				assert_eq!(buffer.len(), ROWS * 16);
				assert_eq!(buffer.as_slice(), &bytes(&values(ROWS))[..]);
				for (i, row) in buffer.as_slice().chunks_exact(16).enumerate().step_by(97) {
					let convert: fn(&$elem) -> [u8; 16] = $to_bytes;
					assert_eq!(row, &convert(&values(ROWS)[i])[..], "row {i}");
				}
			}

			#[test]
			fn export_of_a_slice_aliases_the_parent_and_outlives_every_handle() {
				// An exported id slice must start exactly 16 bytes per row into the parent and stay
				// readable after the handles are gone.
				let mut c = <$container>::from_vec(values(ROWS));
				c.freeze();
				let base = c.data().as_ptr();
				let (buffer, ptr) = export(c.slice(100, 350), <$container>::data);
				drop(c);
				assert_eq!(ptr, base.wrapping_add(100));
				assert_eq!(buffer.as_ptr(), base.cast::<u8>().wrapping_add(100 * 16));
				assert_eq!(buffer.as_slice(), &bytes(&values(ROWS)[100..350])[..]);
			}
		}
	};
}

id_export!(
	uuid4,
	UuidContainer<Uuid4>,
	Uuid4,
	|i| Uuid4(Uuid::from_u128(0x0123_4567_89ab_cdef_u128 << 64 | i as u128)),
	|u| *u.0.as_bytes()
);
id_export!(uuid7, UuidContainer<Uuid7>, Uuid7, |i| Uuid7(Uuid::from_u128((i as u128 + 1) << 72 | 0xfe)), |u| *u
	.0
	.as_bytes());
id_export!(
	identity_id,
	IdentityIdContainer,
	IdentityId,
	|i| IdentityId(Uuid7(Uuid::from_u128(u128::MAX - i as u128))),
	|u| *u.0.0.as_bytes()
);

fn strings(n: usize) -> Vec<String> {
	(0..n).map(|i| format!("{i}-{}", "ab".repeat(i % 4))).collect()
}

fn utf8_bytes(c: &Utf8Container) -> &[u8] {
	c.inner().compact_parts().0
}

fn utf8_offsets(c: &Utf8Container) -> &[u64] {
	match c.inner().compact_parts().1 {
		Cow::Borrowed(offsets) => offsets,
		Cow::Owned(_) => panic!("unsliced offsets must already start at zero"),
	}
}

fn expected_offsets(rows: &[&[u8]]) -> Vec<i64> {
	let mut offsets = vec![0i64];
	for row in rows {
		offsets.push(offsets.last().unwrap() + row.len() as i64);
	}
	offsets
}

#[test]
fn utf8_export_aliases_bytes_and_offsets_as_a_large_utf8_layout() {
	// A frozen utf8 column must export both buffers without copying and decode as arrow large utf8 rows.
	let rows = strings(ROWS);
	let mut c = Utf8Container::from_vec(rows.clone());
	c.freeze();
	let data_ptr = utf8_bytes(&c).as_ptr();
	let offsets_ptr = utf8_offsets(&c).as_ptr();
	let (data, exported_data_ptr) = export(c.clone(), utf8_bytes);
	let (offsets, exported_offsets_ptr) = export(c.clone(), utf8_offsets);
	assert_eq!(exported_data_ptr, data_ptr);
	assert_eq!(exported_offsets_ptr, offsets_ptr);
	let offsets = ScalarBuffer::<i64>::from(offsets);
	let refs: Vec<&[u8]> = rows.iter().map(|s| s.as_bytes()).collect();
	assert_eq!(&offsets[..], &expected_offsets(&refs)[..]);
	for (i, row) in rows.iter().enumerate() {
		let bytes = &data.as_slice()[offsets[i] as usize..offsets[i + 1] as usize];
		assert_eq!(std::str::from_utf8(bytes).unwrap(), row, "row {i}");
	}
}

#[test]
fn utf8_export_of_a_slice_aliases_the_parent_bytes_with_rebased_offsets() {
	// A non head utf8 slice must alias the parent bytes at its first row and hand out offsets rebased exactly to
	// zero.
	let rows = strings(ROWS);
	let mut c = Utf8Container::from_vec(rows.clone());
	c.freeze();
	let parent = utf8_bytes(&c).as_ptr();
	let start: usize = rows[..100].iter().map(|s| s.len()).sum();
	let s = c.slice(100, 350);
	let rebased = s.inner().compact_parts().1.into_owned();
	let (data, ptr) = export(s, utf8_bytes);
	drop(c);
	assert_eq!(ptr, parent.wrapping_add(start));
	let offsets = ScalarBuffer::<u64>::from(rebased);
	let refs: Vec<&[u8]> = rows[100..350].iter().map(|s| s.as_bytes()).collect();
	let expected: Vec<u64> = expected_offsets(&refs).into_iter().map(|o| o as u64).collect();
	assert_eq!(&offsets[..], &expected[..]);
	assert_eq!(data.len(), *expected.last().unwrap() as usize);
	for (i, row) in refs.iter().enumerate() {
		assert_eq!(&data.as_slice()[offsets[i] as usize..offsets[i + 1] as usize], *row, "row {i}");
	}
}

#[test]
fn blob_export_aliases_bytes_and_offsets() {
	// A frozen blob column must export both buffers without copying, including empty rows.
	let rows: Vec<Vec<u8>> = (0..ROWS).map(|i| (0..(i % 7) as u8).map(|b| b ^ i as u8).collect()).collect();
	let mut c = BlobContainer::from_vec(rows.iter().cloned().map(Blob::new).collect());
	c.freeze();
	let data_ptr = c.inner().compact_parts().0.as_ptr();
	let (data, exported) = export(c.clone(), |c: &BlobContainer| c.inner().compact_parts().0);
	let (offsets, _) = export(c.clone(), |c: &BlobContainer| match c.inner().compact_parts().1 {
		Cow::Borrowed(offsets) => offsets,
		Cow::Owned(_) => panic!("unsliced offsets must already start at zero"),
	});
	assert_eq!(exported, data_ptr);
	let offsets = ScalarBuffer::<i64>::from(offsets);
	let refs: Vec<&[u8]> = rows.iter().map(|r| r.as_slice()).collect();
	assert_eq!(&offsets[..], &expected_offsets(&refs)[..]);
	for (i, row) in refs.iter().enumerate() {
		assert_eq!(&data.as_slice()[offsets[i] as usize..offsets[i + 1] as usize], *row, "row {i}");
	}
}

fn bool_pattern(n: usize) -> Vec<bool> {
	(0..n).map(|i| i % 3 == 0 || i % 11 == 0).collect()
}

fn bool_bytes(c: &BoolContainer) -> &[u8] {
	match c.data().to_packed_bytes() {
		Cow::Borrowed(bytes) => bytes,
		Cow::Owned(_) => panic!("a view at bit zero must borrow its packed bytes"),
	}
}

#[test]
fn bool_export_aliases_the_packed_bits() {
	// Bools must be packed lsb first exactly like arrow, otherwise a zero copy boolean buffer reads the wrong rows.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let base = bool_bytes(&c).as_ptr();
	let (buffer, ptr) = export(c.clone(), bool_bytes);
	assert_eq!(ptr, base);
	assert_eq!(buffer.len(), ROWS.div_ceil(8));
	let bits = BooleanBuffer::new(buffer, 0, ROWS);
	assert_eq!(bits.iter().collect::<Vec<_>>(), bool_pattern(ROWS));
	assert_eq!(bits.count_set_bits(), bool_pattern(ROWS).iter().filter(|b| **b).count());
}

#[test]
fn bool_export_of_a_head_slice_aliases_the_parent_bits() {
	// A head bool slice must alias the parent bytes and read exactly its rows even though the last byte holds
	// parent bits.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let base = bool_bytes(&c).as_ptr();
	let (buffer, ptr) = export(c.take(101), bool_bytes);
	drop(c);
	assert_eq!(ptr, base);
	assert_eq!(buffer.len(), 13);
	let bits = BooleanBuffer::new(buffer, 0, 101);
	assert_eq!(bits.iter().collect::<Vec<_>>(), &bool_pattern(ROWS)[..101]);
}

#[test]
fn bool_packed_bytes_of_an_offset_slice_decode_at_bit_zero() {
	// A slice at a nonzero bit offset must repack so arrow reads it at bit zero with no stray parent bits.
	let mut c = BoolContainer::from_vec(bool_pattern(ROWS));
	c.freeze();
	let s = c.slice(13, 413);
	let packed = s.data().to_packed_bytes();
	assert!(matches!(packed, Cow::Owned(_)));
	assert_eq!(packed.len(), 50);
	let bits = BooleanBuffer::new(Buffer::from_vec(packed.into_owned()), 0, 400);
	assert_eq!(bits.iter().collect::<Vec<_>>(), &bool_pattern(ROWS)[13..413]);
}

#[test]
fn temporal_and_id_types_have_the_arrow_native_layout() {
	// Every exported type must match the size and alignment of its arrow native type, otherwise the zero copy cast
	// is unsound.
	assert_eq!((size_of::<Date>(), align_of::<Date>()), (size_of::<i32>(), align_of::<i32>()));
	assert_eq!((size_of::<Time>(), align_of::<Time>()), (size_of::<i64>(), align_of::<i64>()));
	assert_eq!((size_of::<DateTime>(), align_of::<DateTime>()), (size_of::<i64>(), align_of::<i64>()));
	assert_eq!(
		(size_of::<Duration>(), align_of::<Duration>()),
		(size_of::<IntervalMonthDayNano>(), align_of::<IntervalMonthDayNano>())
	);
	assert_eq!((size_of::<RowNumber>(), align_of::<RowNumber>()), (size_of::<u64>(), align_of::<u64>()));
	assert_eq!((size_of::<Uuid4>(), align_of::<Uuid4>()), (16, 1));
	assert_eq!((size_of::<Uuid7>(), align_of::<Uuid7>()), (16, 1));
	assert_eq!((size_of::<IdentityId>(), align_of::<IdentityId>()), (16, 1));
}
