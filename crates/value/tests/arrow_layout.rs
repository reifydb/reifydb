// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	mem::{align_of, size_of, size_of_val},
	ptr::NonNull,
	sync::Arc,
};

use arrow_buffer::{Buffer, IntervalMonthDayNano, ScalarBuffer, alloc::Allocation};
use reifydb_value::value::{
	container::{number::NumberContainer, row::RowNumberContainer},
	date::Date,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	row_number::RowNumber,
	time::Time,
	uuid::{Uuid4, Uuid7},
};

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
fixed_width_export!(row_number, RowNumberContainer, RowNumber, u64, |i| RowNumber(i as u64 * 5 + 1), |r| r.0);

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
