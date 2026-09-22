// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::{align_of, size_of};

use arrow_buffer::IntervalMonthDayNano;
use reifydb_value::value::{
	date::Date,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	row_number::RowNumber,
	time::Time,
	uuid::{Uuid4, Uuid7},
};

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
