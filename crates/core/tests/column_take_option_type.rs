// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::value_type::ValueType;

#[test]
fn take_keeps_the_option_wrapper_when_every_kept_row_is_defined() {
	// The column type must never depend on the data, otherwise take and slice of the same rows disagree on type.
	let buffer = ColumnBuffer::int4_optional([Some(1), None]);
	let optional_int4 = ValueType::Option(Box::new(ValueType::Int4));
	assert_eq!(buffer.slice(0, 1).get_type(), optional_int4, "slice keeps the option wrapper");
	assert_eq!(buffer.take(1).get_type(), optional_int4, "take must keep it too");
}
