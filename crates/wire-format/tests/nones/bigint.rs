// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::BigInt;
use reifydb_value::value::{
	container::number::NumberContainer, frame::data::FrameColumnData, int::Int, value_type::ValueType,
};

fn make(v: Vec<Int>) -> FrameColumnData {
	FrameColumnData::Int(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Int(BigInt::from(0)),
		Int(BigInt::from(i64::MAX)),
		Int(BigInt::from(i64::MIN)),
		Int(BigInt::from(42)),
		Int(BigInt::from(-999)),
	],
	inner_type: ValueType::Int,
}
