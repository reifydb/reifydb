// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use num_bigint::BigInt;
use reifydb_value::value::{
	container::number::NumberContainer, frame::data::FrameColumnData, uint::Uint, value_type::ValueType,
};

fn make(v: Vec<Uint>) -> FrameColumnData {
	FrameColumnData::Uint(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Uint(BigInt::from(0u64)),
		Uint(BigInt::from(u64::MAX)),
		Uint(BigInt::from(1u64)),
		Uint(BigInt::from(42u64)),
		Uint(BigInt::from(1_000_000u64)),
	],
	inner_type: ValueType::Uint,
}
