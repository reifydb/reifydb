// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::uuid::UuidContainer, frame::data::FrameColumnData, uuid::Uuid7, value_type::ValueType,
};

fn make(v: Vec<Uuid7>) -> FrameColumnData {
	FrameColumnData::Uuid7(UuidContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Uuid7(uuid::Uuid::nil()),
		Uuid7(uuid::Uuid::max()),
		Uuid7(uuid::Uuid::from_u128(1)),
		Uuid7(uuid::Uuid::from_u128(42)),
		Uuid7(uuid::Uuid::from_u128(u128::MAX - 1)),
	],
	inner_type: ValueType::Uuid7,
}
