// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::uuid::UuidContainer, frame::data::FrameColumnData, uuid::Uuid4, value_type::ValueType,
};

fn make(v: Vec<Uuid4>) -> FrameColumnData {
	FrameColumnData::Uuid4(UuidContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Uuid4(uuid::Uuid::nil()),
		Uuid4::generate(),
		Uuid4(uuid::Uuid::max()),
		Uuid4::generate(),
		Uuid4::generate(),
	],
	inner_type: ValueType::Uuid4,
}
