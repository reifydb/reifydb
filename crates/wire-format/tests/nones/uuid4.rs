// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::uuid::UuidContainer, frame::data::FrameColumnData, r#type::Type, uuid::Uuid4};

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
	inner_type: Type::Uuid4,
}
