// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::uuid::UuidContainer, frame::data::FrameColumnData, r#type::Type, uuid::Uuid7};

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
	inner_type: Type::Uuid7,
}
