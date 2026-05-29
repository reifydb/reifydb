// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::uuid::UuidContainer, frame::data::FrameColumnData, uuid::Uuid4};

fn make(v: Vec<Uuid4>) -> FrameColumnData {
	FrameColumnData::Uuid4(UuidContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Uuid4(uuid::Uuid::nil()),
		Uuid4::generate(),
		Uuid4(uuid::Uuid::max()),
	],
	boundary: vec![
		Uuid4(uuid::Uuid::nil()),
		Uuid4(uuid::Uuid::max()),
	],
	single: Uuid4(uuid::Uuid::nil()),
}
