// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::uuid::UuidContainer, frame::data::FrameColumnData, uuid::Uuid7};

fn make(v: Vec<Uuid7>) -> FrameColumnData {
	FrameColumnData::Uuid7(UuidContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Uuid7(uuid::Uuid::nil()),
		Uuid7(uuid::Uuid::max()),
	],
	boundary: vec![
		Uuid7(uuid::Uuid::nil()),
		Uuid7(uuid::Uuid::max()),
	],
	single: Uuid7(uuid::Uuid::nil()),
}
