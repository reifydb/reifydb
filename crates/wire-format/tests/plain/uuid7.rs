// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
