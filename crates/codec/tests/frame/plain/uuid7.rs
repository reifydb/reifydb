// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::uuid_array::uuid7_array, frame::data::FrameColumnData, uuid::Uuid7};

fn make(v: Vec<Uuid7>) -> FrameColumnData {
	FrameColumnData::Uuid7(uuid7_array(v))
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
