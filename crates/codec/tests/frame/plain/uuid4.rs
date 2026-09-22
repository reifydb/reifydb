// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::uuid_array::uuid4_array, frame::data::FrameColumnData, uuid::Uuid4};

fn make(v: Vec<Uuid4>) -> FrameColumnData {
	FrameColumnData::Uuid4(uuid4_array(v))
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
