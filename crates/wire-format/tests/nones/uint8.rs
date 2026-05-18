// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u64, 1, 1_000_000_000, u64::MAX - 1, u64::MAX],
	inner_type: Type::Uint8,
}
