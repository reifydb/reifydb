// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u128, 1, 1_000_000_000_000, u128::MAX - 1, u128::MAX],
	inner_type: Type::Uint16,
}
