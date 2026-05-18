// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, uint::Uint};

fn make(v: Vec<Uint>) -> FrameColumnData {
	FrameColumnData::Uint(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![Uint(BigInt::from(0u64)), Uint(BigInt::from(u64::MAX))],
	boundary: vec![Uint(BigInt::from(0u64)), Uint(BigInt::from(1u64))],
	single: Uint(BigInt::from(0u64)),
}
