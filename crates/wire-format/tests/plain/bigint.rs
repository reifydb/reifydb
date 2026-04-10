// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, int::Int};

fn make(v: Vec<Int>) -> FrameColumnData {
	FrameColumnData::Int(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![Int(BigInt::from(0)), Int(BigInt::from(i64::MAX)), Int(BigInt::from(i64::MIN))],
	boundary: vec![Int(BigInt::from(0)), Int(BigInt::from(-1)), Int(BigInt::from(1))],
	single: Int(BigInt::from(0)),
}
