// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, frame::data::FrameColumnData};

fn make(v: Vec<Decimal>) -> FrameColumnData {
	FrameColumnData::Decimal(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Decimal::new("0".parse().unwrap()),
		Decimal::new("123.456".parse().unwrap()),
		Decimal::new("-99.99".parse().unwrap()),
	],
	boundary: vec![
		Decimal::new("0".parse().unwrap()),
		Decimal::new("0.000001".parse().unwrap()),
		Decimal::new("-999999999.999999999".parse().unwrap()),
	],
	single: Decimal::new("0".parse().unwrap()),
}
