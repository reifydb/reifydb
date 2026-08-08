// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::number::NumberContainer, decimal::Decimal, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<Decimal>) -> FrameColumnData {
	FrameColumnData::Decimal(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Decimal::new("0".parse().unwrap()),
		Decimal::new("123.456".parse().unwrap()),
		Decimal::new("-99.99".parse().unwrap()),
		Decimal::new("0.000001".parse().unwrap()),
		Decimal::new("-999999999.999999999".parse().unwrap()),
	],
	inner_type: ValueType::Decimal,
}
