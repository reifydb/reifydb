// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	blob::Blob, container::blob::BlobContainer, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<Blob>) -> FrameColumnData {
	FrameColumnData::Blob(BlobContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Blob::new(vec![1, 2, 3]),
		Blob::new(vec![]),
		Blob::new(vec![255; 100]),
		Blob::new(vec![0]),
		Blob::new(vec![7, 7, 7]),
	],
	inner_type: ValueType::Blob,
}
