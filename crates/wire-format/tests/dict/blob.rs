// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{blob::Blob, container::blob::BlobContainer, frame::data::FrameColumnData};

fn make(v: Vec<Blob>) -> FrameColumnData {
	FrameColumnData::Blob(BlobContainer::new(v))
}

crate::dict_tests! {
	low_cardinality: {
		let mut v = Vec::new();
		for _ in 0..100 {
			v.push(Blob::new(vec![1, 2, 3]));
			v.push(Blob::new(vec![4, 5, 6]));
			v.push(Blob::new(vec![7, 8, 9]));
		}
		v
	},
	high_cardinality: (0..100u8).map(|i| Blob::new(vec![i, i.wrapping_add(1), i.wrapping_add(2)])).collect::<Vec<_>>(),
}
