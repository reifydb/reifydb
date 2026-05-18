// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{blob::Blob, container::blob::BlobContainer, frame::data::FrameColumnData};

fn make(v: Vec<Blob>) -> FrameColumnData {
	FrameColumnData::Blob(BlobContainer::new(v))
}

crate::plain_tests! {
	typical: vec![Blob::new(vec![1, 2, 3]), Blob::new(vec![]), Blob::new(vec![255; 100])],
	boundary: vec![Blob::new(vec![]), Blob::new(vec![0]), Blob::new(vec![255; 256])],
	single: Blob::new(vec![42]),
}
