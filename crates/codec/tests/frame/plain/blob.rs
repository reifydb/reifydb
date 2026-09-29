// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{blob::Blob, container::varlen_array::blob_array, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Blob>) -> ColumnData {
	data(ValueType::Blob, blob_array(&v))
}

crate::plain_tests! {
	typical: vec![Blob::new(vec![1, 2, 3]), Blob::new(vec![]), Blob::new(vec![255; 100])],
	boundary: vec![Blob::new(vec![]), Blob::new(vec![0]), Blob::new(vec![255; 256])],
	single: Blob::new(vec![42]),
}
