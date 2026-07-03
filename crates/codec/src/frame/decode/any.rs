// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::any::AnyContainer, frame::data::FrameColumnData};

use crate::{error::DecodeError, reader::Reader, value::decode_value_from};

pub(crate) fn decode_any_column(row_count: usize, data: &[u8]) -> Result<FrameColumnData, DecodeError> {
	let mut r = Reader::new(data);
	let mut values = Vec::with_capacity(row_count);
	for _ in 0..row_count {
		values.push(Box::new(decode_value_from(&mut r)?));
	}
	Ok(FrameColumnData::Any(AnyContainer::new(values)))
}
