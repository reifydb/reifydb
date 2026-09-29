// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_buffer::BooleanBuffer;
use reifydb_value::value::{Value, container::any_array::any_array_optional};

use crate::{error::DecodeError, reader::Reader, value::decode_value_from};

pub(crate) fn decode_any_column(
	row_count: usize,
	data: &[u8],
	defined: Option<&BooleanBuffer>,
) -> Result<ArrayRef, DecodeError> {
	let mut r = Reader::new(data);
	let mut values = Vec::with_capacity(row_count);
	for row in 0..row_count {
		match decode_value_from(&mut r)? {
			Value::None {
				..
			} if defined.is_some_and(|defined| defined.value(row)) => {
				return Err(DecodeError::InvalidData(format!(
					"any cell at row {row} holds a none under a set valid bit"
				)));
			}
			Value::None {
				..
			} => values.push(None),
			value => values.push(Some(value)),
		}
	}
	Ok(Arc::new(any_array_optional(values)))
}
