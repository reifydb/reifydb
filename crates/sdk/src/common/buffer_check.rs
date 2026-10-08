// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::error::{Result as SdkResult, SdkError};

pub(crate) fn malformed(reason: String) -> SdkError {
	SdkError::InvalidInput(reason)
}

pub(crate) fn cell_range(offsets: &[u64], data_len: usize, index: usize, what: &str) -> SdkResult<(usize, usize)> {
	let (Some(&start), Some(&end)) = (offsets.get(index), offsets.get(index + 1)) else {
		return Err(malformed(format!("{what} cell {index} has no offsets among the {} sent", offsets.len())));
	};
	let (start, end) = (start as usize, end as usize);
	if start > end || end > data_len {
		return Err(malformed(format!(
			"{what} cell {index} spans {start}..{end}, outside the {data_len} data bytes sent"
		)));
	}
	Ok((start, end))
}

pub(crate) fn fixed_len(data_len: usize, row_count: usize, width: usize, what: &str) -> SdkResult<()> {
	if row_count.checked_mul(width) == Some(data_len) {
		return Ok(());
	}
	Err(malformed(format!("{what} holds {data_len} data bytes for {row_count} rows of {width} bytes each")))
}

pub(crate) fn bitvec_len(bitvec_len: usize, row_count: usize, what: &str) -> SdkResult<()> {
	if bitvec_len >= row_count.div_ceil(8) {
		return Ok(());
	}
	Err(malformed(format!("{what} holds a {bitvec_len} byte bitvec for {row_count} rows")))
}
