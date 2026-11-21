// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{BooleanArray, RecordBatch};
use reifydb_value::{error::ValueError, value::duration::Duration};

use crate::{
	operator::{state::seal::domain::SealDomain, time_at_or_after},
	window::{coord::ByDateTime, driver::store::WindowStore},
};

pub struct Kept {
	pub mask: BooleanArray,
	pub dropped: u64,
}

pub fn kept_unsealed(
	store: &mut dyn WindowStore,
	seal_span: Duration,
	batch: &RecordBatch,
) -> Result<Kept, ValueError> {
	let watermark = ByDateTime::operator_watermark(store)?;
	let first_admitted = ByDateTime::first_admitted(watermark, seal_span).0;
	let mask = time_at_or_after(batch, first_admitted)?;
	Ok(Kept {
		dropped: mask.false_count() as u64,
		mask,
	})
}
