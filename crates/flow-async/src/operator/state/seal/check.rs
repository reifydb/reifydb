// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{BooleanArray, RecordBatch};
use reifydb_value::{
	error::ValueError,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{
	operator::{state::seal::domain::SealDomain, time_at_or_after},
	window::{coord::ByDateTime, driver::store::WindowStore},
};

pub const SEALED_INSERTS: &str = "inserts past the seal";

pub const SEALED_UPDATES: &str = "updates past the seal";

pub const SEALED_REMOVES: &str = "removes past the seal";

pub struct Kept {
	pub mask: BooleanArray,
	pub dropped: u64,
}

pub fn first_admitted(store: &mut dyn WindowStore, seal_span: Duration) -> Result<DateTime, ValueError> {
	let watermark = ByDateTime::operator_watermark(store)?;
	Ok(ByDateTime::first_admitted(watermark, seal_span).0)
}

pub fn first_admitted_once(
	cached: &mut Option<DateTime>,
	store: &mut dyn WindowStore,
	seal_span: Duration,
) -> Result<DateTime, ValueError> {
	if let Some(first_admitted) = *cached {
		return Ok(first_admitted);
	}
	Ok(*cached.insert(first_admitted(store, seal_span)?))
}

pub fn kept_from(first_admitted: DateTime, batch: &RecordBatch) -> Result<Kept, ValueError> {
	let mask = time_at_or_after(batch, first_admitted)?;
	Ok(Kept {
		dropped: mask.false_count() as u64,
		mask,
	})
}
