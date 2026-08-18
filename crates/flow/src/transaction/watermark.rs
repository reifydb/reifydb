// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};
use tracing::{info, warn};

use crate::transaction::{
	FlowTransaction,
	group::{decode_payload, encode_payload},
	state::StateExtension,
};

const IMPLAUSIBLE_JUMP_MS: u64 = 3_600_000;

pub fn source_watermark_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::SOURCE_WATERMARK, vec![])
}

#[derive(Clone, Default)]
pub struct SourceWatermarks;

impl SourceWatermarks {
	pub fn advance(&self, source: OperatorId, txn: &mut impl FlowTransaction, at: DateTime) -> Result<()> {
		let coordinate = at.to_millis();
		let previous = raw(source, txn)?;
		if coordinate <= previous {
			return Ok(());
		}
		if previous > 0 && coordinate > previous.saturating_add(IMPLAUSIBLE_JUMP_MS) {
			warn!(
				source = source.0,
				from_ms = previous,
				to_ms = coordinate,
				delta_ms = coordinate - previous,
				"source watermark jumped by more than an hour in one step; a row stamped from a \
				 clock rather than from its own event time moves the watermark to now and can seal \
				 every open window at once"
			);
		}
		txn.state_set(source, &source_watermark_key(), encode_payload(&coordinate)?)
	}

	pub fn source_watermark(&self, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<DateTime> {
		Ok(DateTime::from_millis(raw(source, txn)?))
	}

	pub fn flow_watermark(&self, sources: &[OperatorId], txn: &mut impl FlowTransaction) -> Result<DateTime> {
		reifydb_assertions! {
			assert!(
				!sources.is_empty(),
				"a flow watermark was read with no sources; the min-merge over nothing would \
				 pin the watermark at zero and hold every horizon open, so the caller failed \
				 to wire the flow's source list"
			);
		}
		let mut merged: Option<u64> = None;
		let mut per_source: Vec<(OperatorId, u64)> = Vec::with_capacity(sources.len());
		for source in sources {
			let value = raw(*source, txn)?;
			per_source.push((*source, value));
			merged = Some(match merged {
				Some(current) => current.min(value),
				None => value,
			});
		}
		let merged = merged.unwrap_or(0);
		if merged == 0 && per_source.iter().any(|(_, value)| *value > 0) {
			let pinning: Vec<u64> = per_source
				.iter()
				.filter(|(_, value)| *value == 0)
				.map(|(source, _)| source.0)
				.collect();
			info!(
				sources = sources.len(),
				pinned_by = ?pinning,
				"flow watermark merged to the epoch while other sources have advanced; the min-merge \
				 holds every horizon open until each listed source reports, so no window can seal"
			);
		}
		Ok(DateTime::from_millis(merged))
	}
}

fn raw(source: OperatorId, txn: &mut impl FlowTransaction) -> Result<u64> {
	match txn.state_get(source, &source_watermark_key())? {
		Some(row) => decode_payload::<u64>(&row),
		None => Ok(0),
	}
}
