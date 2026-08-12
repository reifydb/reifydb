// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};
use tracing::{info, warn};

use crate::transaction::{
	FlowTransaction,
	group::{decode_payload, encode_payload},
};

const PERSIST_BUCKET_MS: u64 = 1_000;

const IMPLAUSIBLE_JUMP_MS: u64 = 3_600_000;

pub fn source_watermark_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::SOURCE_WATERMARK, vec![])
}

#[derive(Default)]
struct SourceState {
	hydrated: bool,
	value: Option<u64>,
}

#[derive(Clone, Default)]
pub struct SourceWatermarks {
	inner: Arc<DashMap<OperatorId, SourceState>>,
}

impl SourceWatermarks {
	pub fn advance(&self, source: OperatorId, txn: &mut impl FlowTransaction, at: DateTime) -> Result<()> {
		let coordinate = at.to_millis();
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		let persist = match state.value {
			Some(previous) => {
				if coordinate <= previous {
					return Ok(());
				}
				coordinate / PERSIST_BUCKET_MS > previous / PERSIST_BUCKET_MS
			}
			None => true,
		};
		if let Some(previous) = state.value
			&& coordinate > previous.saturating_add(IMPLAUSIBLE_JUMP_MS)
		{
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
		state.value = Some(coordinate);
		if persist {
			txn.state_set(source, &source_watermark_key(), encode_payload(&coordinate, at)?)?;
		}
		Ok(())
	}

	pub fn source_watermark(&self, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<DateTime> {
		Ok(DateTime::from_millis(self.raw(source, txn)?))
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
			let value = self.raw(*source, txn)?;
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

	fn raw(&self, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<u64> {
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		Ok(state.value.unwrap_or(0))
	}

	fn hydrate_once(state: &mut SourceState, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		if let Some(row) = txn.state_get(source, &source_watermark_key())? {
			state.value = Some(decode_payload::<u64>(&row)?);
		}
		Ok(())
	}
}

