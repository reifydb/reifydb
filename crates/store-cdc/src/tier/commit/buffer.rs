// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	tier::commit::{batch::FlushBatch, sealed::SealedRanges},
	types::cdc_resident_bytes,
};

#[derive(Default)]
pub(crate) struct BufferInner {
	pub(crate) live: BTreeMap<CommitVersion, Arc<Cdc>>,
	pub(crate) live_bytes: ByteSize,
	pub(crate) in_flight: Option<Arc<FlushBatch>>,
	pub(crate) flushing: bool,
	pub(crate) triggered: bool,
	pub(crate) sealed: SealedRanges,
}

impl BufferInner {
	pub(crate) fn resident_bytes(&self) -> ByteSize {
		self.live_bytes.saturating_add(self.in_flight.as_ref().map_or(ByteSize::ZERO, |batch| batch.bytes))
	}

	pub(crate) fn entries(&self) -> usize {
		self.live.len() + self.in_flight.as_ref().map_or(0, |batch| batch.entries.len())
	}

	pub(crate) fn accepts(&self, version: CommitVersion) -> bool {
		if self.sealed.contains(version) {
			return false;
		}
		if self.live.contains_key(&version) {
			return false;
		}
		!self.in_flight.as_ref().is_some_and(|batch| batch.entries.iter().any(|cdc| cdc.version == version))
	}

	pub(crate) fn append(&mut self, cdc: Arc<Cdc>) {
		self.live_bytes = self.live_bytes.saturating_add(cdc_resident_bytes(&cdc));
		self.live.insert(cdc.version, cdc);
	}

	#[instrument(name = "store::cdc::commit::cut_within", level = "debug", skip_all)]
	pub(crate) fn cut_within(&mut self, cut_bytes: ByteSize) -> FlushBatch {
		let mut entries = Vec::new();
		let mut bytes = ByteSize::ZERO;
		let front = self.live.first_key_value().map(|(version, _)| *version);
		let ceiling = front.and_then(|version| self.sealed.next_start_above(version));
		loop {
			let Some((version, cost)) =
				self.live.first_key_value().map(|(version, cdc)| (*version, cdc_resident_bytes(cdc)))
			else {
				break;
			};
			if ceiling.is_some_and(|ceiling| version >= ceiling) {
				break;
			}
			if entries.last().is_some_and(|last: &Arc<Cdc>| version.0 != last.version.0.saturating_add(1)) {
				break;
			}
			if !entries.is_empty() && bytes.saturating_add(cost) > cut_bytes {
				break;
			}
			let Some((_, cdc)) = self.live.pop_first() else {
				break;
			};
			bytes = bytes.saturating_add(cost);
			entries.push(cdc);
		}
		self.live_bytes = self.live_bytes.saturating_sub(bytes);
		if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
			self.sealed.insert(first.version, last.version);
		}
		FlushBatch {
			entries,
			bytes,
		}
	}
}
