// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::internal::internal,
	interface::cdc::ConsumerClass,
	key::{any::TaggedKey, cdc::CdcConsumerKey},
	lifecycle::watermark::CheckpointFloor,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{Result, error::Error};
use tracing::warn;

use super::checkpoint::CheckpointRow;

pub fn compute_pinning_watermark(
	txn: &mut Transaction<'_>,
	floor: Option<&dyn CheckpointFloor>,
) -> Result<Option<CommitVersion>> {
	let mut min_version: Option<CommitVersion> = None;
	for multi in txn.range(CdcConsumerKey::full_scan(), RangeScope::All, 1024)? {
		let multi = multi?;
		if !matches!(&multi.key, TaggedKey::CdcConsumer(_)) {
			continue;
		}
		let Some(bytes) = CheckpointRow::decode(&multi.bytes) else {
			return Err(Error(Box::new(internal("a cdc consumer checkpoint row could not be decoded"))));
		};
		if bytes.class != ConsumerClass::Pinning {
			continue;
		}
		min_version = Some(min_version.map_or(bytes.version, |m| m.min(bytes.version)));
	}

	if let Some(floor) = floor {
		match floor.floor() {
			Ok(Some(durable)) => {
				min_version = Some(min_version.map_or(durable, |m| m.min(durable)));
			}
			Ok(None) => {}
			Err(err) => {
				warn!(error = %err, "checkpoint floor unavailable, pinning cdc retention at version zero");
				min_version = Some(CommitVersion(0));
			}
		}
	}

	Ok(min_version)
}
