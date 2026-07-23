// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	key::{EncodableKey, version_epoch::VersionEpochKey},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	value::{identity::IdentityId, value_type::ValueType},
};

const RANGE_BATCH: usize = 256;

const DEFAULT_BUCKET_NANOS: u64 = 60_000_000_000;

const AT_NANOS: usize = 0;

const VERSION: usize = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
	pub at_nanos: u64,
	pub version: u64,
}

pub struct EpochLog {
	engine: StandardEngine,
	catalog: Catalog,
	shape: RowShape,
	last: Option<(u64, u64)>,
}

impl EpochLog {
	pub fn new(engine: StandardEngine) -> Self {
		let catalog = engine.catalog();
		Self {
			engine,
			catalog,
			shape: sample_shape(),
			last: None,
		}
	}

	pub fn write(&mut self, at_nanos: u64, version: CommitVersion) -> Result<bool> {
		if version.0 == 0 {
			return Ok(false);
		}

		let period = self.period_of(at_nanos);
		if let Some((last_period, settled_at)) = self.last
			&& last_period == period
			&& version.0 <= settled_at
		{
			return Ok(false);
		}

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		txn.set(&VersionEpochKey::encoded(period), self.encode(at_nanos, version))?;
		let written_at = txn.commit_unchecked()?;

		self.last = Some((period, written_at.0.max(version.0)));
		Ok(true)
	}

	pub fn read_since(&self, oldest_nanos: u64, now_nanos: u64) -> Result<Vec<Sample>> {
		let bucket = self.bucket_nanos();
		let mut samples = Vec::new();

		let txn = self.engine.begin_query(IdentityId::system())?;
		for entry in txn.range(VersionEpochKey::floor_scan(now_nanos), RangeScope::All, RANGE_BATCH) {
			let entry = entry?;
			let Some(key) = VersionEpochKey::decode(&entry.key) else {
				continue;
			};
			if key.bucket_nanos.saturating_add(bucket) <= oldest_nanos {
				break;
			}
			let sample = self.decode(&entry.row);
			if sample.at_nanos >= oldest_nanos {
				samples.push(sample);
			}
		}

		samples.sort_unstable_by_key(|sample| sample.at_nanos);
		Ok(samples)
	}

	pub fn expired_before(&self, oldest_nanos: u64, budget: usize) -> Result<Vec<EncodedKey>> {
		let mut expired = Vec::new();

		let txn = self.engine.begin_query(IdentityId::system())?;
		for entry in txn.range(VersionEpochKey::older_than(oldest_nanos), RangeScope::All, RANGE_BATCH) {
			let entry = entry?;
			if self.decode(&entry.row).at_nanos >= oldest_nanos {
				continue;
			}
			expired.push(entry.key.clone());
			if expired.len() >= budget {
				break;
			}
		}

		Ok(expired)
	}

	pub fn durable_count(&self) -> Result<u64> {
		let txn = self.engine.begin_query(IdentityId::system())?;
		let mut count = 0u64;
		for entry in txn.range(VersionEpochKey::floor_scan(u64::MAX), RangeScope::All, RANGE_BATCH) {
			entry?;
			count += 1;
		}
		Ok(count)
	}

	fn bucket_nanos(&self) -> u64 {
		match self.catalog.get_config_duration(ConfigKey::EpochBucketInterval).as_nanos() {
			Ok(nanos) if nanos > 0 => nanos as u64,
			_ => DEFAULT_BUCKET_NANOS,
		}
	}

	fn period_of(&self, at_nanos: u64) -> u64 {
		let bucket = self.bucket_nanos();
		at_nanos - (at_nanos % bucket)
	}

	fn encode(&self, at_nanos: u64, version: CommitVersion) -> EncodedRow {
		let mut row = self.shape.allocate();
		self.shape.set_u64(&mut row, AT_NANOS, at_nanos);
		self.shape.set_u64(&mut row, VERSION, version.0);
		row
	}

	fn decode(&self, row: &EncodedRow) -> Sample {
		Sample {
			at_nanos: self.shape.get_u64(row, AT_NANOS),
			version: self.shape.get_u64(row, VERSION),
		}
	}
}

fn sample_shape() -> RowShape {
	RowShape::testing(&[ValueType::Uint8, ValueType::Uint8])
}
