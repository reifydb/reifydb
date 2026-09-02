// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	key::{system::VersionEpochKey, typed::key::Key},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::version_epoch::{BUCKET_WIDTH, EpochSeconds, EpochSpan};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, value::identity::IdentityId};

const RANGE_BATCH: usize = 256;

const SAMPLE_WIDTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
	pub at: EpochSeconds,
	pub version: u64,
}

pub struct EpochLog {
	engine: StandardEngine,
	catalog: Catalog,
	last: Option<(EpochSeconds, u64)>,
}

impl EpochLog {
	pub fn new(engine: StandardEngine) -> Self {
		let catalog = engine.catalog();
		Self {
			engine,
			catalog,
			last: None,
		}
	}

	pub fn write(&mut self, at: EpochSeconds, version: CommitVersion) -> Result<bool> {
		if version.0 == 0 {
			return Ok(false);
		}

		let period = self.period_of(at);
		if let Some((last_period, settled_at)) = self.last
			&& last_period == period
			&& version.0 <= settled_at
		{
			return Ok(false);
		}

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		txn.set(&VersionEpochKey::encoded(period), encode(at, version).into_bytes())?;
		let written_at = txn.commit_unchecked()?;

		self.last = Some((period, written_at.0.max(version.0)));
		Ok(true)
	}

	pub fn read_since(&self, oldest: EpochSeconds, now: EpochSeconds) -> Result<Vec<Sample>> {
		let bucket = self.bucket();
		let mut samples = Vec::new();

		let txn = self.engine.begin_query(IdentityId::system())?;
		for entry in txn.range(VersionEpochKey::floor_scan(now), RangeScope::All, RANGE_BATCH) {
			let entry = entry?;
			let Some(key) = VersionEpochKey::decode(&entry.key) else {
				continue;
			};
			if key.bucket.plus(bucket) <= oldest {
				break;
			}
			let Some(sample) = decode(EncodedPodRow::view(&entry.bytes)) else {
				continue;
			};
			if sample.at >= oldest {
				samples.push(sample);
			}
		}

		samples.sort_unstable_by_key(|sample| sample.at);
		Ok(samples)
	}

	pub fn expired_before(&self, oldest: EpochSeconds, budget: usize) -> Result<Vec<EncodedKey>> {
		let mut expired = Vec::new();

		let txn = self.engine.begin_query(IdentityId::system())?;
		for entry in txn.range(VersionEpochKey::older_than(oldest), RangeScope::All, RANGE_BATCH) {
			let entry = entry?;
			let Some(sample) = decode(EncodedPodRow::view(&entry.bytes)) else {
				continue;
			};
			if sample.at >= oldest {
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
		for entry in txn.range(
			VersionEpochKey::floor_scan(EpochSeconds::new(u64::MAX)),
			RangeScope::All,
			RANGE_BATCH,
		) {
			entry?;
			count += 1;
		}
		Ok(count)
	}

	fn bucket(&self) -> EpochSpan {
		let seconds = self.catalog.get_config_duration(ConfigKey::EpochBucketInterval).to_std().as_secs();
		assert!(
			seconds >= BUCKET_WIDTH.seconds(),
			"EpochBucketInterval resolved to {seconds}s, below the {}s epoch resolution: bucketing by it \
			 would divide by zero or file every sample under one period",
			BUCKET_WIDTH.seconds()
		);
		EpochSpan::new(seconds)
	}

	fn period_of(&self, at: EpochSeconds) -> EpochSeconds {
		let bucket = self.bucket().seconds();
		EpochSeconds::new(at.seconds() - (at.seconds() % bucket))
	}
}

fn encode(at: EpochSeconds, version: CommitVersion) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(SAMPLE_WIDTH);
	bytes.extend_from_slice(&at.seconds().to_be_bytes());
	bytes.extend_from_slice(&version.0.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

fn decode(row: &EncodedPodRow) -> Option<Sample> {
	let bytes = row.body();
	if bytes.len() != SAMPLE_WIDTH {
		return None;
	}
	Some(Sample {
		at: EpochSeconds::new(u64::from_be_bytes(bytes[..8].try_into().ok()?)),
		version: u64::from_be_bytes(bytes[8..].try_into().ok()?),
	})
}
