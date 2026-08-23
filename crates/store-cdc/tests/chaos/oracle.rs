// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Retention is whole-block, so a block whose highest version straddles the cutoff survives intact, records below it
//! and all.

use std::collections::{BTreeMap, BTreeSet};

use reifydb_core::interface::cdc::CdcChange;

pub type Version = u64;

/// The whole change list, not merely its length: a tier that round-trips a payload through the wrong codec keeps the
/// count and the timestamp intact and would otherwise pass every read.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
	pub changes: Vec<CdcChange>,
	pub timestamp: u64,
	pub bytes: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
	pub sources: BTreeSet<u64>,
}

/// One sealed block: the contiguous run of versions one cut handed to the persistent tier.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockModel {
	pub versions: Vec<Version>,
	pub min: Version,
	pub max: Version,
	pub min_timestamp: u64,
	pub max_timestamp: u64,
	pub count: u64,
	pub bytes: u64,
}

/// How far a TTL pass must reach. Dropping is exclusive, so when every block is expired no version names the bound and
/// only an unbounded reach expires the block at the very top of the version space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlCutoff {
	Version(Version),
	Unbounded,
}

/// What one `drop_before` must report back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Eviction {
	pub count: u64,
	pub sources: usize,
	pub key_bytes: u64,
	pub value_bytes: u64,
	pub more_remaining: bool,
}

#[derive(Clone)]
pub struct Oracle {
	cut_bytes: u64,
	live: BTreeMap<Version, Record>,
	live_bytes: u64,
	blocks: Vec<BlockModel>,
	records: BTreeMap<Version, Record>,
	dropped: BTreeSet<Version>,
	sealed_below: Option<Version>,
	sealed_versions: BTreeSet<Version>,
	truncated_before: Version,
	blocks_cut: u64,
}

impl Oracle {
	pub fn new(cut_bytes: u64) -> Self {
		Self {
			cut_bytes,
			live: BTreeMap::new(),
			live_bytes: 0,
			blocks: Vec::new(),
			records: BTreeMap::new(),
			dropped: BTreeSet::new(),
			sealed_below: None,
			sealed_versions: BTreeSet::new(),
			truncated_before: 0,
			blocks_cut: 0,
		}
	}

	/// A version is sealed either because a cut carried it or because a blanket floor swallowed it; a lone
	/// high-water mark would refuse every hole under the highest cut, and versions do not arrive in ascending
	/// order.
	pub fn sealed_contains(&self, version: Version) -> bool {
		self.sealed_below.is_some_and(|floor| version <= floor) || self.sealed_versions.contains(&version)
	}

	pub fn live_contains(&self, version: Version) -> bool {
		self.live.contains_key(&version)
	}

	pub fn rejects(&self, version: Version) -> bool {
		self.sealed_contains(version) || self.live.contains_key(&version)
	}

	pub fn write(&mut self, version: Version, record: Record) -> bool {
		if self.rejects(version) {
			return false;
		}
		self.live_bytes = self.live_bytes.saturating_add(record.bytes);
		self.live.insert(version, record.clone());
		self.records.insert(version, record);
		true
	}

	/// The harness must drain here, at a full cut worth of bytes, or the block boundaries stop being a function of
	/// the seed.
	pub fn should_cut(&self) -> bool {
		!self.live.is_empty() && self.live_bytes >= self.cut_bytes
	}

	pub fn flush(&mut self) {
		while !self.live.is_empty() {
			self.cut_one();
		}
	}

	fn cut_one(&mut self) {
		// a block covers one contiguous version run, so a hole must end it even when the byte budget is
		// untouched
		let mut versions: Vec<Version> = Vec::new();
		let mut bytes = 0u64;
		loop {
			let Some((&version, record)) = self.live.first_key_value() else {
				break;
			};
			let cost = record.bytes;
			if versions.last().is_some_and(|last| version != last.saturating_add(1)) {
				break;
			}
			if !versions.is_empty() && bytes.saturating_add(cost) > self.cut_bytes {
				break;
			}
			bytes = bytes.saturating_add(cost);
			versions.push(version);
			self.live.pop_first();
		}
		self.live_bytes = self.live_bytes.saturating_sub(bytes);

		let min = versions[0];
		let max = *versions.last().unwrap();
		let min_timestamp = versions.iter().map(|v| self.records[v].timestamp).min().unwrap();
		let max_timestamp = versions.iter().map(|v| self.records[v].timestamp).max().unwrap();
		let count = versions.len() as u64;
		self.sealed_versions.extend(versions.iter().copied());
		self.blocks.push(BlockModel {
			versions,
			min,
			max,
			min_timestamp,
			max_timestamp,
			count,
			bytes,
		});
		self.blocks_cut += 1;
	}

	/// A block is droppable only when its highest version is strictly below the cutoff; the scan looks one past
	/// `limit` so a limit of zero still reports `more_remaining`. An unbounded reach names no version, so every
	/// block is droppable and only `limit` holds the pass back.
	pub fn drop_before(&mut self, cutoff: TtlCutoff, limit: usize) -> Eviction {
		let droppable = match cutoff {
			TtlCutoff::Version(version) => {
				self.blocks.iter().take_while(|block| block.max < version).count()
			}
			TtlCutoff::Unbounded => self.blocks.len(),
		};
		let more_remaining = droppable > limit;
		let doomed = droppable.min(limit);
		if doomed == 0 {
			return Eviction {
				count: 0,
				sources: 0,
				key_bytes: 0,
				value_bytes: 0,
				more_remaining,
			};
		}

		let mut count = 0u64;
		let mut key_bytes = 0u64;
		let mut value_bytes = 0u64;
		let mut sources: BTreeSet<u64> = BTreeSet::new();
		let mut highest = 0u64;
		for block in self.blocks.drain(..doomed) {
			for version in &block.versions {
				let record = self.records.remove(version).expect("a sealed version must be held");
				count += record.changes.len() as u64;
				key_bytes += record.key_bytes;
				value_bytes += record.value_bytes;
				sources.extend(record.sources.iter().copied());
				self.dropped.insert(*version);
			}
			highest = highest.max(block.max);
		}
		self.truncated_before = self.truncated_before.max(highest.saturating_add(1));
		if self.truncated_before > 0 {
			let floor = self.truncated_before - 1;
			self.sealed_below = Some(self.sealed_below.map_or(floor, |current| current.max(floor)));
		}

		Eviction {
			count,
			sources: sources.len(),
			key_bytes,
			value_bytes,
			more_remaining,
		}
	}

	/// A boot discards whatever the commit tier held, so a version lost with it is writable again but one a
	/// surviving block carries is not.
	pub fn reopen(&mut self) {
		for version in std::mem::take(&mut self.live).into_keys() {
			self.records.remove(&version);
		}
		self.live_bytes = 0;
		self.sealed_below = self.blocks.iter().map(|block| block.max).max();
		self.sealed_versions.clear();
		self.blocks_cut = 0;
	}

	pub fn read(&self, version: Version) -> Option<&Record> {
		self.records.get(&version)
	}

	pub fn range(&self, lo: Version, hi: Version, want: usize) -> Vec<Version> {
		if lo > hi {
			return Vec::new();
		}
		self.records.range(lo..=hi).map(|(version, _)| *version).take(want).collect()
	}

	pub fn has_above(&self, exclusive: Version, hi: Version) -> bool {
		exclusive < hi && self.records.range(exclusive.saturating_add(1)..=hi).next().is_some()
	}

	pub fn has_in(&self, lo: Version, hi: Version) -> bool {
		lo <= hi && self.records.range(lo..=hi).next().is_some()
	}

	pub fn min_version(&self) -> Option<Version> {
		match self.blocks.first() {
			Some(block) => Some(block.min),
			None => self.live.keys().next().copied(),
		}
	}

	pub fn max_version(&self) -> Option<Version> {
		match self.live.keys().next_back() {
			Some(version) => Some(*version),
			None => self.blocks.last().map(|block| block.max),
		}
	}

	pub fn truncated_before(&self) -> Version {
		self.truncated_before
	}

	/// Answered from block summaries alone, never the commit tier; with nothing at or after the cutoff, one past
	/// the highest sealed version tells a sweep the whole tier is expired.
	///
	/// The reach is the lowest version of the EARLIEST block still holding a record at or after the cutoff, scanned
	/// in version order. Picking the block with the smallest `max_timestamp` instead answers the same only while
	/// timestamps rise with version; a lower-versioned block with a newer record would be dropped under it, and a
	/// sweep must never delete a record above its own cutoff.
	pub fn find_ttl_cutoff(&self, cutoff: u64) -> Option<TtlCutoff> {
		let hit = self.blocks.iter().find(|block| block.max_timestamp >= cutoff).map(|block| block.min);
		match hit {
			Some(version) => Some(TtlCutoff::Version(version)),
			None => (!self.blocks.is_empty()).then_some(TtlCutoff::Unbounded),
		}
	}

	pub fn blocks(&self) -> &[BlockModel] {
		&self.blocks
	}

	pub fn blocks_cut(&self) -> u64 {
		self.blocks_cut
	}

	/// What a read buffer must hold to keep every surviving block resident at once.
	pub fn sealed_bytes(&self) -> u64 {
		self.blocks.iter().map(|block| block.bytes).sum()
	}

	pub fn live_bytes(&self) -> u64 {
		self.live_bytes
	}

	pub fn live_len(&self) -> usize {
		self.live.len()
	}

	pub fn versions(&self) -> Vec<Version> {
		self.records.keys().copied().collect()
	}

	pub fn dropped(&self) -> &BTreeSet<Version> {
		&self.dropped
	}

	/// Self-consistency of the model: a drifted model would otherwise turn a real divergence into a confusing
	/// mismatch against nonsense.
	pub fn check_invariants(&self, label: &str, step: u32) {
		let mut previous: Option<&BlockModel> = None;
		for block in &self.blocks {
			assert_eq!(block.min, block.versions[0], "model={label} step={step} block min drifted");
			assert_eq!(
				block.max,
				*block.versions.last().unwrap(),
				"model={label} step={step} block max drifted"
			);
			assert_eq!(
				block.count,
				block.versions.len() as u64,
				"model={label} step={step} block count drifted"
			);
			assert!(
				block.versions.windows(2).all(|w| w[0] < w[1]),
				"model={label} step={step} block versions are not ascending"
			);
			// a drifted sum would silently disarm the read-buffer eviction check that sizes against it
			assert_eq!(
				block.bytes,
				block.versions.iter().map(|version| self.records[version].bytes).sum::<u64>(),
				"model={label} step={step} block bytes drifted"
			);
			if let Some(previous) = previous {
				assert!(
					previous.max < block.min,
					"model={label} step={step} blocks overlap: [{}..{}] then [{}..{}]",
					previous.min,
					previous.max,
					block.min,
					block.max
				);
			}
			previous = Some(block);
		}

		let mut expected: BTreeSet<Version> = self.live.keys().copied().collect();
		for block in &self.blocks {
			for version in &block.versions {
				assert!(
					expected.insert(*version),
					"model={label} step={step} version {version} is held twice"
				);
			}
		}
		let held: BTreeSet<Version> = self.records.keys().copied().collect();
		assert_eq!(held, expected, "model={label} step={step} records and blocks disagree");

		for version in self.records.keys() {
			assert!(
				*version >= self.truncated_before,
				"model={label} step={step} version {version} survives below the floor {}",
				self.truncated_before
			);
		}

		let live_bytes: u64 = self.live.values().map(|record| record.bytes).sum();
		assert_eq!(live_bytes, self.live_bytes, "model={label} step={step} live bytes drifted");
	}
}
