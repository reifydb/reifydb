// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::series::{Series, SeriesKey, SeriesPartitionMetadata, TimestampPrecision};
use reifydb_value::value::{datetime::DateTime, duration::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BucketId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucket {
	pub start: u64,
	pub end: u64,
	pub width: u64,
}

impl Bucket {
	pub fn id(&self) -> BucketId {
		BucketId(self.start)
	}

	pub fn contains(&self, key: u64) -> bool {
		key >= self.start && key < self.end
	}

	pub fn len(&self) -> u64 {
		self.end - self.start
	}

	pub fn is_empty(&self) -> bool {
		self.end == self.start
	}
}

pub fn bucket_for(key: u64, width: u64) -> Bucket {
	assert!(width > 0, "bucket_for: width must be > 0");
	let start = (key / width) * width;
	Bucket {
		start,
		end: start + width,
		width,
	}
}

pub fn is_closed(
	bucket: &Bucket,
	series: &Series,
	metadata: &SeriesPartitionMetadata,
	now: DateTime,
	grace: Duration,
) -> bool {
	match &series.key {
		SeriesKey::DateTime {
			precision,
			..
		} => {
			let bucket_end_wall = to_datetime(bucket.end, *precision);
			now.saturating_duration_since(bucket_end_wall).to_std() > grace.to_std()
		}
		SeriesKey::Integer {
			..
		} => {
			metadata.newest_key >= bucket.end
				|| now.saturating_duration_since(metadata.last_write_at).to_std() > grace.to_std()
		}
	}
}

fn to_datetime(key: u64, precision: TimestampPrecision) -> DateTime {
	let nanos: u128 = match precision {
		TimestampPrecision::Second => (key as u128) * 1_000_000_000,
		TimestampPrecision::Millisecond => (key as u128) * 1_000_000,
		TimestampPrecision::Microsecond => (key as u128) * 1_000,
		TimestampPrecision::Nanosecond => key as u128,
	};
	DateTime::from_nanos(nanos as u64)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::id::{NamespaceId, SeriesId},
	};

	use super::*;

	#[test]
	fn bucket_for_aligns_to_width() {
		let b = bucket_for(137, 100);
		assert_eq!(b.start, 100);
		assert_eq!(b.end, 200);
		assert_eq!(b.width, 100);
		assert!(b.contains(137));
		assert!(!b.contains(200));
		assert_eq!(b.id(), BucketId(100));
	}

	fn series_with(key: SeriesKey) -> Series {
		Series {
			id: SeriesId(1),
			namespace: NamespaceId(1),
			name: "s".into(),
			columns: vec![],
			tag: None,
			key,
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::Processing,
		}
	}

	#[test]
	fn integer_bucket_closed_when_newest_key_advances() {
		let s = series_with(SeriesKey::Integer {
			column: "k".into(),
		});
		let b = Bucket {
			start: 0,
			end: 100,
			width: 100,
		};
		let mut meta = SeriesPartitionMetadata::new();
		meta.newest_key = 99;
		assert!(!is_closed(&b, &s, &meta, DateTime::from_nanos(0), Duration::zero()));
		meta.newest_key = 100;
		assert!(is_closed(&b, &s, &meta, DateTime::from_nanos(0), Duration::zero()));
	}

	#[test]
	fn datetime_bucket_closes_after_grace_elapses() {
		// The bucket bound is in the series precision (ms) while the clock is nanos: end 1000 is 1e9 nanos.
		let s = series_with(SeriesKey::DateTime {
			column: "ts".into(),
			precision: TimestampPrecision::Millisecond,
		});
		let b = Bucket {
			start: 0,
			end: 1000,
			width: 1000,
		};
		let meta = SeriesPartitionMetadata::new();
		let bucket_end = DateTime::from_nanos(1_000_000_000);
		assert!(!is_closed(&b, &s, &meta, bucket_end, Duration::from_milliseconds(100).unwrap()));
		let past_grace = DateTime::from_nanos(1_000_000_000 + 250_000_000);
		assert!(is_closed(&b, &s, &meta, past_grace, Duration::from_milliseconds(100).unwrap()));
	}

	#[test]
	fn integer_bucket_seals_after_grace_with_no_write() {
		// An integer key carries no wall clock meaning, so a partition that goes silent
		// mid-bucket has no key-based path to sealing. Without the last_write_at backstop
		// its rows are stranded out of the column store forever.
		let s = series_with(SeriesKey::Integer {
			column: "k".into(),
		});
		let b = Bucket {
			start: 0,
			end: 100,
			width: 100,
		};
		let mut meta = SeriesPartitionMetadata::new();
		meta.newest_key = 50;
		meta.last_write_at = DateTime::from_nanos(1_000_000_000);
		let grace = Duration::from_milliseconds(100).unwrap();
		let past_grace = DateTime::from_nanos(1_000_000_000 + 250_000_000);
		assert!(is_closed(&b, &s, &meta, past_grace, grace));
	}

	#[test]
	fn integer_bucket_stays_open_before_grace_elapses() {
		// The backstop must not fire early, or a partition that is merely between writes
		// gets its live bucket sealed and every later row lands outside the materialized block.
		let s = series_with(SeriesKey::Integer {
			column: "k".into(),
		});
		let b = Bucket {
			start: 0,
			end: 100,
			width: 100,
		};
		let mut meta = SeriesPartitionMetadata::new();
		meta.newest_key = 50;
		meta.last_write_at = DateTime::from_nanos(1_000_000_000);
		let grace = Duration::from_milliseconds(100).unwrap();
		let within_grace = DateTime::from_nanos(1_000_000_000 + 50_000_000);
		assert!(!is_closed(&b, &s, &meta, within_grace, grace));
	}

	#[test]
	fn a_silent_integer_partition_seals_every_open_bucket() {
		// The backstop reads the partition clock, not the bucket, so it seals all open
		// buckets at once rather than only the oldest. Pinned deliberately: a partition
		// that stopped receiving rows should flush everything it holds.
		let s = series_with(SeriesKey::Integer {
			column: "k".into(),
		});
		let mut meta = SeriesPartitionMetadata::new();
		meta.newest_key = 50;
		meta.last_write_at = DateTime::from_nanos(1_000_000_000);
		let grace = Duration::from_milliseconds(100).unwrap();
		let past_grace = DateTime::from_nanos(1_000_000_000 + 250_000_000);
		for start in [0u64, 100, 200] {
			let b = Bucket {
				start,
				end: start + 100,
				width: 100,
			};
			assert!(
				meta.newest_key < b.end,
				"precondition: bucket {start} must be open under the key rule, or this asserts nothing"
			);
			assert!(is_closed(&b, &s, &meta, past_grace, grace), "bucket {start} must seal");
		}
	}

	#[test]
	fn the_datetime_arm_ignores_metadata_entirely() {
		// Metadata here would seal an integer bucket twice over: the key is past the end and
		// the partition has been silent far longer than grace. The datetime arm must still
		// answer from the bucket end alone, or a busy series seals buckets that are still live.
		let s = series_with(SeriesKey::DateTime {
			column: "ts".into(),
			precision: TimestampPrecision::Millisecond,
		});
		let b = Bucket {
			start: 0,
			end: 1000,
			width: 1000,
		};
		let mut meta = SeriesPartitionMetadata::new();
		meta.newest_key = u64::MAX;
		meta.last_write_at = DateTime::from_nanos(0);
		let grace = Duration::from_milliseconds(100).unwrap();
		let bucket_end = DateTime::from_nanos(1_000_000_000);
		assert!(!is_closed(&b, &s, &meta, bucket_end, grace));
	}
}
