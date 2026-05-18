// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reifydb_core::interface::catalog::series::{Series, SeriesKey, SeriesMetadata, TimestampPrecision};

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
	metadata: &SeriesMetadata,
	now: SystemTime,
	grace: Duration,
) -> bool {
	match &series.key {
		SeriesKey::DateTime {
			precision,
			..
		} => {
			let bucket_end_wall = to_systemtime(bucket.end, *precision);
			now.duration_since(bucket_end_wall).map(|d| d > grace).unwrap_or(false)
		}
		SeriesKey::Integer {
			..
		} => metadata.newest_key >= bucket.end,
	}
}

fn to_systemtime(key: u64, precision: TimestampPrecision) -> SystemTime {
	let nanos: u128 = match precision {
		TimestampPrecision::Second => (key as u128) * 1_000_000_000,
		TimestampPrecision::Millisecond => (key as u128) * 1_000_000,
		TimestampPrecision::Microsecond => (key as u128) * 1_000,
		TimestampPrecision::Nanosecond => key as u128,
	};
	UNIX_EPOCH + Duration::from_nanos(nanos as u64)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, SeriesId};

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
			underlying: false,
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
		let mut meta = SeriesMetadata::new(s.id);
		meta.newest_key = 99;
		assert!(!is_closed(&b, &s, &meta, SystemTime::now(), Duration::ZERO));
		meta.newest_key = 100;
		assert!(is_closed(&b, &s, &meta, SystemTime::now(), Duration::ZERO));
	}

	#[test]
	fn datetime_bucket_closes_after_grace_elapses() {
		let s = series_with(SeriesKey::DateTime {
			column: "ts".into(),
			precision: TimestampPrecision::Millisecond,
		});
		// bucket.end at 1000ms past epoch
		let b = Bucket {
			start: 0,
			end: 1000,
			width: 1000,
		};
		let meta = SeriesMetadata::new(s.id);
		let bucket_end = UNIX_EPOCH + Duration::from_millis(1000);
		assert!(!is_closed(&b, &s, &meta, bucket_end, Duration::from_millis(100)));
		let past_grace = bucket_end + Duration::from_millis(250);
		assert!(is_closed(&b, &s, &meta, past_grace, Duration::from_millis(100)));
	}
}
