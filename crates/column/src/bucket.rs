// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::series::{Series, SeriesKey, SeriesMetadata, TimestampPrecision};
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

pub fn is_closed(bucket: &Bucket, series: &Series, metadata: &SeriesMetadata, now: DateTime, grace: Duration) -> bool {
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
		} => metadata.newest_key >= bucket.end,
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
		assert!(!is_closed(&b, &s, &meta, DateTime::from_nanos(0), Duration::zero()));
		meta.newest_key = 100;
		assert!(is_closed(&b, &s, &meta, DateTime::from_nanos(0), Duration::zero()));
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
		let bucket_end = DateTime::from_nanos(1_000_000_000);
		assert!(!is_closed(&b, &s, &meta, bucket_end, Duration::from_milliseconds(100).unwrap()));
		let past_grace = DateTime::from_nanos(1_000_000_000 + 250_000_000);
		assert!(is_closed(&b, &s, &meta, past_grace, Duration::from_milliseconds(100).unwrap()));
	}
}
