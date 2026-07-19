// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_value::{byte_size::ByteSize, count::Count, value::duration::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsSample {
	pub scope: Cow<'static, str>,
	pub metric: &'static str,
	pub reading: Reading,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reading {
	Heap(ByteSize),
	Bytes(ByteSize),
	Count(Count),
	Ratio(f64),
	Version(u64),
	Duration(Duration),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadingKind {
	Bytes,
	Count,
	Ratio,
	Duration,
}

impl ReadingKind {
	pub fn reading(self, value: f64) -> Reading {
		match self {
			ReadingKind::Bytes => Reading::Bytes(ByteSize::from_bytes(value as u64)),
			ReadingKind::Count => Reading::Count(Count::new(value as u64)),
			ReadingKind::Ratio => Reading::Ratio(value),
			ReadingKind::Duration => Reading::Duration(
				Duration::from_microseconds(value.min(9.0e15) as i64).unwrap_or_default(),
			),
		}
	}
}

impl MetricsSample {
	pub fn heap(scope: impl Into<Cow<'static, str>>, metric: &'static str, bytes: ByteSize) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Heap(bytes),
		}
	}

	pub fn bytes(scope: impl Into<Cow<'static, str>>, metric: &'static str, bytes: ByteSize) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Bytes(bytes),
		}
	}

	pub fn count(scope: impl Into<Cow<'static, str>>, metric: &'static str, count: u64) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Count(Count::new(count)),
		}
	}

	pub fn ratio(scope: impl Into<Cow<'static, str>>, metric: &'static str, ratio: f64) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Ratio(ratio),
		}
	}

	pub fn version(scope: impl Into<Cow<'static, str>>, metric: &'static str, version: u64) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Version(version),
		}
	}

	pub fn duration(scope: impl Into<Cow<'static, str>>, metric: &'static str, duration: Duration) -> Self {
		Self {
			scope: scope.into(),
			metric,
			reading: Reading::Duration(duration),
		}
	}
}

impl Reading {
	pub fn as_f64(&self) -> f64 {
		match self {
			Reading::Heap(bytes) | Reading::Bytes(bytes) => bytes.as_bytes() as f64,
			Reading::Count(count) => count.as_u64() as f64,
			Reading::Ratio(ratio) => *ratio,
			Reading::Version(version) => *version as f64,
			Reading::Duration(duration) => duration.to_std().as_micros() as f64,
		}
	}

	pub fn unit(&self) -> &'static str {
		match self {
			Reading::Heap(_) | Reading::Bytes(_) => "bytes",
			Reading::Count(_) => "count",
			Reading::Ratio(_) => "ratio",
			Reading::Version(_) => "versions",
			Reading::Duration(_) => "us",
		}
	}

	pub fn heap_bytes(&self) -> Option<u64> {
		match self {
			Reading::Heap(bytes) => Some(bytes.as_bytes()),
			_ => None,
		}
	}
}
