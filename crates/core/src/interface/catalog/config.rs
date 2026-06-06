// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{fmt, str::FromStr};

use reifydb_value::value::{
	Value, decimal::Decimal, duration::Duration, int::Int, ordered_f32::OrderedF32, ordered_f64::OrderedF64,
	uint::Uint, value_type::ValueType,
};

use crate::common::CommitVersion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptError {
	TypeMismatch {
		expected: Vec<ValueType>,
		actual: ValueType,
	},

	InvalidValue(String),
}

impl fmt::Display for AcceptError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::TypeMismatch {
				expected,
				actual,
			} => {
				write!(f, "expected one of {:?}, got {:?}", expected, actual)
			}
			Self::InvalidValue(reason) => write!(f, "{reason}"),
		}
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConfigKey {
	OracleWindowSize,
	OracleWaterMark,
	QueryRowBatchSize,
	RowTtlScanBatchSize,
	RowTtlScanInterval,
	OperatorTtlScanBatchSize,
	OperatorTtlScanInterval,
	VersionEpochSampleInterval,
	HistoricalGcBatchSize,
	HistoricalGcInterval,
	CdcTtlDuration,
	CdcCompactInterval,
	CdcCompactBlockSize,
	CdcCompactSafetyLag,
	CdcCompactMaxBlocksPerTick,
	CdcCompactBlockCacheCapacity,
	CdcCompactZstdLevel,
	CdcRecentCacheCapacity,
	MultiReadBufferPages,
	MultiReadBufferPageSize,
	FlowTick,
	CdcWatermarkWaitTimeout,
	FlowJoinProbeBlockSize,
	ThreadsAsync,
	ThreadsSystem,
	ThreadsQuery,
	ThreadsCommit,
	ThreadsBackground,
	FlowWorkerThreads,
	SubscriptionWorkerThreads,
	RuntimeMetricsInterval,
	MetricFlushInterval,
}

impl ConfigKey {
	pub fn all() -> &'static [Self] {
		&[
			Self::OracleWindowSize,
			Self::OracleWaterMark,
			Self::QueryRowBatchSize,
			Self::RowTtlScanBatchSize,
			Self::RowTtlScanInterval,
			Self::OperatorTtlScanBatchSize,
			Self::OperatorTtlScanInterval,
			Self::VersionEpochSampleInterval,
			Self::HistoricalGcBatchSize,
			Self::HistoricalGcInterval,
			Self::CdcTtlDuration,
			Self::CdcCompactInterval,
			Self::CdcCompactBlockSize,
			Self::CdcCompactSafetyLag,
			Self::CdcCompactMaxBlocksPerTick,
			Self::CdcCompactBlockCacheCapacity,
			Self::CdcCompactZstdLevel,
			Self::CdcRecentCacheCapacity,
			Self::MultiReadBufferPages,
			Self::MultiReadBufferPageSize,
			Self::FlowTick,
			Self::CdcWatermarkWaitTimeout,
			Self::FlowJoinProbeBlockSize,
			Self::ThreadsAsync,
			Self::ThreadsSystem,
			Self::ThreadsQuery,
			Self::ThreadsCommit,
			Self::ThreadsBackground,
			Self::FlowWorkerThreads,
			Self::SubscriptionWorkerThreads,
			Self::RuntimeMetricsInterval,
			Self::MetricFlushInterval,
		]
	}

	pub fn default_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(500),
			Self::OracleWaterMark => Value::Uint8(20),
			Self::QueryRowBatchSize => Value::Uint2(32),
			Self::RowTtlScanBatchSize => Value::Uint8(10000),
			Self::RowTtlScanInterval => Value::duration_seconds(60),
			Self::OperatorTtlScanBatchSize => Value::Uint8(10000),
			Self::OperatorTtlScanInterval => Value::duration_seconds(60),
			Self::VersionEpochSampleInterval => Value::duration_seconds(1),
			Self::HistoricalGcBatchSize => Value::Uint8(50_000),
			Self::HistoricalGcInterval => Value::duration_seconds(30),
			Self::CdcTtlDuration => Value::None {
				inner: ValueType::Duration,
			},
			Self::CdcCompactInterval => Value::duration_seconds(60),
			Self::CdcCompactBlockSize => Value::Uint8(1024),
			Self::CdcCompactSafetyLag => Value::Uint8(1024),
			Self::CdcCompactMaxBlocksPerTick => Value::Uint8(16),
			Self::CdcCompactBlockCacheCapacity => Value::Uint8(8),
			Self::CdcCompactZstdLevel => Value::Uint1(7),
			Self::CdcRecentCacheCapacity => Value::Uint8(128),
			Self::MultiReadBufferPages => Value::Uint8(1024),
			Self::MultiReadBufferPageSize => Value::Uint8(65536),
			Self::FlowTick => Value::duration_seconds(1),
			Self::CdcWatermarkWaitTimeout => Value::duration_seconds(1),
			Self::FlowJoinProbeBlockSize => Value::Uint8(1024),
			Self::ThreadsAsync => Value::Uint2(1),
			Self::ThreadsSystem => Value::Uint2(2),
			Self::ThreadsQuery => Value::Uint2(1),
			Self::ThreadsCommit => Value::Uint2(2),
			Self::ThreadsBackground => Value::Uint2(1),
			Self::FlowWorkerThreads => Value::Uint2(0),
			Self::SubscriptionWorkerThreads => Value::Uint2(0),
			Self::RuntimeMetricsInterval => Value::duration_seconds(5),
			Self::MetricFlushInterval => Value::duration_seconds(10),
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::OracleWaterMark => "Number of conflict windows retained before cleanup is triggered.",
			Self::QueryRowBatchSize => {
				"Number of rows produced per batch by query / DML pipeline operators."
			}
			Self::RowTtlScanBatchSize => "Max rows to examine per batch during a row TTL scan.",
			Self::RowTtlScanInterval => "How often the row TTL actor should scan for expired rows.",
			Self::OperatorTtlScanBatchSize => {
				"Max rows to examine per batch during an operator-state TTL scan."
			}
			Self::OperatorTtlScanInterval => {
				"How often the operator-state TTL actor should scan for expired rows."
			}
			Self::VersionEpochSampleInterval => {
				"How often the version-epoch sampler records a (wall-clock, commit version) sample used to map a TTL duration to a cutoff version."
			}
			Self::HistoricalGcBatchSize => {
				"Max historical (key, version) pairs scanned per shape per historical GC tick."
			}
			Self::HistoricalGcInterval => {
				"How often the historical-version GC actor sweeps __historical for versions older than the oracle read watermark."
			}
			Self::CdcTtlDuration => {
				"Maximum age of CDC entries before eviction. When unset, CDC is retained forever; \
				 when set, must be > 0 and entries older than this duration are evicted regardless \
				 of consumer state."
			}
			Self::CdcCompactInterval => "How often the CDC compaction actor runs.",
			Self::CdcCompactBlockSize => "Number of CDC entries packed into one compressed block.",
			Self::CdcCompactSafetyLag => "Versions newer than (max_version - lag) are never compacted.",
			Self::CdcCompactMaxBlocksPerTick => {
				"Upper bound on consecutive blocks produced per actor tick."
			}
			Self::CdcCompactBlockCacheCapacity => {
				"Number of decompressed CDC blocks held in the in-memory LRU cache."
			}
			Self::CdcCompactZstdLevel => {
				"Zstd compression level for CDC blocks. Range 1-22; higher means smaller blocks but \
				 slower compression. Decompression cost is independent of level."
			}
			Self::CdcRecentCacheCapacity => {
				"Number of most-recent decoded CDC entries held in memory so a caught-up consumer \
				 is served without re-reading and re-deserializing from the backend."
			}
			Self::MultiReadBufferPages => {
				"Number of pages (contiguous row-number buckets) the multi-version read cache keeps \
				 resident before eviction. Raising it trades RAM for fewer persistent-tier reads."
			}
			Self::MultiReadBufferPageSize => {
				"Number of rows per cached page (bucket) in the multi-version read cache. Must be a \
				 power of two; sets the granularity of whole-page read-ahead and completeness tracking."
			}
			Self::FlowTick => {
				"How often the deferred and transactional flow tick coordinators wake up to dispatch \
				 due flows."
			}
			Self::CdcWatermarkWaitTimeout => {
				"Backstop timeout for the CDC consumer's wait for the transaction watermark to reach the \
				 latest commit before consuming; catch-up is event-driven, so this only bounds a missed \
				 wakeup. Must be > 0."
			}
			Self::FlowJoinProbeBlockSize => {
				"Number of opposite-side rows a streaming join pulls per block when probing its stored \
				 state. Bounds resident probe memory without dropping matches; smaller trades fewer \
				 resident rows for more scan round-trips."
			}
			Self::ThreadsAsync => {
				"Number of worker threads for the async runtime. Must be >= 1. \
				 Read at boot before the runtime starts; changes require restart."
			}
			Self::ThreadsSystem => {
				"Number of worker threads for the system pool (lightweight actors). \
				 Must be >= 1. Changes require restart."
			}
			Self::ThreadsQuery => {
				"Number of worker threads for the query pool (execution-heavy actors). \
				 Must be >= 1. Changes require restart."
			}
			Self::ThreadsCommit => {
				"Number of worker threads for the commit pool (synchronous pre-commit flow execution). \
				 Must be >= 1. Changes require restart."
			}
			Self::ThreadsBackground => {
				"Number of worker threads for the background pool (non-critical cleanup and metrics actors). \
				 Must be >= 1. Changes require restart."
			}
			Self::FlowWorkerThreads => {
				"Number of deferred-flow worker actors that maintain deferred views in parallel. \
				 0 means auto (size to the system thread pool). Higher values raise fan-out parallelism \
				 for many independent views. Changes require restart."
			}
			Self::SubscriptionWorkerThreads => {
				"Number of subscription worker actors that fan out CDC changes to ephemeral \
				 subscriptions in parallel. 0 means auto (size to the system thread pool). Higher values \
				 raise fan-out parallelism for many concurrent subscriptions. Changes require restart."
			}
			Self::RuntimeMetricsInterval => {
				"How often the runtime-metrics sampler records a memory snapshot into \
				 system::metrics::runtime::memory::snapshots. When unset, the history sampler is \
				 dormant and only the live ::current view is available; when set, must be > 0."
			}
			Self::MetricFlushInterval => {
				"How often the metric collector flushes accumulated storage and CDC stats into the \
				 system::metrics views. Must be > 0."
			}
		}
	}

	pub fn requires_restart(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::QueryRowBatchSize => false,
			Self::RowTtlScanBatchSize => false,
			Self::RowTtlScanInterval => false,
			Self::OperatorTtlScanBatchSize => false,
			Self::OperatorTtlScanInterval => false,
			Self::VersionEpochSampleInterval => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => false,
			Self::CdcCompactInterval => false,
			Self::CdcCompactBlockSize => false,
			Self::CdcCompactSafetyLag => false,
			Self::CdcCompactMaxBlocksPerTick => false,
			Self::CdcCompactBlockCacheCapacity => true,
			Self::CdcCompactZstdLevel => false,
			Self::CdcRecentCacheCapacity => true,
			Self::MultiReadBufferPages => true,
			Self::MultiReadBufferPageSize => true,
			Self::FlowTick => false,
			Self::CdcWatermarkWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => true,
			Self::ThreadsSystem => true,
			Self::ThreadsQuery => true,
			Self::ThreadsCommit => true,
			Self::ThreadsBackground => true,
			Self::FlowWorkerThreads => true,
			Self::SubscriptionWorkerThreads => true,
			Self::RuntimeMetricsInterval => false,
			Self::MetricFlushInterval => false,
		}
	}

	pub fn expected_types(&self) -> &'static [ValueType] {
		match self {
			Self::OracleWindowSize => &[ValueType::Uint8],
			Self::OracleWaterMark => &[ValueType::Uint8],
			Self::QueryRowBatchSize => &[ValueType::Uint2],
			Self::RowTtlScanBatchSize => &[ValueType::Uint8],
			Self::RowTtlScanInterval => &[ValueType::Duration],
			Self::OperatorTtlScanBatchSize => &[ValueType::Uint8],
			Self::OperatorTtlScanInterval => &[ValueType::Duration],
			Self::VersionEpochSampleInterval => &[ValueType::Duration],
			Self::HistoricalGcBatchSize => &[ValueType::Uint8],
			Self::HistoricalGcInterval => &[ValueType::Duration],
			Self::CdcTtlDuration => &[ValueType::Duration],
			Self::CdcCompactInterval => &[ValueType::Duration],
			Self::CdcCompactBlockSize => &[ValueType::Uint8],
			Self::CdcCompactSafetyLag => &[ValueType::Uint8],
			Self::CdcCompactMaxBlocksPerTick => &[ValueType::Uint8],
			Self::CdcCompactBlockCacheCapacity => &[ValueType::Uint8],
			Self::CdcCompactZstdLevel => &[ValueType::Uint1],
			Self::CdcRecentCacheCapacity => &[ValueType::Uint8],
			Self::MultiReadBufferPages => &[ValueType::Uint8],
			Self::MultiReadBufferPageSize => &[ValueType::Uint8],
			Self::FlowTick => &[ValueType::Duration],
			Self::CdcWatermarkWaitTimeout => &[ValueType::Duration],
			Self::FlowJoinProbeBlockSize => &[ValueType::Uint8],
			Self::ThreadsAsync => &[ValueType::Uint2],
			Self::ThreadsSystem => &[ValueType::Uint2],
			Self::ThreadsQuery => &[ValueType::Uint2],
			Self::ThreadsCommit => &[ValueType::Uint2],
			Self::ThreadsBackground => &[ValueType::Uint2],
			Self::FlowWorkerThreads => &[ValueType::Uint2],
			Self::SubscriptionWorkerThreads => &[ValueType::Uint2],
			Self::RuntimeMetricsInterval => &[ValueType::Duration],
			Self::MetricFlushInterval => &[ValueType::Duration],
		}
	}

	pub fn is_optional(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::QueryRowBatchSize => false,
			Self::RowTtlScanBatchSize => false,
			Self::RowTtlScanInterval => false,
			Self::OperatorTtlScanBatchSize => false,
			Self::OperatorTtlScanInterval => false,
			Self::VersionEpochSampleInterval => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => true,
			Self::CdcCompactInterval => false,
			Self::CdcCompactBlockSize => false,
			Self::CdcCompactSafetyLag => false,
			Self::CdcCompactMaxBlocksPerTick => false,
			Self::CdcCompactBlockCacheCapacity => false,
			Self::CdcCompactZstdLevel => false,
			Self::CdcRecentCacheCapacity => false,
			Self::MultiReadBufferPages => false,
			Self::MultiReadBufferPageSize => false,
			Self::FlowTick => false,
			Self::CdcWatermarkWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => false,
			Self::ThreadsSystem => false,
			Self::ThreadsQuery => false,
			Self::ThreadsCommit => false,
			Self::ThreadsBackground => false,
			Self::FlowWorkerThreads => false,
			Self::SubscriptionWorkerThreads => false,
			Self::RuntimeMetricsInterval => true,
			Self::MetricFlushInterval => false,
		}
	}

	fn validate_canonical(&self, value: &Value) -> Result<(), String> {
		match self {
			Self::CdcTtlDuration => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("CDC_TTL_DURATION must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::CdcCompactInterval => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("CDC_COMPACT_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::CdcCompactBlockSize => match value {
				Value::Uint8(0) => Err("CDC_COMPACT_BLOCK_SIZE must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::QueryRowBatchSize => match value {
				Value::Uint2(0) => Err("QUERY_ROW_BATCH_SIZE must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CdcCompactBlockCacheCapacity => match value {
				Value::Uint8(0) => {
					Err("CDC_COMPACT_BLOCK_CACHE_CAPACITY must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::MultiReadBufferPages => match value {
				Value::Uint8(0) => Err("MULTI_READ_BUFFER_PAGES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiReadBufferPageSize => match value {
				Value::Uint8(v) if v.is_power_of_two() => Ok(()),
				Value::Uint8(_) => {
					Err("MULTI_READ_BUFFER_PAGE_SIZE must be a power of two".to_string())
				}
				_ => Ok(()),
			},
			Self::CdcCompactZstdLevel => match value {
				Value::Uint1(v) if (1..=22).contains(v) => Ok(()),
				Value::Uint1(_) => Err("CDC_COMPACT_ZSTD_LEVEL must be in [1, 22]".to_string()),
				_ => Ok(()),
			},
			Self::HistoricalGcBatchSize => match value {
				Value::Uint8(0) => {
					Err("HISTORICAL_GC_BATCH_SIZE must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::HistoricalGcInterval => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("HISTORICAL_GC_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::FlowTick => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("FLOW_TICK must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::CdcWatermarkWaitTimeout => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("CDC_WATERMARK_WAIT_TIMEOUT must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::FlowJoinProbeBlockSize => match value {
				Value::Uint8(0) => {
					Err("FLOW_JOIN_PROBE_BLOCK_SIZE must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::ThreadsAsync => match value {
				Value::Uint2(0) => Err("THREADS_ASYNC must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsSystem => match value {
				Value::Uint2(0) => Err("THREADS_SYSTEM must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsQuery => match value {
				Value::Uint2(0) => Err("THREADS_QUERY must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsCommit => match value {
				Value::Uint2(0) => Err("THREADS_COMMIT must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsBackground => match value {
				Value::Uint2(0) => Err("THREADS_BACKGROUND must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::FlowWorkerThreads => Ok(()),
			Self::SubscriptionWorkerThreads => Ok(()),
			Self::RuntimeMetricsInterval => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("RUNTIME_METRICS_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::MetricFlushInterval => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRIC_FLUSH_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			_ => Ok(()),
		}
	}

	pub fn accept(&self, value: Value) -> Result<Value, AcceptError> {
		if let Value::None {
			inner,
		} = &value
		{
			if self.is_optional() && self.expected_types().contains(inner) {
				return Ok(value);
			}
			return Err(AcceptError::TypeMismatch {
				expected: self.expected_types().to_vec(),
				actual: value.get_type(),
			});
		}

		let canonical = if self.expected_types().contains(&value.get_type()) {
			value
		} else {
			try_coerce_numeric(&value, self.expected_types()).ok_or_else(|| AcceptError::TypeMismatch {
				expected: self.expected_types().to_vec(),
				actual: value.get_type(),
			})?
		};

		self.validate_canonical(&canonical).map_err(AcceptError::InvalidValue)?;
		Ok(canonical)
	}
}

fn try_coerce_numeric(value: &Value, expected: &[ValueType]) -> Option<Value> {
	for target in expected {
		let coerced = match target {
			ValueType::Uint1 => {
				value.to_usize().filter(|&v| v <= u8::MAX as usize).map(|v| Value::Uint1(v as u8))
			}
			ValueType::Uint2 => {
				value.to_usize().filter(|&v| v <= u16::MAX as usize).map(|v| Value::Uint2(v as u16))
			}
			ValueType::Uint4 => {
				value.to_usize().filter(|&v| v <= u32::MAX as usize).map(|v| Value::Uint4(v as u32))
			}
			ValueType::Uint8 => {
				value.to_usize().filter(|&v| v <= u64::MAX as usize).map(|v| Value::Uint8(v as u64))
			}
			ValueType::Uint16 => value.to_usize().map(|v| Value::Uint16(v as u128)),
			ValueType::Int1 => {
				value.to_usize().filter(|&v| v <= i8::MAX as usize).map(|v| Value::Int1(v as i8))
			}
			ValueType::Int2 => {
				value.to_usize().filter(|&v| v <= i16::MAX as usize).map(|v| Value::Int2(v as i16))
			}
			ValueType::Int4 => {
				value.to_usize().filter(|&v| v <= i32::MAX as usize).map(|v| Value::Int4(v as i32))
			}
			ValueType::Int8 => {
				value.to_usize().filter(|&v| v <= i64::MAX as usize).map(|v| Value::Int8(v as i64))
			}
			ValueType::Int16 => {
				value.to_usize().filter(|&v| v <= i128::MAX as usize).map(|v| Value::Int16(v as i128))
			}
			ValueType::Uint => value.to_usize().map(|v| Value::Uint(Uint::from_u64(v as u64))),
			ValueType::Int => value.to_usize().map(|v| Value::Int(Int::from_i64(v as i64))),
			ValueType::Decimal => value.to_usize().map(|v| Value::Decimal(Decimal::from_i64(v as i64))),
			ValueType::Float4 => {
				value.to_usize().and_then(|v| OrderedF32::try_from(v as f32).ok()).map(Value::Float4)
			}
			ValueType::Float8 => {
				value.to_usize().and_then(|v| OrderedF64::try_from(v as f64).ok()).map(Value::Float8)
			}
			ValueType::Duration => value
				.to_usize()
				.and_then(|v| Duration::from_seconds(v as i64).ok())
				.map(Value::Duration),
			_ => None,
		};
		if coerced.is_some() {
			return coerced;
		}
	}
	None
}

impl fmt::Display for ConfigKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OracleWindowSize => write!(f, "ORACLE_WINDOW_SIZE"),
			Self::OracleWaterMark => write!(f, "ORACLE_WATER_MARK"),
			Self::QueryRowBatchSize => write!(f, "QUERY_ROW_BATCH_SIZE"),
			Self::RowTtlScanBatchSize => write!(f, "ROW_TTL_SCAN_BATCH_SIZE"),
			Self::RowTtlScanInterval => write!(f, "ROW_TTL_SCAN_INTERVAL"),
			Self::OperatorTtlScanBatchSize => write!(f, "OPERATOR_TTL_SCAN_BATCH_SIZE"),
			Self::OperatorTtlScanInterval => write!(f, "OPERATOR_TTL_SCAN_INTERVAL"),
			Self::VersionEpochSampleInterval => write!(f, "VERSION_EPOCH_SAMPLE_INTERVAL"),
			Self::HistoricalGcBatchSize => write!(f, "HISTORICAL_GC_BATCH_SIZE"),
			Self::HistoricalGcInterval => write!(f, "HISTORICAL_GC_INTERVAL"),
			Self::CdcTtlDuration => write!(f, "CDC_TTL_DURATION"),
			Self::CdcCompactInterval => write!(f, "CDC_COMPACT_INTERVAL"),
			Self::CdcCompactBlockSize => write!(f, "CDC_COMPACT_BLOCK_SIZE"),
			Self::CdcCompactSafetyLag => write!(f, "CDC_COMPACT_SAFETY_LAG"),
			Self::CdcCompactMaxBlocksPerTick => write!(f, "CDC_COMPACT_MAX_BLOCKS_PER_TICK"),
			Self::CdcCompactBlockCacheCapacity => write!(f, "CDC_COMPACT_BLOCK_CACHE_CAPACITY"),
			Self::CdcCompactZstdLevel => write!(f, "CDC_COMPACT_ZSTD_LEVEL"),
			Self::CdcRecentCacheCapacity => write!(f, "CDC_RECENT_CACHE_CAPACITY"),
			Self::MultiReadBufferPages => write!(f, "MULTI_READ_BUFFER_PAGES"),
			Self::MultiReadBufferPageSize => write!(f, "MULTI_READ_BUFFER_PAGE_SIZE"),
			Self::FlowTick => write!(f, "FLOW_TICK"),
			Self::CdcWatermarkWaitTimeout => write!(f, "CDC_WATERMARK_WAIT_TIMEOUT"),
			Self::FlowJoinProbeBlockSize => write!(f, "FLOW_JOIN_PROBE_BLOCK_SIZE"),
			Self::ThreadsAsync => write!(f, "THREADS_ASYNC"),
			Self::ThreadsSystem => write!(f, "THREADS_SYSTEM"),
			Self::ThreadsQuery => write!(f, "THREADS_QUERY"),
			Self::ThreadsCommit => write!(f, "THREADS_COMMIT"),
			Self::ThreadsBackground => write!(f, "THREADS_BACKGROUND"),
			Self::FlowWorkerThreads => write!(f, "FLOW_WORKER_THREADS"),
			Self::SubscriptionWorkerThreads => write!(f, "SUBSCRIPTION_WORKER_THREADS"),
			Self::RuntimeMetricsInterval => write!(f, "RUNTIME_METRICS_INTERVAL"),
			Self::MetricFlushInterval => write!(f, "METRIC_FLUSH_INTERVAL"),
		}
	}
}

impl FromStr for ConfigKey {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ORACLE_WINDOW_SIZE" => Ok(Self::OracleWindowSize),
			"ORACLE_WATER_MARK" => Ok(Self::OracleWaterMark),
			"QUERY_ROW_BATCH_SIZE" => Ok(Self::QueryRowBatchSize),
			"ROW_TTL_SCAN_BATCH_SIZE" => Ok(Self::RowTtlScanBatchSize),
			"ROW_TTL_SCAN_INTERVAL" => Ok(Self::RowTtlScanInterval),
			"OPERATOR_TTL_SCAN_BATCH_SIZE" => Ok(Self::OperatorTtlScanBatchSize),
			"OPERATOR_TTL_SCAN_INTERVAL" => Ok(Self::OperatorTtlScanInterval),
			"VERSION_EPOCH_SAMPLE_INTERVAL" => Ok(Self::VersionEpochSampleInterval),
			"HISTORICAL_GC_BATCH_SIZE" => Ok(Self::HistoricalGcBatchSize),
			"HISTORICAL_GC_INTERVAL" => Ok(Self::HistoricalGcInterval),
			"CDC_TTL_DURATION" => Ok(Self::CdcTtlDuration),
			"CDC_COMPACT_INTERVAL" => Ok(Self::CdcCompactInterval),
			"CDC_COMPACT_BLOCK_SIZE" => Ok(Self::CdcCompactBlockSize),
			"CDC_COMPACT_SAFETY_LAG" => Ok(Self::CdcCompactSafetyLag),
			"CDC_COMPACT_MAX_BLOCKS_PER_TICK" => Ok(Self::CdcCompactMaxBlocksPerTick),
			"CDC_COMPACT_BLOCK_CACHE_CAPACITY" => Ok(Self::CdcCompactBlockCacheCapacity),
			"CDC_COMPACT_ZSTD_LEVEL" => Ok(Self::CdcCompactZstdLevel),
			"CDC_RECENT_CACHE_CAPACITY" => Ok(Self::CdcRecentCacheCapacity),
			"MULTI_READ_BUFFER_PAGES" => Ok(Self::MultiReadBufferPages),
			"MULTI_READ_BUFFER_PAGE_SIZE" => Ok(Self::MultiReadBufferPageSize),
			"FLOW_TICK" => Ok(Self::FlowTick),
			"CDC_WATERMARK_WAIT_TIMEOUT" => Ok(Self::CdcWatermarkWaitTimeout),
			"FLOW_JOIN_PROBE_BLOCK_SIZE" => Ok(Self::FlowJoinProbeBlockSize),
			"THREADS_ASYNC" => Ok(Self::ThreadsAsync),
			"THREADS_SYSTEM" => Ok(Self::ThreadsSystem),
			"THREADS_QUERY" => Ok(Self::ThreadsQuery),
			"THREADS_COMMIT" => Ok(Self::ThreadsCommit),
			"THREADS_BACKGROUND" => Ok(Self::ThreadsBackground),
			"FLOW_WORKER_THREADS" => Ok(Self::FlowWorkerThreads),
			"SUBSCRIPTION_WORKER_THREADS" => Ok(Self::SubscriptionWorkerThreads),
			"RUNTIME_METRICS_INTERVAL" => Ok(Self::RuntimeMetricsInterval),
			"METRIC_FLUSH_INTERVAL" => Ok(Self::MetricFlushInterval),
			_ => Err(format!("Unknown system configuration key: {}", s)),
		}
	}
}

#[derive(Debug, Clone)]
pub struct Config {
	pub key: ConfigKey,

	pub value: Value,

	pub default_value: Value,

	pub description: &'static str,

	pub requires_restart: bool,
}

pub trait GetConfig: Send + Sync {
	fn get_config(&self, key: ConfigKey) -> Value;

	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value;

	fn get_config_uint8(&self, key: ConfigKey) -> u64 {
		let val = self.get_config(key);
		match val {
			Value::Uint8(v) => v,
			v => panic!("config key '{}' expected Uint8, got {:?}", key, v),
		}
	}

	fn get_config_uint1(&self, key: ConfigKey) -> u8 {
		let val = self.get_config(key);
		match val {
			Value::Uint1(v) => v,
			v => panic!("config key '{}' expected Uint1, got {:?}", key, v),
		}
	}

	fn get_config_uint2(&self, key: ConfigKey) -> u16 {
		let val = self.get_config(key);
		match val {
			Value::Uint2(v) => v,
			v => panic!("config key '{}' expected Uint2, got {:?}", key, v),
		}
	}

	fn get_config_duration(&self, key: ConfigKey) -> Duration {
		let val = self.get_config(key);
		match val {
			Value::Duration(v) => v,
			v => panic!("config key '{}' expected Duration, got {:?}", key, v),
		}
	}

	fn get_config_duration_opt(&self, key: ConfigKey) -> Option<Duration> {
		match self.get_config(key) {
			Value::None {
				..
			} => None,
			Value::Duration(v) => Some(v),
			v => panic!("config key '{}' expected Duration or None, got {:?}", key, v),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_cdc_ttl_default_is_typed_null() {
		// Defaulting to Value::None means "TTL not configured" - producer skips cleanup.
		let default = ConfigKey::CdcTtlDuration.default_value();
		assert!(matches!(
			default,
			Value::None {
				inner: ValueType::Duration
			}
		));
	}

	#[test]
	fn test_cdc_ttl_accept_passes_typed_null() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		let v = ConfigKey::CdcTtlDuration.accept(none.clone()).unwrap();
		assert_eq!(v, none);
	}

	#[test]
	fn test_cdc_ttl_accept_passes_positive_duration() {
		let one_sec = Value::duration_seconds(1);
		assert_eq!(ConfigKey::CdcTtlDuration.accept(one_sec.clone()).unwrap(), one_sec);

		let one_hour = Value::duration_seconds(3600);
		assert_eq!(ConfigKey::CdcTtlDuration.accept(one_hour.clone()).unwrap(), one_hour);
	}

	#[test]
	fn test_cdc_ttl_accept_rejects_zero() {
		let zero = Value::duration_seconds(0);
		match ConfigKey::CdcTtlDuration.accept(zero).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_cdc_ttl_accept_rejects_negative() {
		let negative = Value::duration_seconds(-5);
		assert!(matches!(ConfigKey::CdcTtlDuration.accept(negative), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_other_keys_accept_in_type_values() {
		// Keys without bespoke validation should accept any in-type value.
		assert!(ConfigKey::OracleWindowSize.accept(Value::Uint8(0)).is_ok());
		assert!(ConfigKey::RowTtlScanInterval.accept(Value::duration_seconds(0)).is_ok());
	}

	#[test]
	fn test_cdc_ttl_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_TTL_DURATION".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcTtlDuration);
		assert_eq!(format!("{}", ConfigKey::CdcTtlDuration), "CDC_TTL_DURATION");
	}

	#[test]
	fn test_cdc_ttl_in_all() {
		assert!(ConfigKey::all().contains(&ConfigKey::CdcTtlDuration));
	}

	#[test]
	fn test_all_contains_every_compact_key_and_has_expected_len() {
		let all = ConfigKey::all();
		assert_eq!(all.len(), 32);
		assert!(all.contains(&ConfigKey::VersionEpochSampleInterval));
		assert!(all.contains(&ConfigKey::CdcWatermarkWaitTimeout));
		assert!(all.contains(&ConfigKey::FlowJoinProbeBlockSize));
		assert!(all.contains(&ConfigKey::CdcCompactInterval));
		assert!(all.contains(&ConfigKey::CdcCompactBlockSize));
		assert!(all.contains(&ConfigKey::CdcCompactSafetyLag));
		assert!(all.contains(&ConfigKey::CdcCompactMaxBlocksPerTick));
		assert!(all.contains(&ConfigKey::CdcCompactBlockCacheCapacity));
		assert!(all.contains(&ConfigKey::CdcCompactZstdLevel));
		assert!(all.contains(&ConfigKey::CdcRecentCacheCapacity));
		assert!(all.contains(&ConfigKey::MultiReadBufferPages));
		assert!(all.contains(&ConfigKey::MultiReadBufferPageSize));
		assert!(all.contains(&ConfigKey::QueryRowBatchSize));
		assert!(all.contains(&ConfigKey::ThreadsAsync));
		assert!(all.contains(&ConfigKey::ThreadsSystem));
		assert!(all.contains(&ConfigKey::ThreadsQuery));
		assert!(all.contains(&ConfigKey::ThreadsCommit));
		assert!(all.contains(&ConfigKey::ThreadsBackground));
		assert!(all.contains(&ConfigKey::RuntimeMetricsInterval));
		assert!(all.contains(&ConfigKey::MetricFlushInterval));
		assert!(all.contains(&ConfigKey::SubscriptionWorkerThreads));
	}

	#[test]
	fn test_runtime_metrics_interval_metadata() {
		// Single optional Duration knob: default on (5s), none disables the history sampler.
		assert_eq!(ConfigKey::RuntimeMetricsInterval.default_value(), Value::duration_seconds(5));
		assert_eq!(ConfigKey::RuntimeMetricsInterval.expected_types(), &[ValueType::Duration]);
		assert!(ConfigKey::RuntimeMetricsInterval.is_optional());
	}

	#[test]
	fn test_runtime_metrics_interval_round_trip() {
		assert_eq!("RUNTIME_METRICS_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::RuntimeMetricsInterval);
		assert_eq!(format!("{}", ConfigKey::RuntimeMetricsInterval), "RUNTIME_METRICS_INTERVAL");
	}

	#[test]
	fn test_runtime_metrics_interval_accepts_none_and_positive_rejects_zero() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(ConfigKey::RuntimeMetricsInterval.accept(none.clone()).unwrap(), none);

		let five = Value::duration_seconds(5);
		assert_eq!(ConfigKey::RuntimeMetricsInterval.accept(five.clone()).unwrap(), five);

		let zero = Value::duration_seconds(0);
		assert!(matches!(ConfigKey::RuntimeMetricsInterval.accept(zero), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_metric_flush_interval_metadata() {
		// Always-on (non-optional) Duration knob defaulting to the historical 10s flush cadence.
		assert_eq!(ConfigKey::MetricFlushInterval.default_value(), Value::duration_seconds(10));
		assert_eq!(ConfigKey::MetricFlushInterval.expected_types(), &[ValueType::Duration]);
		assert!(!ConfigKey::MetricFlushInterval.is_optional());
		assert!(!ConfigKey::MetricFlushInterval.requires_restart());
	}

	#[test]
	fn test_metric_flush_interval_round_trip() {
		assert_eq!("METRIC_FLUSH_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::MetricFlushInterval);
		assert_eq!(format!("{}", ConfigKey::MetricFlushInterval), "METRIC_FLUSH_INTERVAL");
	}

	#[test]
	fn test_metric_flush_interval_accepts_positive_rejects_zero() {
		let ten = Value::duration_seconds(10);
		assert_eq!(ConfigKey::MetricFlushInterval.accept(ten.clone()).unwrap(), ten);

		let zero = Value::duration_seconds(0);
		assert!(matches!(ConfigKey::MetricFlushInterval.accept(zero), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_cdc_recent_cache_capacity_round_trip() {
		assert_eq!(
			"CDC_RECENT_CACHE_CAPACITY".parse::<ConfigKey>().unwrap(),
			ConfigKey::CdcRecentCacheCapacity
		);
		assert_eq!(format!("{}", ConfigKey::CdcRecentCacheCapacity), "CDC_RECENT_CACHE_CAPACITY");
	}

	#[test]
	fn test_cdc_recent_cache_capacity_metadata() {
		assert_eq!(ConfigKey::CdcRecentCacheCapacity.default_value(), Value::Uint8(128));
		assert_eq!(ConfigKey::CdcRecentCacheCapacity.expected_types(), &[ValueType::Uint8]);
		assert!(ConfigKey::CdcRecentCacheCapacity.requires_restart());
		assert!(!ConfigKey::CdcRecentCacheCapacity.is_optional());
	}

	#[test]
	fn test_multi_read_buffer_pages_round_trip() {
		assert_eq!("MULTI_READ_BUFFER_PAGES".parse::<ConfigKey>().unwrap(), ConfigKey::MultiReadBufferPages);
		assert_eq!(format!("{}", ConfigKey::MultiReadBufferPages), "MULTI_READ_BUFFER_PAGES");
	}

	#[test]
	fn test_multi_read_buffer_pages_metadata_and_rejects_zero() {
		assert_eq!(ConfigKey::MultiReadBufferPages.default_value(), Value::Uint8(1024));
		assert_eq!(ConfigKey::MultiReadBufferPages.expected_types(), &[ValueType::Uint8]);
		assert!(ConfigKey::MultiReadBufferPages.requires_restart());
		assert!(!ConfigKey::MultiReadBufferPages.is_optional());
		match ConfigKey::MultiReadBufferPages.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_multi_read_buffer_page_size_round_trip() {
		assert_eq!(
			"MULTI_READ_BUFFER_PAGE_SIZE".parse::<ConfigKey>().unwrap(),
			ConfigKey::MultiReadBufferPageSize
		);
		assert_eq!(format!("{}", ConfigKey::MultiReadBufferPageSize), "MULTI_READ_BUFFER_PAGE_SIZE");
	}

	#[test]
	fn test_multi_read_buffer_page_size_metadata_and_rejects_non_power_of_two() {
		// Page size must be a power of two because pages are addressed by a row-number bit shift
		// (bucket = row >> shift); a non-power-of-two would not map to a single shift.
		assert_eq!(ConfigKey::MultiReadBufferPageSize.default_value(), Value::Uint8(65536));
		assert_eq!(ConfigKey::MultiReadBufferPageSize.expected_types(), &[ValueType::Uint8]);
		assert!(ConfigKey::MultiReadBufferPageSize.requires_restart());
		assert!(!ConfigKey::MultiReadBufferPageSize.is_optional());
		assert_eq!(
			ConfigKey::MultiReadBufferPageSize.accept(Value::Uint8(4096)).unwrap(),
			Value::Uint8(4096),
			"a power-of-two page size is accepted"
		);
		match ConfigKey::MultiReadBufferPageSize.accept(Value::Uint8(1000)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("power of two"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_threads_keys_round_trip() {
		assert_eq!("THREADS_ASYNC".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsAsync);
		assert_eq!("THREADS_SYSTEM".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsSystem);
		assert_eq!("THREADS_QUERY".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsQuery);
		assert_eq!("THREADS_COMMIT".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsCommit);
		assert_eq!("THREADS_BACKGROUND".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsBackground);
		assert_eq!(format!("{}", ConfigKey::ThreadsAsync), "THREADS_ASYNC");
		assert_eq!(format!("{}", ConfigKey::ThreadsSystem), "THREADS_SYSTEM");
		assert_eq!(format!("{}", ConfigKey::ThreadsQuery), "THREADS_QUERY");
		assert_eq!(format!("{}", ConfigKey::ThreadsCommit), "THREADS_COMMIT");
		assert_eq!(format!("{}", ConfigKey::ThreadsBackground), "THREADS_BACKGROUND");
	}

	#[test]
	fn test_threads_defaults() {
		assert_eq!(ConfigKey::ThreadsAsync.default_value(), Value::Uint2(1));
		assert_eq!(ConfigKey::ThreadsSystem.default_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsQuery.default_value(), Value::Uint2(1));
		assert_eq!(ConfigKey::ThreadsCommit.default_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsBackground.default_value(), Value::Uint2(1));
	}

	#[test]
	fn test_threads_reject_zero() {
		for key in [
			ConfigKey::ThreadsAsync,
			ConfigKey::ThreadsSystem,
			ConfigKey::ThreadsQuery,
			ConfigKey::ThreadsCommit,
			ConfigKey::ThreadsBackground,
		] {
			match key.accept(Value::Uint2(0)).unwrap_err() {
				AcceptError::InvalidValue(reason) => {
					assert!(
						reason.contains("greater than zero"),
						"{key}: unexpected reason: {reason}"
					);
				}
				other => panic!("{key}: expected InvalidValue, got {other:?}"),
			}
		}
	}

	#[test]
	fn test_threads_accept_positive() {
		assert_eq!(ConfigKey::ThreadsAsync.accept(Value::Uint2(4)).unwrap(), Value::Uint2(4));
		assert_eq!(ConfigKey::ThreadsSystem.accept(Value::Uint2(8)).unwrap(), Value::Uint2(8));
		assert_eq!(ConfigKey::ThreadsQuery.accept(Value::Uint2(16)).unwrap(), Value::Uint2(16));
		assert_eq!(ConfigKey::ThreadsCommit.accept(Value::Uint2(4)).unwrap(), Value::Uint2(4));
		assert_eq!(ConfigKey::ThreadsBackground.accept(Value::Uint2(2)).unwrap(), Value::Uint2(2));
	}

	#[test]
	fn test_threads_coerce_int4_to_uint2() {
		let v = ConfigKey::ThreadsQuery.accept(Value::Int4(8)).unwrap();
		assert_eq!(v, Value::Uint2(8));
	}

	#[test]
	fn test_threads_require_restart() {
		assert!(ConfigKey::ThreadsAsync.requires_restart());
		assert!(ConfigKey::ThreadsSystem.requires_restart());
		assert!(ConfigKey::ThreadsQuery.requires_restart());
		assert!(ConfigKey::ThreadsCommit.requires_restart());
		assert!(ConfigKey::ThreadsBackground.requires_restart());
	}

	#[test]
	fn test_query_row_batch_size_default_is_uint2_32() {
		assert_eq!(ConfigKey::QueryRowBatchSize.default_value(), Value::Uint2(32));
	}

	#[test]
	fn test_query_row_batch_size_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "QUERY_ROW_BATCH_SIZE".parse().unwrap();
		assert_eq!(key, ConfigKey::QueryRowBatchSize);
		assert_eq!(format!("{}", ConfigKey::QueryRowBatchSize), "QUERY_ROW_BATCH_SIZE");
	}

	#[test]
	fn test_query_row_batch_size_accept_rejects_zero() {
		match ConfigKey::QueryRowBatchSize.accept(Value::Uint2(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_query_row_batch_size_accept_passes_positive() {
		assert_eq!(ConfigKey::QueryRowBatchSize.accept(Value::Uint2(1)).unwrap(), Value::Uint2(1));
		assert_eq!(ConfigKey::QueryRowBatchSize.accept(Value::Uint2(1024)).unwrap(), Value::Uint2(1024));
	}

	#[test]
	fn test_query_row_batch_size_accept_rejects_zero_after_coercion() {
		match ConfigKey::QueryRowBatchSize.accept(Value::Int4(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"));
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_query_row_batch_size_coerces_int4_to_uint2() {
		let v = ConfigKey::QueryRowBatchSize.accept(Value::Int4(64)).unwrap();
		assert_eq!(v, Value::Uint2(64));
	}

	#[test]
	fn test_cdc_compact_interval_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_COMPACT_INTERVAL".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcCompactInterval);
		assert_eq!(format!("{}", ConfigKey::CdcCompactInterval), "CDC_COMPACT_INTERVAL");
	}

	#[test]
	fn test_cdc_compact_block_size_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_COMPACT_BLOCK_SIZE".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcCompactBlockSize);
		assert_eq!(format!("{}", ConfigKey::CdcCompactBlockSize), "CDC_COMPACT_BLOCK_SIZE");
	}

	#[test]
	fn test_cdc_compact_safety_lag_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_COMPACT_SAFETY_LAG".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcCompactSafetyLag);
		assert_eq!(format!("{}", ConfigKey::CdcCompactSafetyLag), "CDC_COMPACT_SAFETY_LAG");
	}

	#[test]
	fn test_cdc_compact_max_blocks_per_tick_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_COMPACT_MAX_BLOCKS_PER_TICK".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcCompactMaxBlocksPerTick);
		assert_eq!(format!("{}", ConfigKey::CdcCompactMaxBlocksPerTick), "CDC_COMPACT_MAX_BLOCKS_PER_TICK");
	}

	#[test]
	fn test_cdc_compact_interval_default_is_duration() {
		assert!(matches!(ConfigKey::CdcCompactInterval.default_value(), Value::Duration(_)));
	}

	#[test]
	fn test_cdc_compact_block_size_default_is_uint8_1024() {
		assert_eq!(ConfigKey::CdcCompactBlockSize.default_value(), Value::Uint8(1024));
	}

	#[test]
	fn test_cdc_compact_safety_lag_default_is_uint8_1024() {
		assert_eq!(ConfigKey::CdcCompactSafetyLag.default_value(), Value::Uint8(1024));
	}

	#[test]
	fn test_cdc_compact_max_blocks_per_tick_default_is_uint8_16() {
		assert_eq!(ConfigKey::CdcCompactMaxBlocksPerTick.default_value(), Value::Uint8(16));
	}

	#[test]
	fn test_cdc_compact_interval_accept_passes_positive_duration() {
		let one_sec = Value::duration_seconds(1);
		assert_eq!(ConfigKey::CdcCompactInterval.accept(one_sec.clone()).unwrap(), one_sec);
	}

	#[test]
	fn test_cdc_compact_interval_accept_rejects_zero() {
		let zero = Value::duration_seconds(0);
		match ConfigKey::CdcCompactInterval.accept(zero).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_cdc_compact_interval_accept_rejects_negative() {
		let negative = Value::duration_seconds(-5);
		assert!(matches!(ConfigKey::CdcCompactInterval.accept(negative), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_cdc_compact_block_size_accept_rejects_zero() {
		match ConfigKey::CdcCompactBlockSize.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_cdc_compact_block_size_accept_passes_positive() {
		assert_eq!(ConfigKey::CdcCompactBlockSize.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
		assert_eq!(ConfigKey::CdcCompactBlockSize.accept(Value::Uint8(1024)).unwrap(), Value::Uint8(1024));
	}

	#[test]
	fn test_cdc_compact_safety_lag_and_max_blocks_accept_zero() {
		assert_eq!(ConfigKey::CdcCompactSafetyLag.accept(Value::Uint8(0)).unwrap(), Value::Uint8(0));
		assert_eq!(ConfigKey::CdcCompactMaxBlocksPerTick.accept(Value::Uint8(0)).unwrap(), Value::Uint8(0));
	}

	#[test]
	fn test_accept_coerces_int4_to_uint8_for_block_size() {
		// SET CONFIG CDC_COMPACT_BLOCK_SIZE = 1024 (parsed as Int4) becomes Uint8(1024).
		let v = ConfigKey::CdcCompactBlockSize.accept(Value::Int4(1024)).unwrap();
		assert_eq!(v, Value::Uint8(1024));
	}

	#[test]
	fn test_accept_coerces_int8_to_uint8_for_block_size() {
		let v = ConfigKey::CdcCompactBlockSize.accept(Value::Int8(2048)).unwrap();
		assert_eq!(v, Value::Uint8(2048));
	}

	#[test]
	fn test_accept_rejects_zero_after_coercion() {
		// Int4(0) coerces to Uint8(0), then validate_canonical rejects it.
		match ConfigKey::CdcCompactBlockSize.accept(Value::Int4(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"));
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_accept_rejects_negative_int_for_uint8_key() {
		// to_usize() returns None for negatives -> all coercion arms fail -> TypeMismatch.
		assert!(matches!(
			ConfigKey::CdcCompactBlockSize.accept(Value::Int4(-1)),
			Err(AcceptError::TypeMismatch { .. })
		));
	}

	#[test]
	fn test_accept_coerces_int_to_duration_via_seconds() {
		// SET CONFIG CDC_COMPACT_INTERVAL = 60 (Int4) -> Duration(60s).
		let v = ConfigKey::CdcCompactInterval.accept(Value::Int4(60)).unwrap();
		assert!(matches!(v, Value::Duration(_)));
	}

	#[test]
	fn test_accept_idempotent_on_canonical_uint8() {
		let canonical = Value::Uint8(42);
		assert_eq!(ConfigKey::OracleWindowSize.accept(canonical.clone()).unwrap(), canonical);
	}

	#[test]
	fn test_accept_idempotent_on_canonical_duration() {
		let canonical = Value::duration_seconds(5);
		assert_eq!(ConfigKey::CdcCompactInterval.accept(canonical.clone()).unwrap(), canonical);
	}

	#[test]
	fn test_accept_rejects_typed_null_for_non_optional_key() {
		let err = ConfigKey::CdcCompactBlockSize
			.accept(Value::None {
				inner: ValueType::Uint8,
			})
			.unwrap_err();
		assert!(matches!(err, AcceptError::TypeMismatch { .. }));
	}

	#[test]
	fn test_accept_passes_typed_null_for_optional_key() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(ConfigKey::CdcTtlDuration.accept(none.clone()).unwrap(), none);
	}

	#[test]
	fn test_accept_rejects_wrong_inner_type_typed_null_for_optional_key() {
		// Optional key still rejects typed-null whose inner doesn't match expected_types.
		let err = ConfigKey::CdcTtlDuration
			.accept(Value::None {
				inner: ValueType::Uint8,
			})
			.unwrap_err();
		assert!(matches!(err, AcceptError::TypeMismatch { .. }));
	}

	#[test]
	fn test_historical_gc_keys_round_trip() {
		assert_eq!("HISTORICAL_GC_BATCH_SIZE".parse::<ConfigKey>().unwrap(), ConfigKey::HistoricalGcBatchSize);
		assert_eq!("HISTORICAL_GC_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::HistoricalGcInterval);
		assert_eq!(format!("{}", ConfigKey::HistoricalGcBatchSize), "HISTORICAL_GC_BATCH_SIZE");
		assert_eq!(format!("{}", ConfigKey::HistoricalGcInterval), "HISTORICAL_GC_INTERVAL");
	}

	#[test]
	fn test_historical_gc_defaults() {
		assert_eq!(ConfigKey::HistoricalGcBatchSize.default_value(), Value::Uint8(50_000));
		assert!(matches!(ConfigKey::HistoricalGcInterval.default_value(), Value::Duration(_)));
	}

	#[test]
	fn test_historical_gc_batch_size_rejects_zero() {
		match ConfigKey::HistoricalGcBatchSize.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_historical_gc_interval_rejects_zero() {
		let zero = Value::duration_seconds(0);
		match ConfigKey::HistoricalGcInterval.accept(zero).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}
}
