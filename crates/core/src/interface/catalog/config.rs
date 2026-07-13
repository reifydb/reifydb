// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, str::FromStr};

use reifydb_value::value::{Value, duration::Duration, value_type::ValueType};

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
	RetentionEvictInterval,
	RetentionEvictBatchSize,
	RetentionEvictMaxBatchesPerTick,
	OperatorTtlScanBatchSize,
	OperatorTtlScanInterval,
	VersionEpochSampleInterval,
	HistoricalGcBatchSize,
	HistoricalGcInterval,
	CdcTtlDuration,
	CdcTtlScanInterval,
	CdcTtlScanBatchSize,
	CdcTtlScanMaxBatchesPerTick,
	CdcCompactInterval,
	CdcCompactBlockSize,
	CdcCompactSafetyLag,
	CdcCompactMaxBlocksPerTick,
	CdcCompactBlockCacheCapacity,
	CdcCompactZstdLevel,
	CdcRecentCacheCapacity,
	CdcWalAutocheckpoint,
	MultiReadBufferPages,
	MultiReadBufferPageSize,
	MultiFlushInterval,
	MultiWalAutocheckpoint,
	FlowTick,
	CdcWatermarkWaitTimeout,
	CdcConsumeWaitTimeout,
	FlowJoinProbeBlockSize,
	ThreadsAsync,
	ThreadsCoordination,
	ThreadsFlow,
	ThreadsTask,
	ThreadsCompute,
	SubscriptionWorkerThreads,
	RuntimeMetricsInterval,
	MetricFlushInterval,
	MetricsRuntimeRetention,
	MetricsProfilerRetention,
	MetricsProfilerSnapshotInterval,
}

impl ConfigKey {
	pub fn all() -> &'static [Self] {
		&[
			Self::OracleWindowSize,
			Self::OracleWaterMark,
			Self::QueryRowBatchSize,
			Self::RetentionEvictInterval,
			Self::RetentionEvictBatchSize,
			Self::RetentionEvictMaxBatchesPerTick,
			Self::OperatorTtlScanBatchSize,
			Self::OperatorTtlScanInterval,
			Self::VersionEpochSampleInterval,
			Self::HistoricalGcBatchSize,
			Self::HistoricalGcInterval,
			Self::CdcTtlDuration,
			Self::CdcTtlScanInterval,
			Self::CdcTtlScanBatchSize,
			Self::CdcTtlScanMaxBatchesPerTick,
			Self::CdcCompactInterval,
			Self::CdcCompactBlockSize,
			Self::CdcCompactSafetyLag,
			Self::CdcCompactMaxBlocksPerTick,
			Self::CdcCompactBlockCacheCapacity,
			Self::CdcCompactZstdLevel,
			Self::CdcRecentCacheCapacity,
			Self::CdcWalAutocheckpoint,
			Self::MultiReadBufferPages,
			Self::MultiReadBufferPageSize,
			Self::MultiFlushInterval,
			Self::MultiWalAutocheckpoint,
			Self::FlowTick,
			Self::CdcWatermarkWaitTimeout,
			Self::CdcConsumeWaitTimeout,
			Self::FlowJoinProbeBlockSize,
			Self::ThreadsAsync,
			Self::ThreadsCoordination,
			Self::ThreadsFlow,
			Self::ThreadsTask,
			Self::ThreadsCompute,
			Self::SubscriptionWorkerThreads,
			Self::RuntimeMetricsInterval,
			Self::MetricFlushInterval,
			Self::MetricsRuntimeRetention,
			Self::MetricsProfilerRetention,
			Self::MetricsProfilerSnapshotInterval,
		]
	}

	pub fn default_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(500),
			Self::OracleWaterMark => Value::Uint8(20),
			Self::QueryRowBatchSize => Value::Uint2(32),
			Self::RetentionEvictInterval => Value::duration_seconds(60),
			Self::RetentionEvictBatchSize => Value::Uint8(1024),
			Self::RetentionEvictMaxBatchesPerTick => Value::Uint8(8),
			Self::OperatorTtlScanBatchSize => Value::Uint8(10000),
			Self::OperatorTtlScanInterval => Value::duration_seconds(60),
			Self::VersionEpochSampleInterval => Value::duration_seconds(1),
			Self::HistoricalGcBatchSize => Value::Uint8(50_000),
			Self::HistoricalGcInterval => Value::duration_seconds(30),
			Self::CdcTtlDuration => Value::None {
				inner: ValueType::Duration,
			},
			Self::CdcTtlScanInterval => Value::duration_seconds(30),
			Self::CdcTtlScanBatchSize => Value::Uint8(8192),
			Self::CdcTtlScanMaxBatchesPerTick => Value::Uint8(32),
			Self::CdcCompactInterval => Value::duration_seconds(60),
			Self::CdcCompactBlockSize => Value::Uint8(1024),
			Self::CdcCompactSafetyLag => Value::Uint8(1024),
			Self::CdcCompactMaxBlocksPerTick => Value::Uint8(16),
			Self::CdcCompactBlockCacheCapacity => Value::Uint8(8),
			Self::CdcCompactZstdLevel => Value::Uint1(2),
			Self::CdcRecentCacheCapacity => Value::Uint8(128),
			Self::CdcWalAutocheckpoint => Value::Uint8(10000),
			Self::MultiReadBufferPages => Value::Uint8(1024),
			Self::MultiReadBufferPageSize => Value::Uint8(65536),
			Self::MultiFlushInterval => Value::duration_seconds(5),
			Self::MultiWalAutocheckpoint => Value::Uint8(10000),
			Self::FlowTick => Value::duration_seconds(1),
			Self::CdcWatermarkWaitTimeout => Value::duration_seconds(1),
			Self::CdcConsumeWaitTimeout => Value::duration_seconds(30),
			Self::FlowJoinProbeBlockSize => Value::Uint8(1024),
			Self::ThreadsAsync => Value::Uint2(1),
			Self::ThreadsCoordination => Value::Uint2(2),
			Self::ThreadsFlow => Value::Uint2(2),
			Self::ThreadsTask => Value::Uint2(2),
			Self::ThreadsCompute => Value::Uint2(2),
			Self::SubscriptionWorkerThreads => Value::Uint2(0),
			Self::RuntimeMetricsInterval => Value::duration_seconds(5),
			Self::MetricFlushInterval => Value::duration_seconds(10),
			Self::MetricsRuntimeRetention => Value::duration_seconds(7 * 24 * 3600),
			Self::MetricsProfilerRetention => Value::duration_seconds(3600),
			Self::MetricsProfilerSnapshotInterval => Value::None {
				inner: ValueType::Duration,
			},
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::OracleWaterMark => "Number of conflict windows retained before cleanup is triggered.",
			Self::QueryRowBatchSize => {
				"Number of rows produced per batch by query / DML pipeline operators."
			}
			Self::RetentionEvictInterval => {
				"How often the retention evictor scans shapes with a row TTL for expired rows."
			}
			Self::RetentionEvictBatchSize => {
				"Max rows examined (and thus evicted) per transaction during a retention eviction tick."
			}
			Self::RetentionEvictMaxBatchesPerTick => {
				"Upper bound on eviction transactions per retention tick. Caps how long one tick can run when draining a backlog; remaining work resumes on the next tick."
			}
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
			Self::CdcTtlScanInterval => {
				"How often the CDC producer actor scans for and evicts expired CDC entries."
			}
			Self::CdcTtlScanBatchSize => {
				"Max CDC entries deleted per transaction during a CDC TTL eviction tick."
			}
			Self::CdcTtlScanMaxBatchesPerTick => {
				"Upper bound on delete transactions per CDC TTL eviction tick. Caps how long one tick can run when draining a backlog; remaining work continues on the next tick."
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
			Self::CdcWalAutocheckpoint => {
				"WAL frame threshold (SQLite wal_autocheckpoint PRAGMA) for the CDC log's SQLite tier. \
				 CDC has no explicit checkpoint of its own, so this is the sole control over how often \
				 cdc.db's WAL is checkpointed into the main file. Higher values checkpoint less often with \
				 a larger WAL; since CDC is written on the commit path, this also bounds how often a commit \
				 pays an inline auto-checkpoint. Read once at boot; changing it requires a restart."
			}
			Self::MultiReadBufferPages => {
				"Number of pages (contiguous row-number buckets) the multi-version read cache keeps \
				 resident before eviction. Raising it trades RAM for fewer persistent-tier reads."
			}
			Self::MultiReadBufferPageSize => {
				"Number of rows per cached page (bucket) in the multi-version read cache. Must be a \
				 power of two; sets the granularity of whole-page read-ahead and completeness tracking."
			}
			Self::MultiFlushInterval => {
				"How often the persistent-flush actor drains the in-memory commit buffer into the multi \
				 store's SQLite tier and checkpoints its WAL. Longer intervals coalesce more writes per \
				 flush - fewer, larger WAL checkpoints and a larger WAL - at the cost of more resident \
				 commit-buffer memory and a longer window before data is materialized in the persistent \
				 file. Read once at boot; changing it requires a restart."
			}
			Self::MultiWalAutocheckpoint => {
				"WAL frame threshold for the multi store's SQLite tier: sets both the SQLite \
				 wal_autocheckpoint PRAGMA and the frame count above which the flush actor forces a \
				 blocking RESTART checkpoint. Higher values checkpoint less often with a larger WAL, \
				 reducing checkpoint I/O and blocking-checkpoint frequency; lower values keep the WAL \
				 small at the cost of more frequent checkpoints. Read once at boot; changing it requires \
				 a restart."
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
			Self::CdcConsumeWaitTimeout => {
				"Backstop timeout for the CDC consumer's wait for a consume reply from the downstream \
				 consumer. A lost reply would otherwise wedge the poll loop forever; on timeout the batch \
				 is re-dispatched without advancing the checkpoint. Must be > 0."
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
			Self::ThreadsCoordination => {
				"Number of worker threads for the coordination group (long-lived actors with \
				 tiny high-frequency handlers and periodic background actors); pinned dispatch. \
				 Must be >= 1. Changes require restart."
			}
			Self::ThreadsFlow => {
				"Number of worker threads for the flow group (long-lived heavy-handler actors: \
				 materialized-view flow execution); pinned dispatch. \
				 Must be >= 1. Changes require restart."
			}
			Self::ThreadsTask => {
				"Number of worker threads for the task pool (short-lived work: per-request \
				 actors and one-shot jobs). Must be >= 1. Changes require restart."
			}
			Self::ThreadsCompute => {
				"Number of worker threads for the compute pool (data-parallel work via install(), \
				 never actors). Must be >= 1. Changes require restart."
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
			Self::MetricsRuntimeRetention => {
				"Row TTL applied to the system::metrics::runtime::* snapshot series so old samples are \
				 evicted. Seeded onto each runtime series at bootstrap only when it has no row settings \
				 yet; changing it affects series created after the change, not already-seeded ones. \
				 Must be > 0."
			}
			Self::MetricsProfilerRetention => {
				"Row TTL applied to the system::metrics::profiler::*::snapshots series so old samples are \
				 evicted. Seeded onto each profiler series at bootstrap only when it has no row settings \
				 yet; changing it affects series created after the change, not already-seeded ones. \
				 Must be > 0."
			}
			Self::MetricsProfilerSnapshotInterval => {
				"How often the profiler snapshot actor flushes in-memory aggregates into \
				 system::metrics::profiler::*::snapshots. Defaults to none, which disables snapshot \
				 persistence entirely (the actor is never spawned) and leaves only the live ::current \
				 view available; when set, must be > 0. Read once at subsystem construction, so \
				 changing it requires a restart."
			}
		}
	}

	pub fn requires_restart(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::QueryRowBatchSize => false,
			Self::RetentionEvictInterval => true,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::OperatorTtlScanBatchSize => false,
			Self::OperatorTtlScanInterval => false,
			Self::VersionEpochSampleInterval => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => false,
			Self::CdcTtlScanInterval => true,
			Self::CdcTtlScanBatchSize => false,
			Self::CdcTtlScanMaxBatchesPerTick => false,
			Self::CdcCompactInterval => false,
			Self::CdcCompactBlockSize => false,
			Self::CdcCompactSafetyLag => false,
			Self::CdcCompactMaxBlocksPerTick => false,
			Self::CdcCompactBlockCacheCapacity => true,
			Self::CdcCompactZstdLevel => false,
			Self::CdcRecentCacheCapacity => true,
			Self::CdcWalAutocheckpoint => true,
			Self::MultiReadBufferPages => true,
			Self::MultiReadBufferPageSize => true,
			Self::MultiFlushInterval => true,
			Self::MultiWalAutocheckpoint => true,
			Self::FlowTick => false,
			Self::CdcWatermarkWaitTimeout => false,
			Self::CdcConsumeWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => true,
			Self::ThreadsCoordination => true,
			Self::ThreadsFlow => true,
			Self::ThreadsTask => true,
			Self::ThreadsCompute => true,
			Self::SubscriptionWorkerThreads => true,
			Self::RuntimeMetricsInterval => false,
			Self::MetricFlushInterval => false,
			Self::MetricsRuntimeRetention => true,
			Self::MetricsProfilerRetention => true,
			Self::MetricsProfilerSnapshotInterval => true,
		}
	}

	pub fn expected_types(&self) -> &'static [ValueType] {
		match self {
			Self::OracleWindowSize => &[ValueType::Uint8],
			Self::OracleWaterMark => &[ValueType::Uint8],
			Self::QueryRowBatchSize => &[ValueType::Uint2],
			Self::RetentionEvictInterval => &[ValueType::Duration],
			Self::RetentionEvictBatchSize => &[ValueType::Uint8],
			Self::RetentionEvictMaxBatchesPerTick => &[ValueType::Uint8],
			Self::OperatorTtlScanBatchSize => &[ValueType::Uint8],
			Self::OperatorTtlScanInterval => &[ValueType::Duration],
			Self::VersionEpochSampleInterval => &[ValueType::Duration],
			Self::HistoricalGcBatchSize => &[ValueType::Uint8],
			Self::HistoricalGcInterval => &[ValueType::Duration],
			Self::CdcTtlDuration => &[ValueType::Duration],
			Self::CdcTtlScanInterval => &[ValueType::Duration],
			Self::CdcTtlScanBatchSize => &[ValueType::Uint8],
			Self::CdcTtlScanMaxBatchesPerTick => &[ValueType::Uint8],
			Self::CdcCompactInterval => &[ValueType::Duration],
			Self::CdcCompactBlockSize => &[ValueType::Uint8],
			Self::CdcCompactSafetyLag => &[ValueType::Uint8],
			Self::CdcCompactMaxBlocksPerTick => &[ValueType::Uint8],
			Self::CdcCompactBlockCacheCapacity => &[ValueType::Uint8],
			Self::CdcCompactZstdLevel => &[ValueType::Uint1],
			Self::CdcRecentCacheCapacity => &[ValueType::Uint8],
			Self::CdcWalAutocheckpoint => &[ValueType::Uint8],
			Self::MultiReadBufferPages => &[ValueType::Uint8],
			Self::MultiReadBufferPageSize => &[ValueType::Uint8],
			Self::MultiFlushInterval => &[ValueType::Duration],
			Self::MultiWalAutocheckpoint => &[ValueType::Uint8],
			Self::FlowTick => &[ValueType::Duration],
			Self::CdcWatermarkWaitTimeout => &[ValueType::Duration],
			Self::CdcConsumeWaitTimeout => &[ValueType::Duration],
			Self::FlowJoinProbeBlockSize => &[ValueType::Uint8],
			Self::ThreadsAsync => &[ValueType::Uint2],
			Self::ThreadsCoordination => &[ValueType::Uint2],
			Self::ThreadsFlow => &[ValueType::Uint2],
			Self::ThreadsTask => &[ValueType::Uint2],
			Self::ThreadsCompute => &[ValueType::Uint2],
			Self::SubscriptionWorkerThreads => &[ValueType::Uint2],
			Self::RuntimeMetricsInterval => &[ValueType::Duration],
			Self::MetricFlushInterval => &[ValueType::Duration],
			Self::MetricsRuntimeRetention => &[ValueType::Duration],
			Self::MetricsProfilerRetention => &[ValueType::Duration],
			Self::MetricsProfilerSnapshotInterval => &[ValueType::Duration],
		}
	}

	pub fn is_optional(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::QueryRowBatchSize => false,
			Self::RetentionEvictInterval => false,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::OperatorTtlScanBatchSize => false,
			Self::OperatorTtlScanInterval => false,
			Self::VersionEpochSampleInterval => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => true,
			Self::CdcTtlScanInterval => false,
			Self::CdcTtlScanBatchSize => false,
			Self::CdcTtlScanMaxBatchesPerTick => false,
			Self::CdcCompactInterval => false,
			Self::CdcCompactBlockSize => false,
			Self::CdcCompactSafetyLag => false,
			Self::CdcCompactMaxBlocksPerTick => false,
			Self::CdcCompactBlockCacheCapacity => false,
			Self::CdcCompactZstdLevel => false,
			Self::CdcRecentCacheCapacity => false,
			Self::CdcWalAutocheckpoint => false,
			Self::MultiReadBufferPages => false,
			Self::MultiReadBufferPageSize => false,
			Self::MultiFlushInterval => false,
			Self::MultiWalAutocheckpoint => false,
			Self::FlowTick => false,
			Self::CdcWatermarkWaitTimeout => false,
			Self::CdcConsumeWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => false,
			Self::ThreadsCoordination => false,
			Self::ThreadsFlow => false,
			Self::ThreadsTask => false,
			Self::ThreadsCompute => false,
			Self::SubscriptionWorkerThreads => false,
			Self::RuntimeMetricsInterval => true,
			Self::MetricFlushInterval => false,
			Self::MetricsRuntimeRetention => false,
			Self::MetricsProfilerRetention => false,
			Self::MetricsProfilerSnapshotInterval => true,
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
			Self::MultiFlushInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("MULTI_FLUSH_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiWalAutocheckpoint => match value {
				Value::Uint8(0) => {
					Err("MULTI_WAL_AUTOCHECKPOINT must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::CdcWalAutocheckpoint => match value {
				Value::Uint8(0) => Err("CDC_WAL_AUTOCHECKPOINT must be greater than zero".to_string()),
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
			Self::CdcConsumeWaitTimeout => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("CDC_CONSUME_WAIT_TIMEOUT must be greater than zero".to_string())
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
			Self::ThreadsCoordination => match value {
				Value::Uint2(0) => Err("THREADS_COORDINATION must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsFlow => match value {
				Value::Uint2(0) => Err("THREADS_FLOW must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsTask => match value {
				Value::Uint2(0) => Err("THREADS_TASK must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::ThreadsCompute => match value {
				Value::Uint2(0) => Err("THREADS_COMPUTE must be greater than zero".to_string()),
				_ => Ok(()),
			},
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
			Self::MetricsRuntimeRetention => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_RUNTIME_RETENTION must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::MetricsProfilerRetention => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_PROFILER_RETENTION must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::MetricsProfilerSnapshotInterval => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_PROFILER_SNAPSHOT_INTERVAL must be greater than zero"
							.to_string())
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

		if !self.expected_types().contains(&value.get_type()) {
			return Err(AcceptError::TypeMismatch {
				expected: self.expected_types().to_vec(),
				actual: value.get_type(),
			});
		}

		self.validate_canonical(&value).map_err(AcceptError::InvalidValue)?;
		Ok(value)
	}
}

impl fmt::Display for ConfigKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OracleWindowSize => write!(f, "ORACLE_WINDOW_SIZE"),
			Self::OracleWaterMark => write!(f, "ORACLE_WATER_MARK"),
			Self::QueryRowBatchSize => write!(f, "QUERY_ROW_BATCH_SIZE"),
			Self::RetentionEvictInterval => write!(f, "RETENTION_EVICT_INTERVAL"),
			Self::RetentionEvictBatchSize => write!(f, "RETENTION_EVICT_BATCH_SIZE"),
			Self::RetentionEvictMaxBatchesPerTick => write!(f, "RETENTION_EVICT_MAX_BATCHES_PER_TICK"),
			Self::OperatorTtlScanBatchSize => write!(f, "OPERATOR_TTL_SCAN_BATCH_SIZE"),
			Self::OperatorTtlScanInterval => write!(f, "OPERATOR_TTL_SCAN_INTERVAL"),
			Self::VersionEpochSampleInterval => write!(f, "VERSION_EPOCH_SAMPLE_INTERVAL"),
			Self::HistoricalGcBatchSize => write!(f, "HISTORICAL_GC_BATCH_SIZE"),
			Self::HistoricalGcInterval => write!(f, "HISTORICAL_GC_INTERVAL"),
			Self::CdcTtlDuration => write!(f, "CDC_TTL_DURATION"),
			Self::CdcTtlScanInterval => write!(f, "CDC_TTL_SCAN_INTERVAL"),
			Self::CdcTtlScanBatchSize => write!(f, "CDC_TTL_SCAN_BATCH_SIZE"),
			Self::CdcTtlScanMaxBatchesPerTick => write!(f, "CDC_TTL_SCAN_MAX_BATCHES_PER_TICK"),
			Self::CdcCompactInterval => write!(f, "CDC_COMPACT_INTERVAL"),
			Self::CdcCompactBlockSize => write!(f, "CDC_COMPACT_BLOCK_SIZE"),
			Self::CdcCompactSafetyLag => write!(f, "CDC_COMPACT_SAFETY_LAG"),
			Self::CdcCompactMaxBlocksPerTick => write!(f, "CDC_COMPACT_MAX_BLOCKS_PER_TICK"),
			Self::CdcCompactBlockCacheCapacity => write!(f, "CDC_COMPACT_BLOCK_CACHE_CAPACITY"),
			Self::CdcCompactZstdLevel => write!(f, "CDC_COMPACT_ZSTD_LEVEL"),
			Self::CdcRecentCacheCapacity => write!(f, "CDC_RECENT_CACHE_CAPACITY"),
			Self::CdcWalAutocheckpoint => write!(f, "CDC_WAL_AUTOCHECKPOINT"),
			Self::MultiReadBufferPages => write!(f, "MULTI_READ_BUFFER_PAGES"),
			Self::MultiReadBufferPageSize => write!(f, "MULTI_READ_BUFFER_PAGE_SIZE"),
			Self::MultiFlushInterval => write!(f, "MULTI_FLUSH_INTERVAL"),
			Self::MultiWalAutocheckpoint => write!(f, "MULTI_WAL_AUTOCHECKPOINT"),
			Self::FlowTick => write!(f, "FLOW_TICK"),
			Self::CdcWatermarkWaitTimeout => write!(f, "CDC_WATERMARK_WAIT_TIMEOUT"),
			Self::CdcConsumeWaitTimeout => write!(f, "CDC_CONSUME_WAIT_TIMEOUT"),
			Self::FlowJoinProbeBlockSize => write!(f, "FLOW_JOIN_PROBE_BLOCK_SIZE"),
			Self::ThreadsAsync => write!(f, "THREADS_ASYNC"),
			Self::ThreadsCoordination => write!(f, "THREADS_COORDINATION"),
			Self::ThreadsFlow => write!(f, "THREADS_FLOW"),
			Self::ThreadsTask => write!(f, "THREADS_TASK"),
			Self::ThreadsCompute => write!(f, "THREADS_COMPUTE"),
			Self::SubscriptionWorkerThreads => write!(f, "SUBSCRIPTION_WORKER_THREADS"),
			Self::RuntimeMetricsInterval => write!(f, "RUNTIME_METRICS_INTERVAL"),
			Self::MetricFlushInterval => write!(f, "METRIC_FLUSH_INTERVAL"),
			Self::MetricsRuntimeRetention => write!(f, "METRICS_RUNTIME_RETENTION"),
			Self::MetricsProfilerRetention => write!(f, "METRICS_PROFILER_RETENTION"),
			Self::MetricsProfilerSnapshotInterval => write!(f, "METRICS_PROFILER_SNAPSHOT_INTERVAL"),
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
			"RETENTION_EVICT_INTERVAL" => Ok(Self::RetentionEvictInterval),
			"RETENTION_EVICT_BATCH_SIZE" => Ok(Self::RetentionEvictBatchSize),
			"RETENTION_EVICT_MAX_BATCHES_PER_TICK" => Ok(Self::RetentionEvictMaxBatchesPerTick),
			"OPERATOR_TTL_SCAN_BATCH_SIZE" => Ok(Self::OperatorTtlScanBatchSize),
			"OPERATOR_TTL_SCAN_INTERVAL" => Ok(Self::OperatorTtlScanInterval),
			"VERSION_EPOCH_SAMPLE_INTERVAL" => Ok(Self::VersionEpochSampleInterval),
			"HISTORICAL_GC_BATCH_SIZE" => Ok(Self::HistoricalGcBatchSize),
			"HISTORICAL_GC_INTERVAL" => Ok(Self::HistoricalGcInterval),
			"CDC_TTL_DURATION" => Ok(Self::CdcTtlDuration),
			"CDC_TTL_SCAN_INTERVAL" => Ok(Self::CdcTtlScanInterval),
			"CDC_TTL_SCAN_BATCH_SIZE" => Ok(Self::CdcTtlScanBatchSize),
			"CDC_TTL_SCAN_MAX_BATCHES_PER_TICK" => Ok(Self::CdcTtlScanMaxBatchesPerTick),
			"CDC_COMPACT_INTERVAL" => Ok(Self::CdcCompactInterval),
			"CDC_COMPACT_BLOCK_SIZE" => Ok(Self::CdcCompactBlockSize),
			"CDC_COMPACT_SAFETY_LAG" => Ok(Self::CdcCompactSafetyLag),
			"CDC_COMPACT_MAX_BLOCKS_PER_TICK" => Ok(Self::CdcCompactMaxBlocksPerTick),
			"CDC_COMPACT_BLOCK_CACHE_CAPACITY" => Ok(Self::CdcCompactBlockCacheCapacity),
			"CDC_COMPACT_ZSTD_LEVEL" => Ok(Self::CdcCompactZstdLevel),
			"CDC_RECENT_CACHE_CAPACITY" => Ok(Self::CdcRecentCacheCapacity),
			"CDC_WAL_AUTOCHECKPOINT" => Ok(Self::CdcWalAutocheckpoint),
			"MULTI_READ_BUFFER_PAGES" => Ok(Self::MultiReadBufferPages),
			"MULTI_READ_BUFFER_PAGE_SIZE" => Ok(Self::MultiReadBufferPageSize),
			"MULTI_FLUSH_INTERVAL" => Ok(Self::MultiFlushInterval),
			"MULTI_WAL_AUTOCHECKPOINT" => Ok(Self::MultiWalAutocheckpoint),
			"FLOW_TICK" => Ok(Self::FlowTick),
			"CDC_WATERMARK_WAIT_TIMEOUT" => Ok(Self::CdcWatermarkWaitTimeout),
			"CDC_CONSUME_WAIT_TIMEOUT" => Ok(Self::CdcConsumeWaitTimeout),
			"FLOW_JOIN_PROBE_BLOCK_SIZE" => Ok(Self::FlowJoinProbeBlockSize),
			"THREADS_ASYNC" => Ok(Self::ThreadsAsync),
			"THREADS_COORDINATION" => Ok(Self::ThreadsCoordination),
			"THREADS_FLOW" => Ok(Self::ThreadsFlow),
			"THREADS_TASK" => Ok(Self::ThreadsTask),
			"THREADS_COMPUTE" => Ok(Self::ThreadsCompute),
			"SUBSCRIPTION_WORKER_THREADS" => Ok(Self::SubscriptionWorkerThreads),
			"RUNTIME_METRICS_INTERVAL" => Ok(Self::RuntimeMetricsInterval),
			"METRIC_FLUSH_INTERVAL" => Ok(Self::MetricFlushInterval),
			"METRICS_RUNTIME_RETENTION" => Ok(Self::MetricsRuntimeRetention),
			"METRICS_PROFILER_RETENTION" => Ok(Self::MetricsProfilerRetention),
			"METRICS_PROFILER_SNAPSHOT_INTERVAL" => Ok(Self::MetricsProfilerSnapshotInterval),
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
		assert!(ConfigKey::OperatorTtlScanInterval.accept(Value::duration_seconds(0)).is_ok());
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
		assert_eq!(all.len(), 44);
		assert!(all.contains(&ConfigKey::RetentionEvictInterval));
		assert!(all.contains(&ConfigKey::RetentionEvictBatchSize));
		assert!(all.contains(&ConfigKey::RetentionEvictMaxBatchesPerTick));
		assert!(all.contains(&ConfigKey::MultiFlushInterval));
		assert!(all.contains(&ConfigKey::MultiWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::CdcWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::MetricsRuntimeRetention));
		assert!(all.contains(&ConfigKey::MetricsProfilerRetention));
		assert!(all.contains(&ConfigKey::MetricsProfilerSnapshotInterval));
		assert!(all.contains(&ConfigKey::VersionEpochSampleInterval));
		assert!(all.contains(&ConfigKey::CdcWatermarkWaitTimeout));
		assert!(all.contains(&ConfigKey::CdcConsumeWaitTimeout));
		assert!(all.contains(&ConfigKey::FlowJoinProbeBlockSize));
		assert!(all.contains(&ConfigKey::CdcTtlScanInterval));
		assert!(all.contains(&ConfigKey::CdcTtlScanBatchSize));
		assert!(all.contains(&ConfigKey::CdcTtlScanMaxBatchesPerTick));
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
		assert!(all.contains(&ConfigKey::ThreadsCoordination));
		assert!(all.contains(&ConfigKey::ThreadsFlow));
		assert!(all.contains(&ConfigKey::ThreadsTask));
		assert!(all.contains(&ConfigKey::ThreadsCompute));
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
		assert_eq!("THREADS_COORDINATION".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsCoordination);
		assert_eq!("THREADS_FLOW".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsFlow);
		assert_eq!("THREADS_TASK".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsTask);
		assert_eq!("THREADS_COMPUTE".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsCompute);
		assert_eq!(format!("{}", ConfigKey::ThreadsAsync), "THREADS_ASYNC");
		assert_eq!(format!("{}", ConfigKey::ThreadsCoordination), "THREADS_COORDINATION");
		assert_eq!(format!("{}", ConfigKey::ThreadsFlow), "THREADS_FLOW");
		assert_eq!(format!("{}", ConfigKey::ThreadsTask), "THREADS_TASK");
		assert_eq!(format!("{}", ConfigKey::ThreadsCompute), "THREADS_COMPUTE");
	}

	#[test]
	fn test_threads_defaults() {
		assert_eq!(ConfigKey::ThreadsAsync.default_value(), Value::Uint2(1));
		assert_eq!(ConfigKey::ThreadsCoordination.default_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsFlow.default_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsTask.default_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsCompute.default_value(), Value::Uint2(2));
	}

	#[test]
	fn test_threads_reject_zero() {
		for key in [
			ConfigKey::ThreadsAsync,
			ConfigKey::ThreadsCoordination,
			ConfigKey::ThreadsFlow,
			ConfigKey::ThreadsTask,
			ConfigKey::ThreadsCompute,
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
		assert_eq!(ConfigKey::ThreadsCoordination.accept(Value::Uint2(8)).unwrap(), Value::Uint2(8));
		assert_eq!(ConfigKey::ThreadsFlow.accept(Value::Uint2(16)).unwrap(), Value::Uint2(16));
		assert_eq!(ConfigKey::ThreadsTask.accept(Value::Uint2(4)).unwrap(), Value::Uint2(4));
		assert_eq!(ConfigKey::ThreadsCompute.accept(Value::Uint2(2)).unwrap(), Value::Uint2(2));
	}

	#[test]
	fn test_threads_reject_int4_for_uint2_key() {
		// accept is strict: coercion happens at the CALL boundary via cast_value.
		assert!(matches!(ConfigKey::ThreadsTask.accept(Value::Int4(8)), Err(AcceptError::TypeMismatch { .. })));
	}

	#[test]
	fn test_threads_require_restart() {
		assert!(ConfigKey::ThreadsAsync.requires_restart());
		assert!(ConfigKey::ThreadsCoordination.requires_restart());
		assert!(ConfigKey::ThreadsFlow.requires_restart());
		assert!(ConfigKey::ThreadsTask.requires_restart());
		assert!(ConfigKey::ThreadsCompute.requires_restart());
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
	fn test_query_row_batch_size_rejects_mismatched_type() {
		// accept is strict: an Int4 no longer coerces here, regardless of the value.
		assert!(matches!(
			ConfigKey::QueryRowBatchSize.accept(Value::Int4(64)),
			Err(AcceptError::TypeMismatch { .. })
		));
		assert!(matches!(
			ConfigKey::QueryRowBatchSize.accept(Value::Int4(0)),
			Err(AcceptError::TypeMismatch { .. })
		));
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
	fn test_accept_rejects_int4_for_uint8_block_size() {
		// accept is strict: SET CONFIG casts to Uint8 via cast_value before calling accept.
		assert!(matches!(
			ConfigKey::CdcCompactBlockSize.accept(Value::Int4(1024)),
			Err(AcceptError::TypeMismatch { .. })
		));
		assert!(matches!(
			ConfigKey::CdcCompactBlockSize.accept(Value::Int8(2048)),
			Err(AcceptError::TypeMismatch { .. })
		));
	}

	#[test]
	fn test_accept_rejects_zero_of_canonical_type() {
		match ConfigKey::CdcCompactBlockSize.accept(Value::Uint8(0)).unwrap_err() {
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
	fn test_accept_rejects_int_for_duration_key() {
		// Bare integers carry no unit: duration keys take Duration values (or duration
		// strings cast at the CALL boundary), never int-as-seconds.
		assert!(matches!(
			ConfigKey::CdcCompactInterval.accept(Value::Int4(60)),
			Err(AcceptError::TypeMismatch { .. })
		));
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
	fn test_metrics_retention_round_trip() {
		assert_eq!(
			"METRICS_RUNTIME_RETENTION".parse::<ConfigKey>().unwrap(),
			ConfigKey::MetricsRuntimeRetention
		);
		assert_eq!(
			"METRICS_PROFILER_RETENTION".parse::<ConfigKey>().unwrap(),
			ConfigKey::MetricsProfilerRetention
		);
		assert_eq!(format!("{}", ConfigKey::MetricsRuntimeRetention), "METRICS_RUNTIME_RETENTION");
		assert_eq!(format!("{}", ConfigKey::MetricsProfilerRetention), "METRICS_PROFILER_RETENTION");
	}

	#[test]
	fn test_metrics_retention_defaults_are_7d_and_1h() {
		// Runtime snapshots are sampled every few seconds, so a week is the cap before eviction;
		// profiler aggregates are far noisier, so they default to a single hour.
		assert_eq!(ConfigKey::MetricsRuntimeRetention.default_value(), Value::duration_seconds(7 * 24 * 3600));
		assert_eq!(ConfigKey::MetricsProfilerRetention.default_value(), Value::duration_seconds(3600));
	}

	#[test]
	fn test_metrics_retention_metadata() {
		for key in [ConfigKey::MetricsRuntimeRetention, ConfigKey::MetricsProfilerRetention] {
			assert_eq!(key.expected_types(), &[ValueType::Duration], "{key}");
			assert!(!key.is_optional(), "{key} is always defaulted, never unset");
		}
	}

	#[test]
	fn test_metrics_retention_rejects_zero() {
		// Zero retention would map the eviction cutoff to "now" and wipe every snapshot on the next
		// scan, so it must be rejected like the other positive-duration knobs.
		for key in [ConfigKey::MetricsRuntimeRetention, ConfigKey::MetricsProfilerRetention] {
			match key.accept(Value::duration_seconds(0)).unwrap_err() {
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
	fn test_metrics_profiler_snapshot_interval_default_is_none() {
		// Snapshot persistence is opt-in: leaving this key untouched must not spawn the
		// ProfilerSnapshotActor or grow system::metrics::profiler::*::snapshots. A consumer
		// that wants persisted profiler snapshots sets this explicitly to a positive duration.
		assert_eq!(
			ConfigKey::MetricsProfilerSnapshotInterval.default_value(),
			Value::None {
				inner: ValueType::Duration,
			}
		);
	}

	#[test]
	fn test_metrics_profiler_snapshot_interval_accepts_none_to_disable_persistence() {
		// None is the mechanism a consumer (e.g. raptor, which only ever reads the live
		// in-memory accumulator and never queries the persisted ::snapshots series) uses to
		// stop ProfilerSnapshotActor from being spawned at all, eliminating unbounded
		// system::metrics::profiler::*::snapshots disk growth.
		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(ConfigKey::MetricsProfilerSnapshotInterval.accept(none.clone()).unwrap(), none);
	}

	#[test]
	fn test_metrics_profiler_snapshot_interval_rejects_zero_and_negative() {
		// A zero or negative tick interval would either busy-loop the snapshot actor or fail
		// to schedule its timer, so it must be rejected like every other positive-duration
		// knob rather than silently misbehaving at runtime.
		match ConfigKey::MetricsProfilerSnapshotInterval.accept(Value::duration_seconds(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert!(matches!(
			ConfigKey::MetricsProfilerSnapshotInterval.accept(Value::duration_seconds(-5)),
			Err(AcceptError::InvalidValue(_))
		));
	}

	#[test]
	fn test_metrics_profiler_snapshot_interval_requires_restart() {
		// ProfilerSnapshotActor::init() arms a single fixed-period ctx.schedule_tick(...) at
		// actor start and never re-reads this config live, so a change without a restart would
		// silently have no effect. This test exists so a future change that makes the actor
		// live-reconfigurable doesn't forget to flip this bit back to false.
		assert!(ConfigKey::MetricsProfilerSnapshotInterval.requires_restart());
	}

	#[test]
	fn test_metrics_profiler_snapshot_interval_round_trips_through_display_and_from_str() {
		assert_eq!(
			"METRICS_PROFILER_SNAPSHOT_INTERVAL".parse::<ConfigKey>().unwrap(),
			ConfigKey::MetricsProfilerSnapshotInterval
		);
		assert_eq!(
			format!("{}", ConfigKey::MetricsProfilerSnapshotInterval),
			"METRICS_PROFILER_SNAPSHOT_INTERVAL"
		);
	}

	#[test]
	fn test_metrics_profiler_snapshot_interval_in_all() {
		assert!(ConfigKey::all().contains(&ConfigKey::MetricsProfilerSnapshotInterval));
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
