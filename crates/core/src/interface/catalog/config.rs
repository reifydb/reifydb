// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, str::FromStr};

use reifydb_runtime::version_epoch::BUCKET_WIDTH;
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
	QueryRowBatchSize,
	QueryMemoryLimit,
	RetentionEvictInterval,
	RetentionEvictBatchSize,
	RetentionEvictMaxBatchesPerTick,
	EpochBucketInterval,
	RetentionStartupGrace,
	MaxRetentionHorizonFloor,
	HistoricalGcBatchSize,
	HistoricalGcInterval,
	CdcTtlDuration,
	CdcTtlScanInterval,
	CdcTtlScanBatchSize,
	CdcWalAutocheckpoint,
	CdcCommitBufferBytes,
	CdcBlockCutBytes,
	CdcReadBufferBytes,
	MultiPointBufferShardBytes,
	MultiRangeBufferShardBytes,
	OperatorPointBufferShardBytes,
	OperatorRangeBufferShardBytes,
	MultiPointBufferShards,
	MultiRangeBufferShards,
	OperatorPointBufferShards,
	OperatorRangeBufferShards,
	MultiFlushInterval,
	MultiFlushBudgetBytes,
	MultiWalAutocheckpoint,
	OperatorFlushInterval,
	OperatorFlushBudgetBytes,
	OperatorWalAutocheckpoint,
	FlowTick,
	FlowSampleInterval,
	FlowBacklogMemoryLimit,
	FlowPullBatchBytes,
	FlowLoadBatchBytes,
	CdcConsumeWaitTimeout,
	FlowJoinProbeBlockSize,
	ThreadsAsync,
	ThreadsCoordination,
	ThreadsFlow,
	ThreadsTask,
	ThreadsCompute,
	SubscriptionWorkerThreads,
	MetricsFlushInterval,
	MetricsSampleInterval,
	MetricsSnapshotInterval,
	CommitGroupLinger,
	CommitGroupMaxTransactions,
	TombstoneReapInterval,
	TombstoneReapBatchSize,
	QueueLeaseReapInterval,
	QueueLeaseReapBatchSize,
	QueueRetentionInterval,
	QueueRetentionBatchSize,
}

impl ConfigKey {
	pub fn all() -> &'static [Self] {
		&[
			Self::OracleWindowSize,
			Self::QueryRowBatchSize,
			Self::QueryMemoryLimit,
			Self::RetentionEvictInterval,
			Self::RetentionEvictBatchSize,
			Self::RetentionEvictMaxBatchesPerTick,
			Self::EpochBucketInterval,
			Self::RetentionStartupGrace,
			Self::MaxRetentionHorizonFloor,
			Self::HistoricalGcBatchSize,
			Self::HistoricalGcInterval,
			Self::CdcTtlDuration,
			Self::CdcTtlScanInterval,
			Self::CdcTtlScanBatchSize,
			Self::CdcWalAutocheckpoint,
			Self::CdcCommitBufferBytes,
			Self::CdcBlockCutBytes,
			Self::CdcReadBufferBytes,
			Self::MultiPointBufferShardBytes,
			Self::MultiRangeBufferShardBytes,
			Self::OperatorPointBufferShardBytes,
			Self::OperatorRangeBufferShardBytes,
			Self::MultiPointBufferShards,
			Self::MultiRangeBufferShards,
			Self::OperatorPointBufferShards,
			Self::OperatorRangeBufferShards,
			Self::MultiFlushInterval,
			Self::MultiFlushBudgetBytes,
			Self::MultiWalAutocheckpoint,
			Self::OperatorFlushInterval,
			Self::OperatorFlushBudgetBytes,
			Self::OperatorWalAutocheckpoint,
			Self::FlowTick,
			Self::FlowSampleInterval,
			Self::FlowBacklogMemoryLimit,
			Self::FlowPullBatchBytes,
			Self::FlowLoadBatchBytes,
			Self::CdcConsumeWaitTimeout,
			Self::FlowJoinProbeBlockSize,
			Self::ThreadsAsync,
			Self::ThreadsCoordination,
			Self::ThreadsFlow,
			Self::ThreadsTask,
			Self::ThreadsCompute,
			Self::SubscriptionWorkerThreads,
			Self::MetricsFlushInterval,
			Self::MetricsSampleInterval,
			Self::MetricsSnapshotInterval,
			Self::CommitGroupLinger,
			Self::CommitGroupMaxTransactions,
			Self::TombstoneReapInterval,
			Self::TombstoneReapBatchSize,
			Self::QueueLeaseReapInterval,
			Self::QueueLeaseReapBatchSize,
			Self::QueueRetentionInterval,
			Self::QueueRetentionBatchSize,
		]
	}

	pub fn default_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(500),
			Self::QueryRowBatchSize => Value::Uint2(128),
			Self::QueryMemoryLimit => Value::Uint8(1024 * 1024 * 1024),
			Self::RetentionEvictInterval => Value::duration_seconds(60),
			Self::RetentionEvictBatchSize => Value::Uint8(1024),
			Self::RetentionEvictMaxBatchesPerTick => Value::Uint8(8),
			Self::EpochBucketInterval => Value::duration_seconds(60),
			Self::RetentionStartupGrace => Value::duration_seconds(300),
			Self::MaxRetentionHorizonFloor => Value::duration_seconds(7 * 24 * 60 * 60),
			Self::HistoricalGcBatchSize => Value::Uint8(50_000),
			Self::HistoricalGcInterval => Value::duration_seconds(30),
			Self::CdcTtlDuration => Value::None {
				inner: ValueType::Duration,
			},
			Self::CdcTtlScanInterval => Value::duration_seconds(30),
			Self::CdcTtlScanBatchSize => Value::Uint8(8192),
			Self::CdcWalAutocheckpoint => Value::Uint8(10000),
			Self::CdcCommitBufferBytes => Value::Uint8(256 * 1024 * 1024),
			Self::CdcBlockCutBytes => Value::Uint8(4 * 1024 * 1024),
			Self::CdcReadBufferBytes => Value::Uint8(256 * 1024 * 1024),
			Self::MultiPointBufferShardBytes => Value::Uint8(4 * 1024 * 1024),
			Self::MultiRangeBufferShardBytes => Value::Uint8(4 * 1024 * 1024),
			Self::OperatorPointBufferShardBytes => Value::Uint8(4 * 1024 * 1024),
			Self::OperatorRangeBufferShardBytes => Value::Uint8(4 * 1024 * 1024),
			Self::MultiPointBufferShards => Value::Uint2(16),
			Self::MultiRangeBufferShards => Value::Uint2(16),
			Self::OperatorPointBufferShards => Value::Uint2(16),
			Self::OperatorRangeBufferShards => Value::Uint2(16),
			Self::MultiFlushInterval => Value::duration_seconds(5),
			Self::MultiFlushBudgetBytes => Value::Uint8(4 * 1024 * 1024),
			Self::MultiWalAutocheckpoint => Value::Uint8(10000),
			Self::OperatorFlushInterval => Value::duration_seconds(5),
			Self::OperatorFlushBudgetBytes => Value::Uint8(4 * 1024 * 1024),
			Self::OperatorWalAutocheckpoint => Value::Uint8(10000),
			Self::FlowTick => Value::duration_seconds(1),
			Self::FlowSampleInterval => Value::duration_seconds(60),
			Self::FlowBacklogMemoryLimit => Value::Uint8(64 * 1024 * 1024),
			Self::FlowPullBatchBytes => Value::Uint8(8 * 1024 * 1024),
			Self::FlowLoadBatchBytes => Value::Uint8(8 * 1024 * 1024),
			Self::CdcConsumeWaitTimeout => Value::duration_seconds(30),
			Self::FlowJoinProbeBlockSize => Value::Uint8(1024),
			Self::ThreadsAsync => Value::Uint2(1),
			Self::ThreadsCoordination => Value::Uint2(2),
			Self::ThreadsFlow => Value::Uint2(2),
			Self::ThreadsTask => Value::Uint2(2),
			Self::ThreadsCompute => Value::Uint2(2),
			Self::SubscriptionWorkerThreads => Value::Uint2(0),
			Self::MetricsFlushInterval => Value::duration_seconds(10),
			Self::MetricsSampleInterval => Value::duration_seconds(10),
			Self::MetricsSnapshotInterval => Value::None {
				inner: ValueType::Duration,
			},
			Self::CommitGroupLinger => Value::None {
				inner: ValueType::Duration,
			},
			Self::CommitGroupMaxTransactions => Value::Uint8(256),
			Self::TombstoneReapInterval => Value::duration_seconds(1),
			Self::TombstoneReapBatchSize => Value::Uint8(1024),
			Self::QueueLeaseReapInterval => Value::duration_seconds(5),
			Self::QueueLeaseReapBatchSize => Value::Uint8(1024),
			Self::QueueRetentionInterval => Value::duration_seconds(60),
			Self::QueueRetentionBatchSize => Value::Uint8(1024),
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::QueryRowBatchSize => {
				"Number of rows produced per batch by query / DML pipeline operators."
			}
			Self::QueryMemoryLimit => {
				"Maximum bytes a single query may buffer in memory across its blocking operators (joins, sort, top k, distinct) and its accumulated result. A query that would exceed this fails with QUERY_006 instead of growing without bound. Read fresh for each query, so changes take effect immediately."
			}
			Self::RetentionEvictInterval => {
				"How often the retention evictor scans objects with a row TTL for expired rows."
			}
			Self::RetentionEvictBatchSize => {
				"Max rows examined (and thus evicted) per transaction during a retention eviction tick."
			}
			Self::RetentionEvictMaxBatchesPerTick => {
				"Upper bound on eviction transactions per retention tick. Caps how long one tick can run when draining a backlog; remaining work resumes on the next tick."
			}
			Self::EpochBucketInterval => {
				"Wall-clock width of one durable version-epoch bucket. The epoch log persists at most one \
				 (bucket, commit version) sample per bucket, and those samples are what let TTLs resolve a \
				 cutoff after a restart. Smaller buckets give finer expiry resolution at the cost of more \
				 persisted samples over the retention horizon."
			}
			Self::RetentionStartupGrace => {
				"How long after startup every retention executor computes cutoffs but deletes nothing. A \
				 process restarted after a long downtime wakes with a large expired backlog; the grace \
				 period plus per-class budgets drain it over many ticks instead of one mass eviction."
			}
			Self::MaxRetentionHorizonFloor => {
				"Lower bound on the retained version-epoch horizon. The horizon is the longest declared \
				 TTL in the catalog, never less than this floor; epoch samples older than the horizon are \
				 pruned. A TTL longer than the horizon could not resolve a cutoff, so it is rejected at \
				 declaration time rather than silently never expiring."
			}
			Self::HistoricalGcBatchSize => {
				"Max historical (key, version) pairs scanned per object per historical GC tick."
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
			Self::CdcWalAutocheckpoint => {
				"WAL frame threshold (SQLite wal_autocheckpoint PRAGMA) for the CDC log's SQLite tier. \
				 CDC has no explicit checkpoint of its own, so this is the sole control over how often \
				 cdc.db's WAL is checkpointed into the main file. Higher values checkpoint less often with \
				 a larger WAL; since CDC is written on the commit path, this also bounds how often a commit \
				 pays an inline auto-checkpoint. Read once at boot; changing it requires a restart."
			}
			Self::CdcCommitBufferBytes => {
				"Upper bound on unflushed CDC bytes held in the commit buffer. A writer that would push the \
				 buffer past this stalls until the flusher drains it, so this is the back-pressure point \
				 between the commit path and the persistent tier. Read once at boot."
			}
			Self::CdcBlockCutBytes => {
				"Target size of one CDC block. The commit buffer cuts a block once its pending bytes reach \
				 this, and that block is the unit of flush, of persistent storage, and of read-cache \
				 residency. Larger blocks compress better but coarsen retention, which drops whole blocks. \
				 Read once at boot."
			}
			Self::CdcReadBufferBytes => {
				"Resident byte budget for the CDC read cache of decoded blocks, split evenly across its \
				 shards. None disables the cache outright, so every miss below the commit buffer decodes a \
				 block straight from the persistent tier. Read once at boot."
			}
			Self::MultiPointBufferShardBytes => {
				"Resident byte budget for each shard of the multi-version point cache; total cache memory is \
				 this value times the shard count. None disables the cache outright, so \
				 every point read that misses the commit buffer goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiRangeBufferShardBytes => {
				"Resident byte budget for each shard of the multi-version range cache; total cache memory is \
				 this value times the shard count. None disables the cache outright, so \
				 every multi-version range scan goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::OperatorPointBufferShardBytes => {
				"Resident byte budget for each shard of the operator-state point cache; total cache memory is \
				 this value times the shard count. None disables the cache outright, so \
				 every point read that misses the commit buffer goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::OperatorRangeBufferShardBytes => {
				"Resident byte budget for each shard of the operator-state range cache; total cache memory is \
				 this value times the shard count. None disables the cache outright, so \
				 every operator range scan goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiPointBufferShards => {
				"Number of lock-striped shards in the multi-version point cache. Each shard carries its own \
				 byte budget, so raising this raises total cache memory proportionally rather \
				 than dividing a fixed pot. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiRangeBufferShards => {
				"Number of lock-striped shards in the multi-version range cache. Each shard carries its own \
				 byte budget, so raising this raises total cache memory proportionally rather \
				 than dividing a fixed pot. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::OperatorPointBufferShards => {
				"Number of lock-striped shards in the operator-state point cache. Each shard carries its own \
				 byte budget, so raising this raises total cache memory proportionally rather \
				 than dividing a fixed pot. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::OperatorRangeBufferShards => {
				"Number of lock-striped shards in the operator-state range cache. Each shard carries its own \
				 byte budget, so raising this raises total cache memory proportionally rather \
				 than dividing a fixed pot. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiFlushInterval => {
				"How often the persistent-flush actor drains the in-memory commit buffer into the multi \
				 store's SQLite tier. Longer intervals coalesce more writes per flush - a larger WAL - at \
				 the cost of more resident commit-buffer memory and a longer window before data is \
				 materialized in the persistent file. Read once at boot; changing it requires a restart."
			}
			Self::MultiFlushBudgetBytes => {
				"Maximum bytes of buffered entries the persistent-flush class moves from the commit \
				 buffer to the SQLite tier in one slice. Bounds how long a single flush holds the lane, \
				 so a large backlog drains across ticks instead of stalling every other retention class \
				 behind it."
			}
			Self::MultiWalAutocheckpoint => {
				"WAL frame threshold for the multi store's SQLite tier: sets the SQLite \
				 wal_autocheckpoint PRAGMA that governs when SQLite folds the WAL back into the main \
				 database. Higher values checkpoint less often with a larger WAL, reducing checkpoint \
				 I/O; lower values keep the WAL small at the cost of more frequent checkpoints. Read once \
				 at boot; changing it requires a restart."
			}
			Self::OperatorFlushInterval => {
				"How often the persistent-flush actor drains the in-memory commit buffer into the \
				 operator store's SQLite tier. Longer intervals coalesce more writes per flush - a \
				 larger WAL - at the cost of more resident commit-buffer memory and a longer window \
				 before operator state is materialized in the persistent file. Read once at boot; \
				 changing it requires a restart."
			}
			Self::OperatorFlushBudgetBytes => {
				"Maximum bytes the persistent-flush class moves from the operator commit buffer to \
				 the SQLite tier in one slice. Bounds how long a single flush holds the lane, so a \
				 large backlog drains across ticks instead of stalling every other retention class \
				 behind it."
			}
			Self::OperatorWalAutocheckpoint => {
				"WAL frame threshold for the operator store's SQLite tier: sets the SQLite \
				 wal_autocheckpoint PRAGMA that governs when SQLite folds the WAL back into the main \
				 database. Higher values checkpoint less often with a larger WAL, reducing checkpoint \
				 I/O; lower values keep the WAL small at the cost of more frequent checkpoints. Read \
				 once at boot; changing it requires a restart."
			}
			Self::FlowTick => {
				"How often the deferred and transactional flow tick coordinators wake up to dispatch \
				 due flows."
			}
			Self::FlowSampleInterval => {
				"How often each flow actor samples its operators' approximate memory into the \
				 system::metrics::runtime::memory samples (scope operator::N). Runs on the operator's \
				 own thread, off the apply path. When none, operator sampling is disabled entirely; when \
				 set, must be > 0."
			}
			Self::FlowBacklogMemoryLimit => {
				"Byte ceiling of the shared in-memory backlog of decoded CDC entries that feeds flow \
				 consumers. Producer-fed at commit granularity; entries below every flow's cursor are \
				 dropped eagerly and the lowest versions are evicted first once the ceiling is exceeded, \
				 at which point a flow that far behind reloads from disk through the catch-up loader. \
				 Because payload rows are shared, the tally is an upper bound of unique memory."
			}
			Self::FlowPullBatchBytes => {
				"Byte budget a flow actor applies per pull from the CDC backlog. A flow that has fallen \
				 behind receives up to this many bytes of decoded changes in one slice, so catch-up is \
				 vectorized instead of per-version."
			}
			Self::FlowLoadBatchBytes => {
				"Byte budget of one catch-up loader read from the CDC log on behalf of flows that are \
				 behind the in-memory backlog. Identical concurrent requests share a single read."
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
			Self::MetricsFlushInterval => {
				"How often the metric collector flushes accumulated storage and CDC accounting into the \
				 system::metrics KV store that backs the storage and cdc views. Must be > 0."
			}
			Self::MetricsSampleInterval => {
				"How often the metrics sampler polls every domain, rolls the window and publishes the \
				 system::metrics ::current and ::total caches. Always on; there is no off value, only a \
				 cadence. Must be > 0. Read once at boot; changing it requires a restart."
			}
			Self::MetricsSnapshotInterval => {
				"How often the published ::current reading of every domain is appended to its ::snapshots \
				 series. When none, no snapshot is ever written; when set, must be > 0 and not shorter than \
				 METRICS_SAMPLE_INTERVAL. Read once at boot; changing it requires a restart."
			}
			Self::CommitGroupLinger => {
				"Maximum time an unchecked commit submitted to the group-commit coordinator waits \
				 for other commits to join its group before the merged transaction is flushed. \
				 Defaults to none, which disables grouping entirely (every submission commits \
				 immediately in its own transaction); when set, must be > 0. Read once at database \
				 construction, so changing it requires a restart."
			}
			Self::CommitGroupMaxTransactions => {
				"Upper bound on commits merged into one group-commit flush. A group is flushed as \
				 soon as it reaches this size, even before the linger expires. Must be > 0."
			}
			Self::TombstoneReapInterval => {
				"How often the tombstone reaper scans persistent tables for delete-mode tombstones whose superseding write has flushed."
			}
			Self::TombstoneReapBatchSize => {
				"Max tombstones one reap statement may physically delete per table per slice. Bounds the write-connection hold; remaining tombstones drain on the next slice."
			}
			Self::QueueLeaseReapInterval => {
				"How often the queue reaper scans for leases whose deadline has passed. A dead worker's item cannot be redelivered sooner than this, so it should stay well below any declared lease ttl."
			}
			Self::QueueLeaseReapBatchSize => {
				"Max queue item-state records one reap slice may scan. Bounds the slice on a deep backlog; the scan resumes from its cursor on the next slice."
			}
			Self::QueueRetentionInterval => {
				"How often the queue retention sweeper deletes finished items whose terminal attempt is older than the queue's declared retention.done, and deduplication records past their own ttl."
			}
			Self::QueueRetentionBatchSize => {
				"Max records one queue retention slice may scan across its item and deduplication sweeps. Remaining work drains on the next slice."
			}
		}
	}

	pub fn requires_restart(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::QueryRowBatchSize => false,
			Self::QueryMemoryLimit => false,
			Self::RetentionEvictInterval => true,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::EpochBucketInterval => false,
			Self::RetentionStartupGrace => false,
			Self::MaxRetentionHorizonFloor => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => false,
			Self::CdcTtlScanInterval => true,
			Self::CdcTtlScanBatchSize => false,
			Self::CdcWalAutocheckpoint => true,
			Self::CdcCommitBufferBytes => true,
			Self::CdcBlockCutBytes => true,
			Self::CdcReadBufferBytes => true,
			Self::MultiPointBufferShardBytes => true,
			Self::MultiRangeBufferShardBytes => true,
			Self::OperatorPointBufferShardBytes => true,
			Self::OperatorRangeBufferShardBytes => true,
			Self::MultiPointBufferShards => true,
			Self::MultiRangeBufferShards => true,
			Self::OperatorPointBufferShards => true,
			Self::OperatorRangeBufferShards => true,
			Self::MultiFlushInterval => true,
			Self::MultiFlushBudgetBytes => false,
			Self::MultiWalAutocheckpoint => true,
			Self::OperatorFlushInterval => true,
			Self::OperatorFlushBudgetBytes => true,
			Self::OperatorWalAutocheckpoint => true,
			Self::FlowTick => false,
			Self::FlowSampleInterval => false,
			Self::FlowBacklogMemoryLimit => true,
			Self::FlowPullBatchBytes => true,
			Self::FlowLoadBatchBytes => true,
			Self::CdcConsumeWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => true,
			Self::ThreadsCoordination => true,
			Self::ThreadsFlow => true,
			Self::ThreadsTask => true,
			Self::ThreadsCompute => true,
			Self::SubscriptionWorkerThreads => true,
			Self::MetricsFlushInterval => false,
			Self::MetricsSampleInterval => true,
			Self::MetricsSnapshotInterval => true,
			Self::CommitGroupLinger => true,
			Self::CommitGroupMaxTransactions => true,
			Self::TombstoneReapInterval => false,
			Self::TombstoneReapBatchSize => false,
			Self::QueueLeaseReapInterval => false,
			Self::QueueLeaseReapBatchSize => false,
			Self::QueueRetentionInterval => false,
			Self::QueueRetentionBatchSize => false,
		}
	}

	pub fn expected_types(&self) -> &'static [ValueType] {
		match self {
			Self::OracleWindowSize => &[ValueType::Uint8],
			Self::QueryRowBatchSize => &[ValueType::Uint2],
			Self::QueryMemoryLimit => &[ValueType::Uint8],
			Self::RetentionEvictInterval => &[ValueType::Duration],
			Self::RetentionEvictBatchSize => &[ValueType::Uint8],
			Self::RetentionEvictMaxBatchesPerTick => &[ValueType::Uint8],
			Self::EpochBucketInterval => &[ValueType::Duration],
			Self::RetentionStartupGrace => &[ValueType::Duration],
			Self::MaxRetentionHorizonFloor => &[ValueType::Duration],
			Self::HistoricalGcBatchSize => &[ValueType::Uint8],
			Self::HistoricalGcInterval => &[ValueType::Duration],
			Self::CdcTtlDuration => &[ValueType::Duration],
			Self::CdcTtlScanInterval => &[ValueType::Duration],
			Self::CdcTtlScanBatchSize => &[ValueType::Uint8],
			Self::CdcWalAutocheckpoint => &[ValueType::Uint8],
			Self::CdcCommitBufferBytes => &[ValueType::Uint8],
			Self::CdcBlockCutBytes => &[ValueType::Uint8],
			Self::CdcReadBufferBytes => &[ValueType::Uint8],
			Self::MultiPointBufferShardBytes => &[ValueType::Uint8],
			Self::MultiRangeBufferShardBytes => &[ValueType::Uint8],
			Self::OperatorPointBufferShardBytes => &[ValueType::Uint8],
			Self::OperatorRangeBufferShardBytes => &[ValueType::Uint8],
			Self::MultiPointBufferShards => &[ValueType::Uint2],
			Self::MultiRangeBufferShards => &[ValueType::Uint2],
			Self::OperatorPointBufferShards => &[ValueType::Uint2],
			Self::OperatorRangeBufferShards => &[ValueType::Uint2],
			Self::MultiFlushInterval => &[ValueType::Duration],
			Self::MultiFlushBudgetBytes => &[ValueType::Uint8],
			Self::MultiWalAutocheckpoint => &[ValueType::Uint8],
			Self::OperatorFlushInterval => &[ValueType::Duration],
			Self::OperatorFlushBudgetBytes => &[ValueType::Uint8],
			Self::OperatorWalAutocheckpoint => &[ValueType::Uint8],
			Self::FlowTick => &[ValueType::Duration],
			Self::FlowSampleInterval => &[ValueType::Duration],
			Self::FlowBacklogMemoryLimit => &[ValueType::Uint8],
			Self::FlowPullBatchBytes => &[ValueType::Uint8],
			Self::FlowLoadBatchBytes => &[ValueType::Uint8],
			Self::CdcConsumeWaitTimeout => &[ValueType::Duration],
			Self::FlowJoinProbeBlockSize => &[ValueType::Uint8],
			Self::ThreadsAsync => &[ValueType::Uint2],
			Self::ThreadsCoordination => &[ValueType::Uint2],
			Self::ThreadsFlow => &[ValueType::Uint2],
			Self::ThreadsTask => &[ValueType::Uint2],
			Self::ThreadsCompute => &[ValueType::Uint2],
			Self::SubscriptionWorkerThreads => &[ValueType::Uint2],
			Self::MetricsFlushInterval => &[ValueType::Duration],
			Self::MetricsSampleInterval => &[ValueType::Duration],
			Self::MetricsSnapshotInterval => &[ValueType::Duration],
			Self::CommitGroupLinger => &[ValueType::Duration],
			Self::CommitGroupMaxTransactions => &[ValueType::Uint8],
			Self::TombstoneReapInterval => &[ValueType::Duration],
			Self::TombstoneReapBatchSize => &[ValueType::Uint8],
			Self::QueueLeaseReapInterval => &[ValueType::Duration],
			Self::QueueLeaseReapBatchSize => &[ValueType::Uint8],
			Self::QueueRetentionInterval => &[ValueType::Duration],
			Self::QueueRetentionBatchSize => &[ValueType::Uint8],
		}
	}

	pub fn is_optional(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::QueryRowBatchSize => false,
			Self::QueryMemoryLimit => false,
			Self::RetentionEvictInterval => false,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::EpochBucketInterval => false,
			Self::RetentionStartupGrace => false,
			Self::MaxRetentionHorizonFloor => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::CdcTtlDuration => true,
			Self::CdcTtlScanInterval => false,
			Self::CdcTtlScanBatchSize => false,
			Self::CdcWalAutocheckpoint => false,
			Self::CdcCommitBufferBytes => false,
			Self::CdcBlockCutBytes => false,
			Self::CdcReadBufferBytes => true,
			Self::MultiPointBufferShardBytes => true,
			Self::MultiRangeBufferShardBytes => true,
			Self::OperatorPointBufferShardBytes => true,
			Self::OperatorRangeBufferShardBytes => true,
			Self::MultiPointBufferShards => false,
			Self::MultiRangeBufferShards => false,
			Self::OperatorPointBufferShards => false,
			Self::OperatorRangeBufferShards => false,
			Self::MultiFlushInterval => false,
			Self::MultiFlushBudgetBytes => false,
			Self::MultiWalAutocheckpoint => false,
			Self::OperatorFlushInterval => false,
			Self::OperatorFlushBudgetBytes => false,
			Self::OperatorWalAutocheckpoint => false,
			Self::FlowTick => false,
			Self::FlowSampleInterval => true,
			Self::FlowBacklogMemoryLimit => false,
			Self::FlowPullBatchBytes => false,
			Self::FlowLoadBatchBytes => false,
			Self::CdcConsumeWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => false,
			Self::ThreadsCoordination => false,
			Self::ThreadsFlow => false,
			Self::ThreadsTask => false,
			Self::ThreadsCompute => false,
			Self::SubscriptionWorkerThreads => false,
			Self::MetricsFlushInterval => false,
			Self::MetricsSampleInterval => false,
			Self::MetricsSnapshotInterval => true,
			Self::CommitGroupLinger => true,
			Self::CommitGroupMaxTransactions => false,
			Self::TombstoneReapInterval => false,
			Self::TombstoneReapBatchSize => false,
			Self::QueueLeaseReapInterval => false,
			Self::QueueLeaseReapBatchSize => false,
			Self::QueueRetentionInterval => false,
			Self::QueueRetentionBatchSize => false,
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
			Self::EpochBucketInterval => match value {
				Value::Duration(d) if !d.is_positive() => {
					Err("EPOCH_BUCKET_INTERVAL must be greater than zero".to_string())
				}
				Value::Duration(d) if d.to_std().as_secs() < BUCKET_WIDTH.seconds() => Err(format!(
					"EPOCH_BUCKET_INTERVAL must be at least {}s: the version epoch resolves cutoffs at \
					 second granularity, so a shorter bucket truncates to zero and silently disables \
					 coarse compaction",
					BUCKET_WIDTH.seconds()
				)),
				_ => Ok(()),
			},
			Self::RetentionStartupGrace => match value {
				Value::Duration(d) if d.is_negative() => {
					Err("RETENTION_STARTUP_GRACE must not be negative".to_string())
				}
				_ => Ok(()),
			},
			Self::MaxRetentionHorizonFloor => match value {
				Value::Duration(d) if !d.is_positive() => {
					Err("MAX_RETENTION_HORIZON_FLOOR must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::QueryRowBatchSize => match value {
				Value::Uint2(0) => Err("QUERY_ROW_BATCH_SIZE must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::QueryMemoryLimit => match value {
				Value::Uint8(0) => Err("QUERY_MEMORY_LIMIT must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::FlowBacklogMemoryLimit => match value {
				Value::Uint8(0) => {
					Err("FLOW_BACKLOG_MEMORY_LIMIT must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::FlowPullBatchBytes => match value {
				Value::Uint8(0) => Err("FLOW_PULL_BATCH_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::FlowLoadBatchBytes => match value {
				Value::Uint8(0) => Err("FLOW_LOAD_BATCH_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiPointBufferShardBytes => match value {
				Value::Uint8(0) => Err(
					"MULTI_POINT_BUFFER_SHARD_BYTES must be greater than zero; use none to disable the point cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::MultiRangeBufferShardBytes => match value {
				Value::Uint8(0) => Err(
					"MULTI_RANGE_BUFFER_SHARD_BYTES must be greater than zero; use none to disable the range cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::OperatorPointBufferShardBytes => match value {
				Value::Uint8(0) => Err(
					"OPERATOR_POINT_BUFFER_SHARD_BYTES must be greater than zero; use none to disable the point cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::OperatorRangeBufferShardBytes => match value {
				Value::Uint8(0) => Err(
					"OPERATOR_RANGE_BUFFER_SHARD_BYTES must be greater than zero; use none to disable the range cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::MultiPointBufferShards => match value {
				Value::Uint2(0) => Err("MULTI_POINT_BUFFER_SHARDS must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiRangeBufferShards => match value {
				Value::Uint2(0) => Err("MULTI_RANGE_BUFFER_SHARDS must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorPointBufferShards => match value {
				Value::Uint2(0) => Err("OPERATOR_POINT_BUFFER_SHARDS must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorRangeBufferShards => match value {
				Value::Uint2(0) => Err("OPERATOR_RANGE_BUFFER_SHARDS must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CdcCommitBufferBytes => match value {
				Value::Uint8(0) => Err("CDC_COMMIT_BUFFER_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CdcBlockCutBytes => match value {
				Value::Uint8(0) => Err("CDC_BLOCK_CUT_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CdcReadBufferBytes => match value {
				Value::Uint8(0) => Err(
					"CDC_READ_BUFFER_BYTES must be greater than zero; use none to disable the block cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::MultiFlushInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("MULTI_FLUSH_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiFlushBudgetBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => Err("MULTI_FLUSH_BUDGET_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiWalAutocheckpoint => match value {
				Value::Uint8(0) => {
					Err("MULTI_WAL_AUTOCHECKPOINT must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::OperatorFlushInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("OPERATOR_FLUSH_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorFlushBudgetBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => {
					Err("OPERATOR_FLUSH_BUDGET_BYTES must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::OperatorWalAutocheckpoint => match value {
				Value::Uint8(0) => {
					Err("OPERATOR_WAL_AUTOCHECKPOINT must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::CdcWalAutocheckpoint => match value {
				Value::Uint8(0) => Err("CDC_WAL_AUTOCHECKPOINT must be greater than zero".to_string()),
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
			Self::FlowSampleInterval => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("FLOW_SAMPLE_INTERVAL must be greater than zero".to_string())
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
			Self::MetricsFlushInterval => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_FLUSH_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::MetricsSampleInterval => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_SAMPLE_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::MetricsSnapshotInterval => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("METRICS_SNAPSHOT_INTERVAL must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::CommitGroupLinger => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("COMMIT_GROUP_LINGER must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			Self::CommitGroupMaxTransactions => match value {
				Value::Uint8(0) => {
					Err("COMMIT_GROUP_MAX_TRANSACTIONS must be greater than zero".to_string())
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
			Self::QueryRowBatchSize => write!(f, "QUERY_ROW_BATCH_SIZE"),
			Self::QueryMemoryLimit => write!(f, "QUERY_MEMORY_LIMIT"),
			Self::RetentionEvictInterval => write!(f, "RETENTION_EVICT_INTERVAL"),
			Self::RetentionEvictBatchSize => write!(f, "RETENTION_EVICT_BATCH_SIZE"),
			Self::RetentionEvictMaxBatchesPerTick => write!(f, "RETENTION_EVICT_MAX_BATCHES_PER_TICK"),
			Self::EpochBucketInterval => write!(f, "EPOCH_BUCKET_INTERVAL"),
			Self::RetentionStartupGrace => write!(f, "RETENTION_STARTUP_GRACE"),
			Self::MaxRetentionHorizonFloor => write!(f, "MAX_RETENTION_HORIZON_FLOOR"),
			Self::HistoricalGcBatchSize => write!(f, "HISTORICAL_GC_BATCH_SIZE"),
			Self::HistoricalGcInterval => write!(f, "HISTORICAL_GC_INTERVAL"),
			Self::CdcTtlDuration => write!(f, "CDC_TTL_DURATION"),
			Self::CdcTtlScanInterval => write!(f, "CDC_TTL_SCAN_INTERVAL"),
			Self::CdcTtlScanBatchSize => write!(f, "CDC_TTL_SCAN_BATCH_SIZE"),
			Self::CdcWalAutocheckpoint => write!(f, "CDC_WAL_AUTOCHECKPOINT"),
			Self::CdcCommitBufferBytes => write!(f, "CDC_COMMIT_BUFFER_BYTES"),
			Self::CdcBlockCutBytes => write!(f, "CDC_BLOCK_CUT_BYTES"),
			Self::CdcReadBufferBytes => write!(f, "CDC_READ_BUFFER_BYTES"),
			Self::MultiPointBufferShardBytes => write!(f, "MULTI_POINT_BUFFER_SHARD_BYTES"),
			Self::MultiRangeBufferShardBytes => write!(f, "MULTI_RANGE_BUFFER_SHARD_BYTES"),
			Self::OperatorPointBufferShardBytes => write!(f, "OPERATOR_POINT_BUFFER_SHARD_BYTES"),
			Self::OperatorRangeBufferShardBytes => write!(f, "OPERATOR_RANGE_BUFFER_SHARD_BYTES"),
			Self::MultiPointBufferShards => write!(f, "MULTI_POINT_BUFFER_SHARDS"),
			Self::MultiRangeBufferShards => write!(f, "MULTI_RANGE_BUFFER_SHARDS"),
			Self::OperatorPointBufferShards => write!(f, "OPERATOR_POINT_BUFFER_SHARDS"),
			Self::OperatorRangeBufferShards => write!(f, "OPERATOR_RANGE_BUFFER_SHARDS"),
			Self::MultiFlushInterval => write!(f, "MULTI_FLUSH_INTERVAL"),
			Self::MultiFlushBudgetBytes => write!(f, "MULTI_FLUSH_BUDGET_BYTES"),
			Self::MultiWalAutocheckpoint => write!(f, "MULTI_WAL_AUTOCHECKPOINT"),
			Self::OperatorFlushInterval => write!(f, "OPERATOR_FLUSH_INTERVAL"),
			Self::OperatorFlushBudgetBytes => write!(f, "OPERATOR_FLUSH_BUDGET_BYTES"),
			Self::OperatorWalAutocheckpoint => write!(f, "OPERATOR_WAL_AUTOCHECKPOINT"),
			Self::FlowTick => write!(f, "FLOW_TICK"),
			Self::FlowSampleInterval => write!(f, "FLOW_SAMPLE_INTERVAL"),
			Self::FlowBacklogMemoryLimit => write!(f, "FLOW_BACKLOG_MEMORY_LIMIT"),
			Self::FlowPullBatchBytes => write!(f, "FLOW_PULL_BATCH_BYTES"),
			Self::FlowLoadBatchBytes => write!(f, "FLOW_LOAD_BATCH_BYTES"),
			Self::CdcConsumeWaitTimeout => write!(f, "CDC_CONSUME_WAIT_TIMEOUT"),
			Self::FlowJoinProbeBlockSize => write!(f, "FLOW_JOIN_PROBE_BLOCK_SIZE"),
			Self::ThreadsAsync => write!(f, "THREADS_ASYNC"),
			Self::ThreadsCoordination => write!(f, "THREADS_COORDINATION"),
			Self::ThreadsFlow => write!(f, "THREADS_FLOW"),
			Self::ThreadsTask => write!(f, "THREADS_TASK"),
			Self::ThreadsCompute => write!(f, "THREADS_COMPUTE"),
			Self::SubscriptionWorkerThreads => write!(f, "SUBSCRIPTION_WORKER_THREADS"),
			Self::MetricsFlushInterval => write!(f, "METRICS_FLUSH_INTERVAL"),
			Self::MetricsSampleInterval => write!(f, "METRICS_SAMPLE_INTERVAL"),
			Self::MetricsSnapshotInterval => write!(f, "METRICS_SNAPSHOT_INTERVAL"),
			Self::CommitGroupLinger => write!(f, "COMMIT_GROUP_LINGER"),
			Self::CommitGroupMaxTransactions => write!(f, "COMMIT_GROUP_MAX_TRANSACTIONS"),
			Self::TombstoneReapInterval => write!(f, "TOMBSTONE_REAP_INTERVAL"),
			Self::TombstoneReapBatchSize => write!(f, "TOMBSTONE_REAP_BATCH_SIZE"),
			Self::QueueLeaseReapInterval => write!(f, "QUEUE_LEASE_REAP_INTERVAL"),
			Self::QueueLeaseReapBatchSize => write!(f, "QUEUE_LEASE_REAP_BATCH_SIZE"),
			Self::QueueRetentionInterval => write!(f, "QUEUE_RETENTION_INTERVAL"),
			Self::QueueRetentionBatchSize => write!(f, "QUEUE_RETENTION_BATCH_SIZE"),
		}
	}
}

impl FromStr for ConfigKey {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ORACLE_WINDOW_SIZE" => Ok(Self::OracleWindowSize),
			"QUERY_ROW_BATCH_SIZE" => Ok(Self::QueryRowBatchSize),
			"QUERY_MEMORY_LIMIT" => Ok(Self::QueryMemoryLimit),
			"RETENTION_EVICT_INTERVAL" => Ok(Self::RetentionEvictInterval),
			"RETENTION_EVICT_BATCH_SIZE" => Ok(Self::RetentionEvictBatchSize),
			"RETENTION_EVICT_MAX_BATCHES_PER_TICK" => Ok(Self::RetentionEvictMaxBatchesPerTick),
			"EPOCH_BUCKET_INTERVAL" => Ok(Self::EpochBucketInterval),
			"RETENTION_STARTUP_GRACE" => Ok(Self::RetentionStartupGrace),
			"MAX_RETENTION_HORIZON_FLOOR" => Ok(Self::MaxRetentionHorizonFloor),
			"HISTORICAL_GC_BATCH_SIZE" => Ok(Self::HistoricalGcBatchSize),
			"HISTORICAL_GC_INTERVAL" => Ok(Self::HistoricalGcInterval),
			"CDC_TTL_DURATION" => Ok(Self::CdcTtlDuration),
			"CDC_TTL_SCAN_INTERVAL" => Ok(Self::CdcTtlScanInterval),
			"CDC_TTL_SCAN_BATCH_SIZE" => Ok(Self::CdcTtlScanBatchSize),
			"CDC_WAL_AUTOCHECKPOINT" => Ok(Self::CdcWalAutocheckpoint),
			"CDC_COMMIT_BUFFER_BYTES" => Ok(Self::CdcCommitBufferBytes),
			"CDC_BLOCK_CUT_BYTES" => Ok(Self::CdcBlockCutBytes),
			"CDC_READ_BUFFER_BYTES" => Ok(Self::CdcReadBufferBytes),
			"MULTI_POINT_BUFFER_SHARD_BYTES" => Ok(Self::MultiPointBufferShardBytes),
			"MULTI_RANGE_BUFFER_SHARD_BYTES" => Ok(Self::MultiRangeBufferShardBytes),
			"OPERATOR_POINT_BUFFER_SHARD_BYTES" => Ok(Self::OperatorPointBufferShardBytes),
			"OPERATOR_RANGE_BUFFER_SHARD_BYTES" => Ok(Self::OperatorRangeBufferShardBytes),
			"MULTI_POINT_BUFFER_SHARDS" => Ok(Self::MultiPointBufferShards),
			"MULTI_RANGE_BUFFER_SHARDS" => Ok(Self::MultiRangeBufferShards),
			"OPERATOR_POINT_BUFFER_SHARDS" => Ok(Self::OperatorPointBufferShards),
			"OPERATOR_RANGE_BUFFER_SHARDS" => Ok(Self::OperatorRangeBufferShards),
			"MULTI_FLUSH_INTERVAL" => Ok(Self::MultiFlushInterval),
			"MULTI_FLUSH_BUDGET_BYTES" => Ok(Self::MultiFlushBudgetBytes),
			"MULTI_WAL_AUTOCHECKPOINT" => Ok(Self::MultiWalAutocheckpoint),
			"OPERATOR_FLUSH_INTERVAL" => Ok(Self::OperatorFlushInterval),
			"OPERATOR_FLUSH_BUDGET_BYTES" => Ok(Self::OperatorFlushBudgetBytes),
			"OPERATOR_WAL_AUTOCHECKPOINT" => Ok(Self::OperatorWalAutocheckpoint),
			"FLOW_TICK" => Ok(Self::FlowTick),
			"FLOW_SAMPLE_INTERVAL" => Ok(Self::FlowSampleInterval),
			"FLOW_BACKLOG_MEMORY_LIMIT" => Ok(Self::FlowBacklogMemoryLimit),
			"FLOW_PULL_BATCH_BYTES" => Ok(Self::FlowPullBatchBytes),
			"FLOW_LOAD_BATCH_BYTES" => Ok(Self::FlowLoadBatchBytes),
			"CDC_CONSUME_WAIT_TIMEOUT" => Ok(Self::CdcConsumeWaitTimeout),
			"FLOW_JOIN_PROBE_BLOCK_SIZE" => Ok(Self::FlowJoinProbeBlockSize),
			"THREADS_ASYNC" => Ok(Self::ThreadsAsync),
			"THREADS_COORDINATION" => Ok(Self::ThreadsCoordination),
			"THREADS_FLOW" => Ok(Self::ThreadsFlow),
			"THREADS_TASK" => Ok(Self::ThreadsTask),
			"THREADS_COMPUTE" => Ok(Self::ThreadsCompute),
			"SUBSCRIPTION_WORKER_THREADS" => Ok(Self::SubscriptionWorkerThreads),
			"METRICS_FLUSH_INTERVAL" => Ok(Self::MetricsFlushInterval),
			"METRICS_SAMPLE_INTERVAL" => Ok(Self::MetricsSampleInterval),
			"METRICS_SNAPSHOT_INTERVAL" => Ok(Self::MetricsSnapshotInterval),
			"COMMIT_GROUP_LINGER" => Ok(Self::CommitGroupLinger),
			"COMMIT_GROUP_MAX_TRANSACTIONS" => Ok(Self::CommitGroupMaxTransactions),
			"TOMBSTONE_REAP_INTERVAL" => Ok(Self::TombstoneReapInterval),
			"TOMBSTONE_REAP_BATCH_SIZE" => Ok(Self::TombstoneReapBatchSize),
			"QUEUE_LEASE_REAP_INTERVAL" => Ok(Self::QueueLeaseReapInterval),
			"QUEUE_LEASE_REAP_BATCH_SIZE" => Ok(Self::QueueLeaseReapBatchSize),
			"QUEUE_RETENTION_INTERVAL" => Ok(Self::QueueRetentionInterval),
			"QUEUE_RETENTION_BATCH_SIZE" => Ok(Self::QueueRetentionBatchSize),
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

	fn get_config_uint8_opt(&self, key: ConfigKey) -> Option<u64> {
		match self.get_config(key) {
			Value::None {
				..
			} => None,
			Value::Uint8(v) => Some(v),
			v => panic!("config key '{}' expected Uint8 or None, got {:?}", key, v),
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
		assert!(ConfigKey::OracleWindowSize.accept(Value::Uint8(0)).is_ok());
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
	fn test_query_memory_limit_defaults_and_round_trips() {
		assert_eq!(ConfigKey::QueryMemoryLimit.default_value(), Value::Uint8(1024 * 1024 * 1024));
		assert_eq!(ConfigKey::QueryMemoryLimit.expected_types(), &[ValueType::Uint8]);
		let key: ConfigKey = "QUERY_MEMORY_LIMIT".parse().unwrap();
		assert_eq!(key, ConfigKey::QueryMemoryLimit);
		assert_eq!(format!("{}", ConfigKey::QueryMemoryLimit), "QUERY_MEMORY_LIMIT");
	}

	#[test]
	fn test_query_memory_limit_rejects_zero() {
		// A zero budget would reject every query, including trivial ones, so it must not be settable.
		assert!(ConfigKey::QueryMemoryLimit.accept(Value::Uint8(0)).is_err());
		assert_eq!(ConfigKey::QueryMemoryLimit.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_query_memory_limit_requires_restart_and_optional() {
		// Read fresh for each query, so a live change takes effect immediately.
		assert!(!ConfigKey::QueryMemoryLimit.requires_restart());
		// Always defaulted to 1 GiB, never unset.
		assert!(!ConfigKey::QueryMemoryLimit.is_optional());
	}

	#[test]
	fn test_all_contains_every_compact_key_and_has_expected_len() {
		let all = ConfigKey::all();
		assert_eq!(all.len(), 56);
		assert!(all.contains(&ConfigKey::QueryMemoryLimit));
		assert!(all.contains(&ConfigKey::CommitGroupLinger));
		assert!(all.contains(&ConfigKey::CommitGroupMaxTransactions));
		assert!(all.contains(&ConfigKey::RetentionEvictInterval));
		assert!(all.contains(&ConfigKey::RetentionEvictBatchSize));
		assert!(all.contains(&ConfigKey::RetentionEvictMaxBatchesPerTick));
		assert!(all.contains(&ConfigKey::MultiFlushInterval));
		assert!(all.contains(&ConfigKey::MultiWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::OperatorFlushInterval));
		assert!(all.contains(&ConfigKey::OperatorFlushBudgetBytes));
		assert!(all.contains(&ConfigKey::OperatorWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::CdcWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::CdcConsumeWaitTimeout));
		assert!(all.contains(&ConfigKey::FlowJoinProbeBlockSize));
		assert!(all.contains(&ConfigKey::CdcTtlScanInterval));
		assert!(all.contains(&ConfigKey::CdcTtlScanBatchSize));
		assert!(all.contains(&ConfigKey::MaxRetentionHorizonFloor));
		assert!(all.contains(&ConfigKey::FlowLoadBatchBytes));
		assert!(all.contains(&ConfigKey::CdcCommitBufferBytes));
		assert!(all.contains(&ConfigKey::CdcBlockCutBytes));
		assert!(all.contains(&ConfigKey::CdcReadBufferBytes));
		assert!(all.contains(&ConfigKey::OperatorPointBufferShardBytes));
		assert!(all.contains(&ConfigKey::OperatorRangeBufferShardBytes));
		assert!(all.contains(&ConfigKey::MultiPointBufferShards));
		assert!(all.contains(&ConfigKey::MultiRangeBufferShards));
		assert!(all.contains(&ConfigKey::OperatorPointBufferShards));
		assert!(all.contains(&ConfigKey::OperatorRangeBufferShards));
		assert!(all.contains(&ConfigKey::FlowBacklogMemoryLimit));
		assert!(all.contains(&ConfigKey::FlowPullBatchBytes));
		assert!(all.contains(&ConfigKey::FlowLoadBatchBytes));
		assert!(all.contains(&ConfigKey::QueryRowBatchSize));
		assert!(all.contains(&ConfigKey::ThreadsAsync));
		assert!(all.contains(&ConfigKey::ThreadsCoordination));
		assert!(all.contains(&ConfigKey::ThreadsFlow));
		assert!(all.contains(&ConfigKey::ThreadsTask));
		assert!(all.contains(&ConfigKey::ThreadsCompute));
		assert!(all.contains(&ConfigKey::MetricsFlushInterval));
		assert!(all.contains(&ConfigKey::SubscriptionWorkerThreads));
		assert!(all.contains(&ConfigKey::FlowSampleInterval));
		assert!(all.contains(&ConfigKey::MetricsSampleInterval));
		assert!(all.contains(&ConfigKey::MetricsSnapshotInterval));
		assert!(all.contains(&ConfigKey::QueueLeaseReapInterval));
		assert!(all.contains(&ConfigKey::QueueLeaseReapBatchSize));
		assert!(all.contains(&ConfigKey::QueueRetentionInterval));
		assert!(all.contains(&ConfigKey::QueueRetentionBatchSize));
	}

	#[test]
	fn test_metrics_sample_interval_is_always_on() {
		// Sampling is the one path that populates every ::current; an off value would let a
		// domain go silently unsampled, which is exactly the failure the redesign removed.
		assert_eq!(ConfigKey::MetricsSampleInterval.default_value(), Value::duration_seconds(10));
		assert_eq!(ConfigKey::MetricsSampleInterval.expected_types(), &[ValueType::Duration]);
		assert!(!ConfigKey::MetricsSampleInterval.is_optional(), "there is no off value, only a cadence");
		assert!(ConfigKey::MetricsSampleInterval.requires_restart(), "read once at boot");

		let ten = Value::duration_seconds(10);
		assert_eq!(ConfigKey::MetricsSampleInterval.accept(ten.clone()).unwrap(), ten);
		let zero = Value::duration_seconds(0);
		assert!(matches!(ConfigKey::MetricsSampleInterval.accept(zero), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_metrics_snapshot_interval_accepts_none_and_positive_rejects_zero() {
		// none means snapshotting is off entirely; zero would write duplicate rows forever.
		assert_eq!(
			ConfigKey::MetricsSnapshotInterval.default_value(),
			Value::None {
				inner: ValueType::Duration
			},
			"snapshotting must be opt-in"
		);
		assert!(ConfigKey::MetricsSnapshotInterval.is_optional(), "none must stay accepted to turn it off");
		assert!(ConfigKey::MetricsSnapshotInterval.requires_restart(), "read once at boot");

		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(ConfigKey::MetricsSnapshotInterval.accept(none.clone()).unwrap(), none);

		let minute = Value::duration_seconds(60);
		assert_eq!(ConfigKey::MetricsSnapshotInterval.accept(minute.clone()).unwrap(), minute);

		let zero = Value::duration_seconds(0);
		match ConfigKey::MetricsSnapshotInterval.accept(zero).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("must be greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_metrics_sampler_keys_round_trip() {
		for (key, name) in [
			(ConfigKey::MetricsSampleInterval, "METRICS_SAMPLE_INTERVAL"),
			(ConfigKey::MetricsSnapshotInterval, "METRICS_SNAPSHOT_INTERVAL"),
		] {
			assert_eq!(format!("{key}"), name);
			assert_eq!(name.parse::<ConfigKey>().unwrap(), key);
		}
	}

	#[test]
	fn test_flow_sample_interval_metadata() {
		// Optional Duration knob: defaults on at once-a-minute; none disables
		// per-operator sampling entirely.
		assert_eq!(ConfigKey::FlowSampleInterval.default_value(), Value::duration_seconds(60));
		assert_eq!(ConfigKey::FlowSampleInterval.expected_types(), &[ValueType::Duration]);
		assert!(ConfigKey::FlowSampleInterval.is_optional());
	}

	#[test]
	fn test_flow_sample_interval_round_trip() {
		assert_eq!("FLOW_SAMPLE_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::FlowSampleInterval);
		assert_eq!(format!("{}", ConfigKey::FlowSampleInterval), "FLOW_SAMPLE_INTERVAL");
	}

	#[test]
	fn test_flow_sample_interval_accepts_none_and_positive_rejects_zero() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(
			ConfigKey::FlowSampleInterval.accept(none.clone()).unwrap(),
			none,
			"none must be accepted so sampling can be turned off"
		);

		let minute = Value::duration_seconds(60);
		assert_eq!(ConfigKey::FlowSampleInterval.accept(minute.clone()).unwrap(), minute);

		let zero = Value::duration_seconds(0);
		assert!(matches!(ConfigKey::FlowSampleInterval.accept(zero), Err(AcceptError::InvalidValue(_))));
	}

	#[test]
	fn test_metrics_flush_interval_metadata() {
		// A non-optional Duration knob: there is no "off" value, only a cadence.
		assert_eq!(ConfigKey::MetricsFlushInterval.default_value(), Value::duration_seconds(10));
		assert_eq!(ConfigKey::MetricsFlushInterval.expected_types(), &[ValueType::Duration]);
		assert!(!ConfigKey::MetricsFlushInterval.is_optional());
		assert!(!ConfigKey::MetricsFlushInterval.requires_restart());
	}

	#[test]
	fn test_metrics_flush_interval_round_trip() {
		assert_eq!("METRICS_FLUSH_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::MetricsFlushInterval);
		assert_eq!(format!("{}", ConfigKey::MetricsFlushInterval), "METRICS_FLUSH_INTERVAL");
	}

	#[test]
	fn test_metrics_flush_interval_accepts_positive_rejects_zero() {
		let ten = Value::duration_seconds(10);
		assert_eq!(ConfigKey::MetricsFlushInterval.accept(ten.clone()).unwrap(), ten);

		let zero = Value::duration_seconds(0);
		assert!(matches!(ConfigKey::MetricsFlushInterval.accept(zero), Err(AcceptError::InvalidValue(_))));
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
	fn test_query_row_batch_size_default_is_uint2_128() {
		assert_eq!(ConfigKey::QueryRowBatchSize.default_value(), Value::Uint2(128));
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
	fn test_accept_rejects_int4_for_uint8_key() {
		// accept is strict: SET CONFIG casts to Uint8 via cast_value before calling accept.
		assert!(matches!(
			ConfigKey::FlowLoadBatchBytes.accept(Value::Int4(1024)),
			Err(AcceptError::TypeMismatch { .. })
		));
		assert!(matches!(
			ConfigKey::FlowLoadBatchBytes.accept(Value::Int8(2048)),
			Err(AcceptError::TypeMismatch { .. })
		));
	}

	#[test]
	fn test_accept_rejects_zero_of_canonical_type() {
		match ConfigKey::FlowLoadBatchBytes.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"));
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_accept_rejects_negative_int_for_uint8_key() {
		// accept is strict on type, so an Int4 is refused before its value is ever inspected; the
		// sign is incidental.
		assert!(matches!(
			ConfigKey::FlowLoadBatchBytes.accept(Value::Int4(-1)),
			Err(AcceptError::TypeMismatch { .. })
		));
	}

	#[test]
	fn test_accept_rejects_int_for_duration_key() {
		// Bare integers carry no unit: duration keys take Duration values (or duration
		// strings cast at the CALL boundary), never int-as-seconds.
		assert!(matches!(
			ConfigKey::MaxRetentionHorizonFloor.accept(Value::Int4(60)),
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
		assert_eq!(ConfigKey::MaxRetentionHorizonFloor.accept(canonical.clone()).unwrap(), canonical);
	}

	#[test]
	fn test_accept_rejects_typed_null_for_non_optional_key() {
		let err = ConfigKey::FlowLoadBatchBytes
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
	fn test_commit_group_linger_default_is_none() {
		// Group commit is opt-in: leaving this key untouched must keep every unchecked commit on
		// its own transaction and version. Grouping is enabled by setting a positive linger.
		assert_eq!(
			ConfigKey::CommitGroupLinger.default_value(),
			Value::None {
				inner: ValueType::Duration,
			}
		);
		assert!(ConfigKey::CommitGroupLinger.is_optional());
		assert_eq!(ConfigKey::CommitGroupLinger.expected_types(), &[ValueType::Duration]);
	}

	#[test]
	fn test_commit_group_linger_accepts_none_to_disable_grouping() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		assert_eq!(ConfigKey::CommitGroupLinger.accept(none.clone()).unwrap(), none);
	}

	#[test]
	fn test_commit_group_linger_rejects_zero_and_negative() {
		// A zero linger arms a fire-immediately timer for every submission and a negative one
		// cannot schedule at all; "disabled" is expressed by absence, so zero must be rejected.
		match ConfigKey::CommitGroupLinger.accept(Value::duration_seconds(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert!(matches!(
			ConfigKey::CommitGroupLinger.accept(Value::duration_seconds(-5)),
			Err(AcceptError::InvalidValue(_))
		));
	}

	#[test]
	fn test_commit_group_linger_requires_restart() {
		// The coordinator is spawned (or not) once at database construction and never re-reads
		// this key, so a live change would silently have no effect.
		assert!(ConfigKey::CommitGroupLinger.requires_restart());
	}

	#[test]
	fn test_commit_group_linger_round_trips_through_display_and_from_str() {
		assert_eq!("COMMIT_GROUP_LINGER".parse::<ConfigKey>().unwrap(), ConfigKey::CommitGroupLinger);
		assert_eq!(format!("{}", ConfigKey::CommitGroupLinger), "COMMIT_GROUP_LINGER");
	}

	#[test]
	fn test_commit_group_max_transactions_metadata() {
		assert_eq!(ConfigKey::CommitGroupMaxTransactions.default_value(), Value::Uint8(256));
		assert_eq!(ConfigKey::CommitGroupMaxTransactions.expected_types(), &[ValueType::Uint8]);
		assert!(!ConfigKey::CommitGroupMaxTransactions.is_optional());
		assert!(ConfigKey::CommitGroupMaxTransactions.requires_restart());
	}

	#[test]
	fn test_commit_group_max_transactions_rejects_zero() {
		// A zero bound would flush every group before it could accept a single submission,
		// deadlocking every commit behind a group that can never fill.
		match ConfigKey::CommitGroupMaxTransactions.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert_eq!(ConfigKey::CommitGroupMaxTransactions.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_commit_group_max_transactions_round_trips_through_display_and_from_str() {
		assert_eq!(
			"COMMIT_GROUP_MAX_TRANSACTIONS".parse::<ConfigKey>().unwrap(),
			ConfigKey::CommitGroupMaxTransactions
		);
		assert_eq!(format!("{}", ConfigKey::CommitGroupMaxTransactions), "COMMIT_GROUP_MAX_TRANSACTIONS");
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

	#[test]
	fn test_operator_flush_interval_metadata() {
		assert_eq!(ConfigKey::OperatorFlushInterval.default_value(), Value::duration_seconds(5));
		assert_eq!(ConfigKey::OperatorFlushInterval.expected_types(), &[ValueType::Duration]);
		assert!(!ConfigKey::OperatorFlushInterval.is_optional());
		assert!(ConfigKey::OperatorFlushInterval.requires_restart());
	}

	#[test]
	fn test_operator_flush_interval_rejects_zero_and_negative() {
		// A non-positive interval can never schedule a drain, so the operator commit buffer would
		// grow without bound instead of being handed to the persistent tier.
		match ConfigKey::OperatorFlushInterval.accept(Value::duration_seconds(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert!(matches!(
			ConfigKey::OperatorFlushInterval.accept(Value::duration_seconds(-5)),
			Err(AcceptError::InvalidValue(_))
		));
	}

	#[test]
	fn test_operator_flush_interval_round_trips_through_display_and_from_str() {
		assert_eq!("OPERATOR_FLUSH_INTERVAL".parse::<ConfigKey>().unwrap(), ConfigKey::OperatorFlushInterval);
		assert_eq!(format!("{}", ConfigKey::OperatorFlushInterval), "OPERATOR_FLUSH_INTERVAL");
	}

	#[test]
	fn test_operator_flush_budget_bytes_metadata() {
		assert_eq!(ConfigKey::OperatorFlushBudgetBytes.default_value(), Value::Uint8(4 * 1024 * 1024));
		assert_eq!(ConfigKey::OperatorFlushBudgetBytes.expected_types(), &[ValueType::Uint8]);
		assert!(!ConfigKey::OperatorFlushBudgetBytes.is_optional());
		assert!(
			ConfigKey::OperatorFlushBudgetBytes.requires_restart(),
			"the budget sizes a MemoryBudget built once with the commit tier; declaring it live would \
			 promise a rewrite that no running store can adopt"
		);
	}

	#[test]
	fn test_operator_flush_budget_bytes_rejects_zero() {
		// A zero budget moves nothing per slice, so the flush lane spins forever on a backlog it
		// is never allowed to drain.
		match ConfigKey::OperatorFlushBudgetBytes.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert_eq!(ConfigKey::OperatorFlushBudgetBytes.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_operator_flush_budget_bytes_round_trips_through_display_and_from_str() {
		assert_eq!(
			"OPERATOR_FLUSH_BUDGET_BYTES".parse::<ConfigKey>().unwrap(),
			ConfigKey::OperatorFlushBudgetBytes
		);
		assert_eq!(format!("{}", ConfigKey::OperatorFlushBudgetBytes), "OPERATOR_FLUSH_BUDGET_BYTES");
	}

	#[test]
	fn test_operator_wal_autocheckpoint_metadata() {
		assert_eq!(ConfigKey::OperatorWalAutocheckpoint.default_value(), Value::Uint8(10000));
		assert_eq!(ConfigKey::OperatorWalAutocheckpoint.expected_types(), &[ValueType::Uint8]);
		assert!(!ConfigKey::OperatorWalAutocheckpoint.is_optional());
		assert!(ConfigKey::OperatorWalAutocheckpoint.requires_restart());
	}

	#[test]
	fn test_operator_wal_autocheckpoint_rejects_zero() {
		// Zero is SQLite's "never checkpoint automatically", which lets the operator WAL grow
		// without bound; disabling autocheckpointing must not be reachable by configuration.
		match ConfigKey::OperatorWalAutocheckpoint.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert_eq!(ConfigKey::OperatorWalAutocheckpoint.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_operator_wal_autocheckpoint_round_trips_through_display_and_from_str() {
		assert_eq!(
			"OPERATOR_WAL_AUTOCHECKPOINT".parse::<ConfigKey>().unwrap(),
			ConfigKey::OperatorWalAutocheckpoint
		);
		assert_eq!(format!("{}", ConfigKey::OperatorWalAutocheckpoint), "OPERATOR_WAL_AUTOCHECKPOINT");
	}
}
