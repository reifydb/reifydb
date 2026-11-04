// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt, str::FromStr};

use reifydb_runtime::version_epoch::BUCKET_WIDTH;
use reifydb_value::value::{Value, duration::Duration, value_type::ValueType};

use crate::{common::CommitVersion, default};

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
pub enum ConfigProfile {
	Production,
	Testing,
}

pub fn active_config_profile() -> ConfigProfile {
	if cfg!(feature = "testing") {
		ConfigProfile::Testing
	} else {
		ConfigProfile::Production
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConfigKey {
	OracleWindowSize,
	QueryRowBatchSize,
	QueryBytes,
	RetentionEvictInterval,
	RetentionEvictBatchSize,
	RetentionEvictMaxBatchesPerTick,
	EpochBucketInterval,
	RetentionStartupGrace,
	MaxRetentionHorizonFloor,
	HistoricalGcBatchSize,
	HistoricalGcInterval,
	MultiPointBufferTotalBytes,
	MultiRangeBufferTotalBytes,
	OperatorRangeTierBytes,
	MultiPointBufferShards,
	MultiRangeBufferShards,
	MultiFlushInterval,
	MultiFlushSliceBytes,
	MultiWalAutocheckpoint,
	OperatorResidentBytes,
	OperatorDirtyBytes,
	OperatorFlushSliceBytes,
	OperatorFlushInterval,
	OperatorWalAutocheckpoint,
	FlowTick,
	FlowSampleInterval,
	FlowWakeCoalesce,
	FlowFullWakeInterval,
	FlowActorBatchSize,
	FlowCommitterShards,
	CommitQueueBytes,
	FlowPullBatchBytes,
	FlowLoadBatchBytes,
	SubscriptionLoadBatchBytes,
	SubscriptionWaitTimeout,
	FlowJoinProbeBlockSize,
	ThreadsAsync,
	ThreadsCoordination,
	ThreadsFlow,
	ThreadsTask,
	ThreadsCompute,
	ThreadsMaintenance,
	SubscriptionWorkerThreads,
	MetricsFlushInterval,
	MetricsSampleInterval,
	MetricsSnapshotInterval,
	QueueLeaseReapInterval,
	QueueLeaseReapBatchSize,
	QueueRetentionInterval,
	QueueRetentionBatchSize,
	CommitMergeInterval,
}

impl ConfigKey {
	pub fn all() -> &'static [Self] {
		&[
			Self::OracleWindowSize,
			Self::QueryRowBatchSize,
			Self::QueryBytes,
			Self::RetentionEvictInterval,
			Self::RetentionEvictBatchSize,
			Self::RetentionEvictMaxBatchesPerTick,
			Self::EpochBucketInterval,
			Self::RetentionStartupGrace,
			Self::MaxRetentionHorizonFloor,
			Self::HistoricalGcBatchSize,
			Self::HistoricalGcInterval,
			Self::MultiPointBufferTotalBytes,
			Self::MultiRangeBufferTotalBytes,
			Self::OperatorRangeTierBytes,
			Self::MultiPointBufferShards,
			Self::MultiRangeBufferShards,
			Self::MultiFlushInterval,
			Self::MultiFlushSliceBytes,
			Self::MultiWalAutocheckpoint,
			Self::OperatorResidentBytes,
			Self::OperatorDirtyBytes,
			Self::OperatorFlushSliceBytes,
			Self::OperatorFlushInterval,
			Self::OperatorWalAutocheckpoint,
			Self::FlowTick,
			Self::FlowSampleInterval,
			Self::FlowWakeCoalesce,
			Self::FlowFullWakeInterval,
			Self::FlowActorBatchSize,
			Self::FlowCommitterShards,
			Self::CommitQueueBytes,
			Self::FlowPullBatchBytes,
			Self::FlowLoadBatchBytes,
			Self::SubscriptionLoadBatchBytes,
			Self::SubscriptionWaitTimeout,
			Self::FlowJoinProbeBlockSize,
			Self::ThreadsAsync,
			Self::ThreadsCoordination,
			Self::ThreadsFlow,
			Self::ThreadsTask,
			Self::ThreadsCompute,
			Self::ThreadsMaintenance,
			Self::SubscriptionWorkerThreads,
			Self::MetricsFlushInterval,
			Self::MetricsSampleInterval,
			Self::MetricsSnapshotInterval,
			Self::QueueLeaseReapInterval,
			Self::QueueLeaseReapBatchSize,
			Self::QueueRetentionInterval,
			Self::QueueRetentionBatchSize,
			Self::CommitMergeInterval,
		]
	}

	fn duration_or_none(duration: Option<Duration>) -> Value {
		match duration {
			Some(duration) => Value::Duration(duration),
			None => Value::None {
				inner: ValueType::Duration,
			},
		}
	}

	pub fn default_value(&self) -> Value {
		match active_config_profile() {
			ConfigProfile::Production => self.production_value(),
			ConfigProfile::Testing => self.testing_value(),
		}
	}

	pub fn production_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(default::query::ORACLE_WINDOW_SIZE),
			Self::QueryRowBatchSize => Value::Uint2(default::query::ROW_BATCH_SIZE),
			Self::QueryBytes => Value::Uint8(default::query::MEMORY_LIMIT.as_bytes()),
			Self::RetentionEvictInterval => Value::Duration(default::retention::EVICT_INTERVAL),
			Self::RetentionEvictBatchSize => Value::Uint8(default::retention::EVICT_BATCH_SIZE),
			Self::RetentionEvictMaxBatchesPerTick => {
				Value::Uint8(default::retention::EVICT_MAX_BATCHES_PER_TICK)
			}
			Self::EpochBucketInterval => Value::Duration(default::retention::EPOCH_BUCKET_INTERVAL),
			Self::RetentionStartupGrace => Value::Duration(default::retention::STARTUP_GRACE),
			Self::MaxRetentionHorizonFloor => Value::Duration(default::retention::MAX_HORIZON_FLOOR),
			Self::HistoricalGcBatchSize => Value::Uint8(default::retention::HISTORICAL_GC_BATCH_SIZE),
			Self::HistoricalGcInterval => Value::Duration(default::retention::HISTORICAL_GC_INTERVAL),
			Self::MultiPointBufferTotalBytes => {
				Value::Uint8(default::store::MULTI_POINT_BUFFER_TOTAL.as_bytes())
			}
			Self::MultiRangeBufferTotalBytes => {
				Value::Uint8(default::store::MULTI_RANGE_BUFFER_TOTAL.as_bytes())
			}
			Self::OperatorRangeTierBytes => Value::Uint8(default::store::OPERATOR_RANGE_TIER.as_bytes()),
			Self::MultiPointBufferShards => Value::Uint2(default::store::MULTI_POINT_BUFFER_SHARDS),
			Self::MultiRangeBufferShards => Value::Uint2(default::store::MULTI_RANGE_BUFFER_SHARDS),
			Self::MultiFlushInterval => Value::Duration(default::store::MULTI_FLUSH_INTERVAL),
			Self::MultiFlushSliceBytes => Value::Uint8(default::store::MULTI_FLUSH_BUDGET.as_bytes()),
			Self::MultiWalAutocheckpoint => Value::Uint8(default::store::MULTI_WAL_AUTOCHECKPOINT_PAGES),
			Self::OperatorResidentBytes => {
				Value::Uint8(default::store::OPERATOR_RESIDENT_BUDGET.as_bytes())
			}
			Self::OperatorDirtyBytes => Value::Uint8(default::store::OPERATOR_DIRTY_BUDGET.as_bytes()),
			Self::OperatorFlushSliceBytes => Value::Uint8(default::store::OPERATOR_FLUSH_SLICE.as_bytes()),
			Self::OperatorFlushInterval => Value::Duration(default::store::OPERATOR_FLUSH_INTERVAL),
			Self::OperatorWalAutocheckpoint => {
				Value::Uint8(default::store::OPERATOR_WAL_AUTOCHECKPOINT_PAGES)
			}
			Self::FlowTick => Value::Duration(default::flow::TICK),
			Self::FlowSampleInterval => Value::Duration(default::flow::SAMPLE_INTERVAL),
			Self::FlowWakeCoalesce => Value::Duration(default::flow::WAKE_COALESCE),
			Self::FlowFullWakeInterval => Value::Duration(default::flow::FULL_WAKE_INTERVAL),
			Self::FlowActorBatchSize => Value::Uint2(default::flow::ACTOR_BATCH_SIZE),
			Self::FlowCommitterShards => Value::Uint2(default::flow::COMMITTER_SHARDS),
			Self::CommitQueueBytes => Value::Uint8(default::commit::QUEUE_MEMORY_LIMIT.as_bytes()),
			Self::FlowPullBatchBytes => Value::Uint8(default::flow::PULL_BATCH.as_bytes()),
			Self::FlowLoadBatchBytes => Value::Uint8(default::flow::LOAD_BATCH.as_bytes()),
			Self::SubscriptionLoadBatchBytes => Value::Uint8(default::subscription::LOAD_BATCH.as_bytes()),
			Self::SubscriptionWaitTimeout => Value::Duration(default::subscription::WAIT_TIMEOUT),
			Self::FlowJoinProbeBlockSize => Value::Uint8(default::flow::JOIN_PROBE_BLOCK_SIZE),
			Self::ThreadsAsync => Value::Uint2(default::threads::ASYNC),
			Self::ThreadsCoordination => Value::Uint2(default::threads::COORDINATION),
			Self::ThreadsFlow => Value::Uint2(default::threads::FLOW),
			Self::ThreadsTask => Value::Uint2(default::threads::TASK),
			Self::ThreadsCompute => Value::Uint2(default::threads::COMPUTE),
			Self::ThreadsMaintenance => Value::Uint2(default::threads::MAINTENANCE),
			Self::SubscriptionWorkerThreads => Value::Uint2(default::threads::SUBSCRIPTION_WORKER),
			Self::MetricsFlushInterval => Value::Duration(default::metrics::FLUSH_INTERVAL),
			Self::MetricsSampleInterval => Value::Duration(default::metrics::SAMPLE_INTERVAL),
			Self::MetricsSnapshotInterval => Self::duration_or_none(default::metrics::SNAPSHOT_INTERVAL),
			Self::QueueLeaseReapInterval => Value::Duration(default::queue::LEASE_REAP_INTERVAL),
			Self::QueueLeaseReapBatchSize => Value::Uint8(default::queue::LEASE_REAP_BATCH_SIZE),
			Self::QueueRetentionInterval => Value::Duration(default::queue::RETENTION_INTERVAL),
			Self::QueueRetentionBatchSize => Value::Uint8(default::queue::RETENTION_BATCH_SIZE),
			Self::CommitMergeInterval => Value::Duration(default::store::COMMIT_MERGE_INTERVAL),
		}
	}

	pub fn testing_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(default::query::ORACLE_WINDOW_SIZE_TESTING),
			Self::QueryRowBatchSize => Value::Uint2(default::query::ROW_BATCH_SIZE_TESTING),
			Self::QueryBytes => Value::Uint8(default::query::MEMORY_LIMIT_TESTING.as_bytes()),
			Self::RetentionEvictInterval => Value::Duration(default::retention::EVICT_INTERVAL_TESTING),
			Self::RetentionEvictBatchSize => Value::Uint8(default::retention::EVICT_BATCH_SIZE_TESTING),
			Self::RetentionEvictMaxBatchesPerTick => {
				Value::Uint8(default::retention::EVICT_MAX_BATCHES_PER_TICK_TESTING)
			}
			Self::EpochBucketInterval => Value::Duration(default::retention::EPOCH_BUCKET_INTERVAL_TESTING),
			Self::RetentionStartupGrace => Value::Duration(default::retention::STARTUP_GRACE_TESTING),
			Self::MaxRetentionHorizonFloor => {
				Value::Duration(default::retention::MAX_HORIZON_FLOOR_TESTING)
			}
			Self::HistoricalGcBatchSize => {
				Value::Uint8(default::retention::HISTORICAL_GC_BATCH_SIZE_TESTING)
			}
			Self::HistoricalGcInterval => {
				Value::Duration(default::retention::HISTORICAL_GC_INTERVAL_TESTING)
			}
			Self::MultiPointBufferTotalBytes => {
				Value::Uint8(default::store::MULTI_POINT_BUFFER_TOTAL_TESTING.as_bytes())
			}
			Self::MultiRangeBufferTotalBytes => {
				Value::Uint8(default::store::MULTI_RANGE_BUFFER_TOTAL_TESTING.as_bytes())
			}
			Self::OperatorRangeTierBytes => {
				Value::Uint8(default::store::OPERATOR_RANGE_TIER_TESTING.as_bytes())
			}
			Self::MultiPointBufferShards => Value::Uint2(default::store::MULTI_POINT_BUFFER_SHARDS_TESTING),
			Self::MultiRangeBufferShards => Value::Uint2(default::store::MULTI_RANGE_BUFFER_SHARDS_TESTING),
			Self::MultiFlushInterval => Value::Duration(default::store::MULTI_FLUSH_INTERVAL_TESTING),
			Self::MultiFlushSliceBytes => {
				Value::Uint8(default::store::MULTI_FLUSH_BUDGET_TESTING.as_bytes())
			}
			Self::MultiWalAutocheckpoint => {
				Value::Uint8(default::store::MULTI_WAL_AUTOCHECKPOINT_PAGES_TESTING)
			}
			Self::OperatorResidentBytes => {
				Value::Uint8(default::store::OPERATOR_RESIDENT_BUDGET_TESTING.as_bytes())
			}
			Self::OperatorDirtyBytes => {
				Value::Uint8(default::store::OPERATOR_DIRTY_BUDGET_TESTING.as_bytes())
			}
			Self::OperatorFlushSliceBytes => {
				Value::Uint8(default::store::OPERATOR_FLUSH_SLICE_TESTING.as_bytes())
			}
			Self::OperatorFlushInterval => Value::Duration(default::store::OPERATOR_FLUSH_INTERVAL_TESTING),
			Self::OperatorWalAutocheckpoint => {
				Value::Uint8(default::store::OPERATOR_WAL_AUTOCHECKPOINT_PAGES_TESTING)
			}
			Self::FlowTick => Value::Duration(default::flow::TICK_TESTING),
			Self::FlowSampleInterval => Value::Duration(default::flow::SAMPLE_INTERVAL_TESTING),
			Self::FlowWakeCoalesce => Value::Duration(default::flow::WAKE_COALESCE_TESTING),
			Self::FlowFullWakeInterval => Value::Duration(default::flow::FULL_WAKE_INTERVAL_TESTING),
			Self::FlowActorBatchSize => Value::Uint2(default::flow::ACTOR_BATCH_SIZE_TESTING),
			Self::FlowCommitterShards => Value::Uint2(default::flow::COMMITTER_SHARDS_TESTING),
			Self::CommitQueueBytes => Value::Uint8(default::commit::QUEUE_MEMORY_LIMIT_TESTING.as_bytes()),
			Self::FlowPullBatchBytes => Value::Uint8(default::flow::PULL_BATCH_TESTING.as_bytes()),
			Self::FlowLoadBatchBytes => Value::Uint8(default::flow::LOAD_BATCH_TESTING.as_bytes()),
			Self::SubscriptionLoadBatchBytes => {
				Value::Uint8(default::subscription::LOAD_BATCH_TESTING.as_bytes())
			}
			Self::SubscriptionWaitTimeout => Value::Duration(default::subscription::WAIT_TIMEOUT_TESTING),
			Self::FlowJoinProbeBlockSize => Value::Uint8(default::flow::JOIN_PROBE_BLOCK_SIZE_TESTING),
			Self::ThreadsAsync => Value::Uint2(default::threads::ASYNC_TESTING),
			Self::ThreadsCoordination => Value::Uint2(default::threads::COORDINATION_TESTING),
			Self::ThreadsFlow => Value::Uint2(default::threads::FLOW_TESTING),
			Self::ThreadsTask => Value::Uint2(default::threads::TASK_TESTING),
			Self::ThreadsCompute => Value::Uint2(default::threads::COMPUTE_TESTING),
			Self::ThreadsMaintenance => Value::Uint2(default::threads::MAINTENANCE_TESTING),
			Self::SubscriptionWorkerThreads => Value::Uint2(default::threads::SUBSCRIPTION_WORKER_TESTING),
			Self::MetricsFlushInterval => Value::Duration(default::metrics::FLUSH_INTERVAL_TESTING),
			Self::MetricsSampleInterval => Value::Duration(default::metrics::SAMPLE_INTERVAL_TESTING),
			Self::MetricsSnapshotInterval => {
				Self::duration_or_none(default::metrics::SNAPSHOT_INTERVAL_TESTING)
			}
			Self::QueueLeaseReapInterval => Value::Duration(default::queue::LEASE_REAP_INTERVAL_TESTING),
			Self::QueueLeaseReapBatchSize => Value::Uint8(default::queue::LEASE_REAP_BATCH_SIZE_TESTING),
			Self::QueueRetentionInterval => Value::Duration(default::queue::RETENTION_INTERVAL_TESTING),
			Self::QueueRetentionBatchSize => Value::Uint8(default::queue::RETENTION_BATCH_SIZE_TESTING),
			Self::CommitMergeInterval => Value::Duration(default::store::COMMIT_MERGE_INTERVAL_TESTING),
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::QueryRowBatchSize => {
				"Number of rows produced per batch by query / DML pipeline operators."
			}
			Self::QueryBytes => {
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
			Self::MultiPointBufferTotalBytes => {
				"Total resident byte budget of the multi-version point cache, split evenly across \
				 MULTI_POINT_BUFFER_SHARDS. Must be at least the shard count. None disables the cache \
				 outright, so every point read that misses the commit buffer goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiRangeBufferTotalBytes => {
				"Total resident byte budget of the multi-version range cache, split evenly across \
				 MULTI_RANGE_BUFFER_SHARDS. Must be at least the shard count. None disables the cache \
				 outright, so every multi-version range scan goes to the persistent tier. Read once at boot; changing it \
				 requires a restart."
			}
			Self::OperatorRangeTierBytes => {
				"Resident byte budget for one tier of the operator-state range cache. Every cached keyspace \
				 carries its own tier, so total cache memory is this value times the number of cached \
				 keyspaces. None disables the cache outright, so every operator range scan goes to the \
				 persistent tier. Read once at boot; changing it requires a restart."
			}
			Self::MultiPointBufferShards => {
				"Number of lock-striped shards the multi-version point cache total budget is split across. \
				 Raising it divides the same memory into more, smaller shards. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiRangeBufferShards => {
				"Number of lock-striped shards the multi-version range cache total budget is split across. \
				 Raising it divides the same memory into more, smaller shards. Must be >= 1. Read once at boot; changing it \
				 requires a restart."
			}
			Self::MultiFlushInterval => {
				"How often the persistent-flush actor drains the in-memory commit buffer into the multi \
				 store's SQLite tier. Longer intervals coalesce more writes per flush - a larger WAL - at \
				 the cost of more resident commit-buffer memory and a longer window before data is \
				 materialized in the persistent file. Read once at boot; changing it requires a restart."
			}
			Self::MultiFlushSliceBytes => {
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
			Self::OperatorResidentBytes => {
				"Byte ceiling on resident operator state. Eviction returns clean state to this limit \
				 once it is exceeded. Dirty state is never evicted, so a tier holding mostly unwritten \
				 state stays above the limit until a flush turns it clean; OPERATOR_DIRTY_BYTES bounds \
				 that, and OPERATOR_FLUSH_SLICE_BYTES sizes the individual commits a flush writes. No single \
				 operator may hold more than this divided by min(live operators, 16). Read once at boot; \
				 changing it requires a restart."
			}
			Self::OperatorDirtyBytes => {
				"Byte ceiling on unwritten operator state. A flush is triggered once dirty resident state \
				 exceeds this, which bounds both how much memory dirty state may hold and how much one \
				 drain has to write. It defaults to the resident budget, so the flush interval is the \
				 normal trigger and this stays a backstop for a workload that dirties state faster than \
				 the interval anticipates. Read once at boot; changing it requires a restart."
			}
			Self::OperatorFlushSliceBytes => {
				"Byte target for a single operator-state flush transaction. The drain stops taking work at \
				 the first group boundary past this value, so one commit can exceed it by the size of that \
				 group. Larger values mean fewer and longer commits, and every operator-state write blocks \
				 for the length of a commit. Read once at boot; changing it requires a restart."
			}
			Self::OperatorFlushInterval => {
				"How often the operator-state flush actor drains dirty resident state into the operator \
				 store's SQLite tier. Operator state stays resident after a flush and is freed \
				 separately by eviction, so memory pressure alone can leave state unflushed \
				 indefinitely, which holds the durable checkpoint back and with it the commit log floor. \
				 Read once at boot; changing it requires a restart."
			}
			Self::OperatorWalAutocheckpoint => {
				"WAL frame threshold for the operator store's SQLite tier: sets the SQLite \
				 wal_autocheckpoint PRAGMA that governs when SQLite folds the WAL back into the main \
				 database. Higher values checkpoint less often with a larger WAL, reducing checkpoint \
				 I/O; lower values keep the WAL small at the cost of more frequent checkpoints. Read \
				 once at boot; changing it requires a restart."
			}
			Self::FlowTick => "How often each deferred flow actor wakes up to dispatch due flows.",
			Self::FlowSampleInterval => {
				"How often each flow actor samples its operators' approximate memory into the \
				 system::metrics::runtime::memory samples (scope operator::N). Runs on the operator's \
				 own thread, off the apply path. When none, operator sampling is disabled entirely; when \
				 set, must be > 0."
			}
			Self::FlowWakeCoalesce => {
				"How long a flow that folds its input waits after one wake before it takes the next, so upstream \
				 steps inside the window merge into one drain. Zero wakes on every step. Read once at boot; \
				 changing it requires a restart."
			}
			Self::FlowFullWakeInterval => {
				"How often the flow supervisor wakes every flow after a commit, so a flow with nothing to do moves \
				 its position forward. Read once at boot; changing it requires a restart."
			}
			Self::FlowActorBatchSize => {
				"How many messages a flow actor handles per turn on its worker thread before yielding to the \
				 other actors pinned to that thread. Smaller values let a woken flow run sooner. Must be >= 1. \
				 Read once at boot; changing it requires a restart."
			}
			Self::FlowCommitterShards => {
				"How many committer actors share the flow commits. Each flow always commits through the same \
				 shard, so its own commits stay in order while different flows commit side by side. Must be \
				 >= 1. Read once at boot; changing it requires a restart."
			}
			Self::CommitQueueBytes => {
				"Byte ceiling of the in-memory queue of released commits that feeds flow readers. With the \
				 commit log on, the oldest commits are evicted once the ceiling is exceeded and a reader that \
				 far behind reads them back from the commit log. With the commit log off, nothing above the \
				 slowest reader is evicted, so the ceiling is soft. Read once at boot; changing it requires a \
				 restart."
			}
			Self::FlowPullBatchBytes => {
				"Byte budget a reader applies per pull from the commit queue. A reader that has fallen \
				 behind receives up to this many bytes of decoded changes in one slice, so catch-up is \
				 vectorized instead of per-version."
			}
			Self::FlowLoadBatchBytes => {
				"Byte budget of one catch-up loader read from the commit log on behalf of flows that are \
				 behind the in-memory commit queue. Identical concurrent requests share a single read."
			}
			Self::SubscriptionLoadBatchBytes => {
				"Byte budget of one catch-up loader read from the commit log on behalf of the subscription \
				 reader when it is behind the in-memory commit queue. A read always returns at least one \
				 commit, so a commit larger than this budget still loads."
			}
			Self::SubscriptionWaitTimeout => {
				"Backstop timeout for the subscription reader's wait for a dispatch reply from the \
				 subscription workers. A lost reply would otherwise wedge the reader forever; on timeout the \
				 process aborts with a report naming the consumer, the pending work and the batch. \
				 Must be > 0."
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
			Self::ThreadsMaintenance => {
				"Number of worker threads for the maintenance actor pool (lifecycle tasks, operator range \
				 eviction, filter rebuilds). A long slice on one actor holds a thread, so a count of 1 lets \
				 the slowest task delay every other one. Must be >= 1. Changes require restart."
			}
			Self::SubscriptionWorkerThreads => {
				"Number of subscription worker actors that fan out committed changes to ephemeral \
				 subscriptions in parallel. 0 means auto (size to the system thread pool). Higher values \
				 raise fan-out parallelism for many concurrent subscriptions. Changes require restart."
			}
			Self::MetricsFlushInterval => {
				"How often the metric collector flushes accumulated storage accounting into the \
				 system::metrics KV store that backs the storage views. Must be > 0."
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
			Self::QueueLeaseReapInterval => {
				"How often the queue reaper scans for leases whose deadline has passed. A dead worker's item cannot be redelivered sooner than this, so it should stay well below any declared lease ttl."
			}
			Self::QueueLeaseReapBatchSize => {
				"Max queue item-state records one reap slice may scan. Bounds the slice on a deep backlog; the scan resumes from its cursor on the next slice."
			}
			Self::CommitMergeInterval => {
				"How often the commit-merge task merges closed commit-buffer maps off the commit path. Commits merge inline only when the mergeable backlog outgrows the commit store's merge backlog."
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
			Self::QueryBytes => false,
			Self::RetentionEvictInterval => true,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::EpochBucketInterval => true,
			Self::RetentionStartupGrace => false,
			Self::MaxRetentionHorizonFloor => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => true,
			Self::MultiPointBufferTotalBytes => true,
			Self::MultiRangeBufferTotalBytes => true,
			Self::OperatorRangeTierBytes => true,
			Self::MultiPointBufferShards => true,
			Self::MultiRangeBufferShards => true,
			Self::MultiFlushInterval => true,
			Self::MultiFlushSliceBytes => false,
			Self::MultiWalAutocheckpoint => true,
			Self::OperatorResidentBytes => true,
			Self::OperatorDirtyBytes => true,
			Self::OperatorFlushSliceBytes => true,
			Self::OperatorFlushInterval => true,
			Self::OperatorWalAutocheckpoint => true,
			Self::FlowTick => false,
			Self::FlowSampleInterval => false,
			Self::FlowWakeCoalesce => true,
			Self::FlowFullWakeInterval => true,
			Self::FlowActorBatchSize => true,
			Self::FlowCommitterShards => true,
			Self::CommitQueueBytes => true,
			Self::FlowPullBatchBytes => true,
			Self::FlowLoadBatchBytes => true,
			Self::SubscriptionLoadBatchBytes => true,
			Self::SubscriptionWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => true,
			Self::ThreadsCoordination => true,
			Self::ThreadsFlow => true,
			Self::ThreadsTask => true,
			Self::ThreadsCompute => true,
			Self::ThreadsMaintenance => true,
			Self::SubscriptionWorkerThreads => true,
			Self::MetricsFlushInterval => false,
			Self::MetricsSampleInterval => true,
			Self::MetricsSnapshotInterval => true,
			Self::QueueLeaseReapInterval => false,
			Self::QueueLeaseReapBatchSize => false,
			Self::QueueRetentionInterval => false,
			Self::QueueRetentionBatchSize => false,
			Self::CommitMergeInterval => true,
		}
	}

	pub fn expected_types(&self) -> &'static [ValueType] {
		match self {
			Self::OracleWindowSize => &[ValueType::Uint8],
			Self::QueryRowBatchSize => &[ValueType::Uint2],
			Self::QueryBytes => &[ValueType::Uint8],
			Self::RetentionEvictInterval => &[ValueType::Duration],
			Self::RetentionEvictBatchSize => &[ValueType::Uint8],
			Self::RetentionEvictMaxBatchesPerTick => &[ValueType::Uint8],
			Self::EpochBucketInterval => &[ValueType::Duration],
			Self::RetentionStartupGrace => &[ValueType::Duration],
			Self::MaxRetentionHorizonFloor => &[ValueType::Duration],
			Self::HistoricalGcBatchSize => &[ValueType::Uint8],
			Self::HistoricalGcInterval => &[ValueType::Duration],
			Self::MultiPointBufferTotalBytes => &[ValueType::Uint8],
			Self::MultiRangeBufferTotalBytes => &[ValueType::Uint8],
			Self::OperatorRangeTierBytes => &[ValueType::Uint8],
			Self::MultiPointBufferShards => &[ValueType::Uint2],
			Self::MultiRangeBufferShards => &[ValueType::Uint2],
			Self::MultiFlushInterval => &[ValueType::Duration],
			Self::MultiFlushSliceBytes => &[ValueType::Uint8],
			Self::MultiWalAutocheckpoint => &[ValueType::Uint8],
			Self::OperatorResidentBytes => &[ValueType::Uint8],
			Self::OperatorDirtyBytes => &[ValueType::Uint8],
			Self::OperatorFlushSliceBytes => &[ValueType::Uint8],
			Self::OperatorFlushInterval => &[ValueType::Duration],
			Self::OperatorWalAutocheckpoint => &[ValueType::Uint8],
			Self::FlowTick => &[ValueType::Duration],
			Self::FlowSampleInterval => &[ValueType::Duration],
			Self::FlowWakeCoalesce => &[ValueType::Duration],
			Self::FlowFullWakeInterval => &[ValueType::Duration],
			Self::FlowActorBatchSize => &[ValueType::Uint2],
			Self::FlowCommitterShards => &[ValueType::Uint2],
			Self::CommitQueueBytes => &[ValueType::Uint8],
			Self::FlowPullBatchBytes => &[ValueType::Uint8],
			Self::FlowLoadBatchBytes => &[ValueType::Uint8],
			Self::SubscriptionLoadBatchBytes => &[ValueType::Uint8],
			Self::SubscriptionWaitTimeout => &[ValueType::Duration],
			Self::FlowJoinProbeBlockSize => &[ValueType::Uint8],
			Self::ThreadsAsync => &[ValueType::Uint2],
			Self::ThreadsCoordination => &[ValueType::Uint2],
			Self::ThreadsFlow => &[ValueType::Uint2],
			Self::ThreadsTask => &[ValueType::Uint2],
			Self::ThreadsCompute => &[ValueType::Uint2],
			Self::ThreadsMaintenance => &[ValueType::Uint2],
			Self::SubscriptionWorkerThreads => &[ValueType::Uint2],
			Self::MetricsFlushInterval => &[ValueType::Duration],
			Self::MetricsSampleInterval => &[ValueType::Duration],
			Self::MetricsSnapshotInterval => &[ValueType::Duration],
			Self::QueueLeaseReapInterval => &[ValueType::Duration],
			Self::QueueLeaseReapBatchSize => &[ValueType::Uint8],
			Self::QueueRetentionInterval => &[ValueType::Duration],
			Self::QueueRetentionBatchSize => &[ValueType::Uint8],
			Self::CommitMergeInterval => &[ValueType::Duration],
		}
	}

	pub fn is_optional(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::QueryRowBatchSize => false,
			Self::QueryBytes => false,
			Self::RetentionEvictInterval => false,
			Self::RetentionEvictBatchSize => false,
			Self::RetentionEvictMaxBatchesPerTick => false,
			Self::EpochBucketInterval => false,
			Self::RetentionStartupGrace => false,
			Self::MaxRetentionHorizonFloor => false,
			Self::HistoricalGcBatchSize => false,
			Self::HistoricalGcInterval => false,
			Self::MultiPointBufferTotalBytes => true,
			Self::MultiRangeBufferTotalBytes => true,
			Self::OperatorRangeTierBytes => true,
			Self::MultiPointBufferShards => false,
			Self::MultiRangeBufferShards => false,
			Self::MultiFlushInterval => false,
			Self::MultiFlushSliceBytes => false,
			Self::MultiWalAutocheckpoint => false,
			Self::OperatorResidentBytes => false,
			Self::OperatorDirtyBytes => false,
			Self::OperatorFlushSliceBytes => false,
			Self::OperatorFlushInterval => false,
			Self::OperatorWalAutocheckpoint => false,
			Self::FlowTick => false,
			Self::FlowSampleInterval => true,
			Self::FlowWakeCoalesce => false,
			Self::FlowFullWakeInterval => false,
			Self::FlowActorBatchSize => false,
			Self::FlowCommitterShards => false,
			Self::CommitQueueBytes => false,
			Self::FlowPullBatchBytes => false,
			Self::FlowLoadBatchBytes => false,
			Self::SubscriptionLoadBatchBytes => false,
			Self::SubscriptionWaitTimeout => false,
			Self::FlowJoinProbeBlockSize => false,
			Self::ThreadsAsync => false,
			Self::ThreadsCoordination => false,
			Self::ThreadsFlow => false,
			Self::ThreadsTask => false,
			Self::ThreadsCompute => false,
			Self::ThreadsMaintenance => false,
			Self::SubscriptionWorkerThreads => false,
			Self::MetricsFlushInterval => false,
			Self::MetricsSampleInterval => false,
			Self::MetricsSnapshotInterval => true,
			Self::QueueLeaseReapInterval => false,
			Self::QueueLeaseReapBatchSize => false,
			Self::QueueRetentionInterval => false,
			Self::QueueRetentionBatchSize => false,
			Self::CommitMergeInterval => false,
		}
	}

	fn validate_canonical(&self, value: &Value) -> Result<(), String> {
		match self {
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
			Self::QueryBytes => match value {
				Value::Uint8(0) => Err("QUERY_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CommitQueueBytes => match value {
				Value::Uint8(0) => {
					Err("COMMIT_QUEUE_BYTES must be greater than zero".to_string())
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
			Self::SubscriptionLoadBatchBytes => match value {
				Value::Uint8(0) => Err("SUBSCRIPTION_LOAD_BATCH_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiPointBufferTotalBytes => match value {
				Value::Uint8(0) => Err(
					"MULTI_POINT_BUFFER_TOTAL_BYTES must be greater than zero; use none to disable the point cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::MultiRangeBufferTotalBytes => match value {
				Value::Uint8(0) => Err(
					"MULTI_RANGE_BUFFER_TOTAL_BYTES must be greater than zero; use none to disable the range cache"
						.to_string(),
				),
				_ => Ok(()),
			},
			Self::OperatorRangeTierBytes => match value {
				Value::Uint8(0) => Err(
					"OPERATOR_RANGE_TIER_BYTES must be greater than zero; use none to disable the range cache"
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
			Self::MultiFlushInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("MULTI_FLUSH_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::CommitMergeInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("COMMIT_MERGE_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiFlushSliceBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => Err("MULTI_FLUSH_SLICE_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::MultiWalAutocheckpoint => match value {
				Value::Uint8(0) => {
					Err("MULTI_WAL_AUTOCHECKPOINT must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::OperatorResidentBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => {
					Err("OPERATOR_RESIDENT_BYTES must be greater than zero".to_string())
				}
				_ => Ok(()),
			},
			Self::OperatorDirtyBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => Err("OPERATOR_DIRTY_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorFlushSliceBytes => match value {
				Value::Uint8(n) if *n > 0 => Ok(()),
				Value::Uint8(_) => Err("OPERATOR_FLUSH_SLICE_BYTES must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorFlushInterval => match value {
				Value::Duration(d) if d.is_positive() => Ok(()),
				Value::Duration(_) => Err("OPERATOR_FLUSH_INTERVAL must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::OperatorWalAutocheckpoint => match value {
				Value::Uint8(0) => {
					Err("OPERATOR_WAL_AUTOCHECKPOINT must be greater than zero".to_string())
				}
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
			Self::FlowWakeCoalesce => match value {
				Value::Duration(d) if d.is_negative() => Err("FLOW_WAKE_COALESCE must not be negative".to_string()),
				_ => Ok(()),
			},
			Self::FlowFullWakeInterval => match value {
				Value::Duration(d) if d.is_negative() || d.is_zero() => {
					Err("FLOW_FULL_WAKE_INTERVAL must be positive".to_string())
				}
				_ => Ok(()),
			},
			Self::FlowActorBatchSize => match value {
				Value::Uint2(0) => Err("FLOW_ACTOR_BATCH_SIZE must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::FlowCommitterShards => match value {
				Value::Uint2(0) => Err("FLOW_COMMITTER_SHARDS must be greater than zero".to_string()),
				_ => Ok(()),
			},
			Self::SubscriptionWaitTimeout => match value {
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("SUBSCRIPTION_WAIT_TIMEOUT must be greater than zero".to_string())
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
			Self::ThreadsMaintenance => match value {
				Value::Uint2(0) => Err("THREADS_MAINTENANCE must be greater than zero".to_string()),
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
			Self::QueryBytes => write!(f, "QUERY_BYTES"),
			Self::RetentionEvictInterval => write!(f, "RETENTION_EVICT_INTERVAL"),
			Self::RetentionEvictBatchSize => write!(f, "RETENTION_EVICT_BATCH_SIZE"),
			Self::RetentionEvictMaxBatchesPerTick => write!(f, "RETENTION_EVICT_MAX_BATCHES_PER_TICK"),
			Self::EpochBucketInterval => write!(f, "EPOCH_BUCKET_INTERVAL"),
			Self::RetentionStartupGrace => write!(f, "RETENTION_STARTUP_GRACE"),
			Self::MaxRetentionHorizonFloor => write!(f, "MAX_RETENTION_HORIZON_FLOOR"),
			Self::HistoricalGcBatchSize => write!(f, "HISTORICAL_GC_BATCH_SIZE"),
			Self::HistoricalGcInterval => write!(f, "HISTORICAL_GC_INTERVAL"),
			Self::MultiPointBufferTotalBytes => write!(f, "MULTI_POINT_BUFFER_TOTAL_BYTES"),
			Self::MultiRangeBufferTotalBytes => write!(f, "MULTI_RANGE_BUFFER_TOTAL_BYTES"),
			Self::OperatorRangeTierBytes => write!(f, "OPERATOR_RANGE_TIER_BYTES"),
			Self::MultiPointBufferShards => write!(f, "MULTI_POINT_BUFFER_SHARDS"),
			Self::MultiRangeBufferShards => write!(f, "MULTI_RANGE_BUFFER_SHARDS"),
			Self::MultiFlushInterval => write!(f, "MULTI_FLUSH_INTERVAL"),
			Self::MultiFlushSliceBytes => write!(f, "MULTI_FLUSH_SLICE_BYTES"),
			Self::MultiWalAutocheckpoint => write!(f, "MULTI_WAL_AUTOCHECKPOINT"),
			Self::OperatorResidentBytes => write!(f, "OPERATOR_RESIDENT_BYTES"),
			Self::OperatorDirtyBytes => write!(f, "OPERATOR_DIRTY_BYTES"),
			Self::OperatorFlushSliceBytes => write!(f, "OPERATOR_FLUSH_SLICE_BYTES"),
			Self::OperatorFlushInterval => write!(f, "OPERATOR_FLUSH_INTERVAL"),
			Self::OperatorWalAutocheckpoint => write!(f, "OPERATOR_WAL_AUTOCHECKPOINT"),
			Self::FlowTick => write!(f, "FLOW_TICK"),
			Self::FlowSampleInterval => write!(f, "FLOW_SAMPLE_INTERVAL"),
			Self::FlowWakeCoalesce => write!(f, "FLOW_WAKE_COALESCE"),
			Self::FlowFullWakeInterval => write!(f, "FLOW_FULL_WAKE_INTERVAL"),
			Self::FlowActorBatchSize => write!(f, "FLOW_ACTOR_BATCH_SIZE"),
			Self::FlowCommitterShards => write!(f, "FLOW_COMMITTER_SHARDS"),
			Self::CommitQueueBytes => write!(f, "COMMIT_QUEUE_BYTES"),
			Self::FlowPullBatchBytes => write!(f, "FLOW_PULL_BATCH_BYTES"),
			Self::FlowLoadBatchBytes => write!(f, "FLOW_LOAD_BATCH_BYTES"),
			Self::SubscriptionLoadBatchBytes => write!(f, "SUBSCRIPTION_LOAD_BATCH_BYTES"),
			Self::SubscriptionWaitTimeout => write!(f, "SUBSCRIPTION_WAIT_TIMEOUT"),
			Self::FlowJoinProbeBlockSize => write!(f, "FLOW_JOIN_PROBE_BLOCK_SIZE"),
			Self::ThreadsAsync => write!(f, "THREADS_ASYNC"),
			Self::ThreadsCoordination => write!(f, "THREADS_COORDINATION"),
			Self::ThreadsFlow => write!(f, "THREADS_FLOW"),
			Self::ThreadsTask => write!(f, "THREADS_TASK"),
			Self::ThreadsCompute => write!(f, "THREADS_COMPUTE"),
			Self::ThreadsMaintenance => write!(f, "THREADS_MAINTENANCE"),
			Self::SubscriptionWorkerThreads => write!(f, "SUBSCRIPTION_WORKER_THREADS"),
			Self::MetricsFlushInterval => write!(f, "METRICS_FLUSH_INTERVAL"),
			Self::MetricsSampleInterval => write!(f, "METRICS_SAMPLE_INTERVAL"),
			Self::MetricsSnapshotInterval => write!(f, "METRICS_SNAPSHOT_INTERVAL"),
			Self::QueueLeaseReapInterval => write!(f, "QUEUE_LEASE_REAP_INTERVAL"),
			Self::QueueLeaseReapBatchSize => write!(f, "QUEUE_LEASE_REAP_BATCH_SIZE"),
			Self::QueueRetentionInterval => write!(f, "QUEUE_RETENTION_INTERVAL"),
			Self::QueueRetentionBatchSize => write!(f, "QUEUE_RETENTION_BATCH_SIZE"),
			Self::CommitMergeInterval => write!(f, "COMMIT_MERGE_INTERVAL"),
		}
	}
}

impl FromStr for ConfigKey {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ORACLE_WINDOW_SIZE" => Ok(Self::OracleWindowSize),
			"QUERY_ROW_BATCH_SIZE" => Ok(Self::QueryRowBatchSize),
			"QUERY_BYTES" => Ok(Self::QueryBytes),
			"RETENTION_EVICT_INTERVAL" => Ok(Self::RetentionEvictInterval),
			"RETENTION_EVICT_BATCH_SIZE" => Ok(Self::RetentionEvictBatchSize),
			"RETENTION_EVICT_MAX_BATCHES_PER_TICK" => Ok(Self::RetentionEvictMaxBatchesPerTick),
			"EPOCH_BUCKET_INTERVAL" => Ok(Self::EpochBucketInterval),
			"RETENTION_STARTUP_GRACE" => Ok(Self::RetentionStartupGrace),
			"MAX_RETENTION_HORIZON_FLOOR" => Ok(Self::MaxRetentionHorizonFloor),
			"HISTORICAL_GC_BATCH_SIZE" => Ok(Self::HistoricalGcBatchSize),
			"HISTORICAL_GC_INTERVAL" => Ok(Self::HistoricalGcInterval),
			"MULTI_POINT_BUFFER_TOTAL_BYTES" => Ok(Self::MultiPointBufferTotalBytes),
			"MULTI_RANGE_BUFFER_TOTAL_BYTES" => Ok(Self::MultiRangeBufferTotalBytes),
			"OPERATOR_RANGE_TIER_BYTES" => Ok(Self::OperatorRangeTierBytes),
			"MULTI_POINT_BUFFER_SHARDS" => Ok(Self::MultiPointBufferShards),
			"MULTI_RANGE_BUFFER_SHARDS" => Ok(Self::MultiRangeBufferShards),
			"MULTI_FLUSH_INTERVAL" => Ok(Self::MultiFlushInterval),
			"MULTI_FLUSH_SLICE_BYTES" => Ok(Self::MultiFlushSliceBytes),
			"MULTI_WAL_AUTOCHECKPOINT" => Ok(Self::MultiWalAutocheckpoint),
			"OPERATOR_RESIDENT_BYTES" => Ok(Self::OperatorResidentBytes),
			"OPERATOR_DIRTY_BYTES" => Ok(Self::OperatorDirtyBytes),
			"OPERATOR_FLUSH_SLICE_BYTES" => Ok(Self::OperatorFlushSliceBytes),
			"OPERATOR_FLUSH_INTERVAL" => Ok(Self::OperatorFlushInterval),
			"OPERATOR_WAL_AUTOCHECKPOINT" => Ok(Self::OperatorWalAutocheckpoint),
			"FLOW_TICK" => Ok(Self::FlowTick),
			"FLOW_SAMPLE_INTERVAL" => Ok(Self::FlowSampleInterval),
			"FLOW_WAKE_COALESCE" => Ok(Self::FlowWakeCoalesce),
			"FLOW_FULL_WAKE_INTERVAL" => Ok(Self::FlowFullWakeInterval),
			"FLOW_ACTOR_BATCH_SIZE" => Ok(Self::FlowActorBatchSize),
			"FLOW_COMMITTER_SHARDS" => Ok(Self::FlowCommitterShards),
			"COMMIT_QUEUE_BYTES" => Ok(Self::CommitQueueBytes),
			"FLOW_PULL_BATCH_BYTES" => Ok(Self::FlowPullBatchBytes),
			"FLOW_LOAD_BATCH_BYTES" => Ok(Self::FlowLoadBatchBytes),
			"SUBSCRIPTION_LOAD_BATCH_BYTES" => Ok(Self::SubscriptionLoadBatchBytes),
			"SUBSCRIPTION_WAIT_TIMEOUT" => Ok(Self::SubscriptionWaitTimeout),
			"FLOW_JOIN_PROBE_BLOCK_SIZE" => Ok(Self::FlowJoinProbeBlockSize),
			"THREADS_ASYNC" => Ok(Self::ThreadsAsync),
			"THREADS_COORDINATION" => Ok(Self::ThreadsCoordination),
			"THREADS_FLOW" => Ok(Self::ThreadsFlow),
			"THREADS_TASK" => Ok(Self::ThreadsTask),
			"THREADS_COMPUTE" => Ok(Self::ThreadsCompute),
			"THREADS_MAINTENANCE" => Ok(Self::ThreadsMaintenance),
			"SUBSCRIPTION_WORKER_THREADS" => Ok(Self::SubscriptionWorkerThreads),
			"METRICS_FLUSH_INTERVAL" => Ok(Self::MetricsFlushInterval),
			"METRICS_SAMPLE_INTERVAL" => Ok(Self::MetricsSampleInterval),
			"METRICS_SNAPSHOT_INTERVAL" => Ok(Self::MetricsSnapshotInterval),
			"QUEUE_LEASE_REAP_INTERVAL" => Ok(Self::QueueLeaseReapInterval),
			"QUEUE_LEASE_REAP_BATCH_SIZE" => Ok(Self::QueueLeaseReapBatchSize),
			"QUEUE_RETENTION_INTERVAL" => Ok(Self::QueueRetentionInterval),
			"QUEUE_RETENTION_BATCH_SIZE" => Ok(Self::QueueRetentionBatchSize),
			"COMMIT_MERGE_INTERVAL" => Ok(Self::CommitMergeInterval),
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
	fn test_optional_duration_default_is_typed_null() {
		// Defaulting to Value::None means no snapshot cadence is configured; a zero default would roll every
		// sample.
		let default = ConfigKey::MetricsSnapshotInterval.default_value();
		assert!(matches!(
			default,
			Value::None {
				inner: ValueType::Duration
			}
		));
	}

	#[test]
	fn test_optional_duration_accept_passes_typed_null() {
		let none = Value::None {
			inner: ValueType::Duration,
		};
		let v = ConfigKey::MetricsSnapshotInterval.accept(none.clone()).unwrap();
		assert_eq!(v, none);
	}

	#[test]
	fn test_optional_duration_accept_passes_positive_duration() {
		let one_sec = Value::duration_seconds(1);
		assert_eq!(ConfigKey::MetricsSnapshotInterval.accept(one_sec.clone()).unwrap(), one_sec);

		let one_hour = Value::duration_seconds(3600);
		assert_eq!(ConfigKey::MetricsSnapshotInterval.accept(one_hour.clone()).unwrap(), one_hour);
	}

	#[test]
	fn test_optional_duration_accept_rejects_zero() {
		let zero = Value::duration_seconds(0);
		match ConfigKey::MetricsSnapshotInterval.accept(zero).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
	}

	#[test]
	fn test_optional_duration_accept_rejects_negative() {
		let negative = Value::duration_seconds(-5);
		assert!(matches!(
			ConfigKey::MetricsSnapshotInterval.accept(negative),
			Err(AcceptError::InvalidValue(_))
		));
	}

	#[test]
	fn test_other_keys_accept_in_type_values() {
		assert!(ConfigKey::OracleWindowSize.accept(Value::Uint8(0)).is_ok());
	}

	#[test]
	fn test_optional_duration_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "METRICS_SNAPSHOT_INTERVAL".parse().unwrap();
		assert_eq!(key, ConfigKey::MetricsSnapshotInterval);
		assert_eq!(format!("{}", ConfigKey::MetricsSnapshotInterval), "METRICS_SNAPSHOT_INTERVAL");
	}

	#[test]
	fn test_optional_duration_in_all() {
		assert!(ConfigKey::all().contains(&ConfigKey::MetricsSnapshotInterval));
	}

	#[test]
	fn test_query_memory_limit_defaults_and_round_trips() {
		assert_eq!(ConfigKey::QueryBytes.production_value(), Value::Uint8(1024 * 1024 * 1024));
		assert_eq!(ConfigKey::QueryBytes.expected_types(), &[ValueType::Uint8]);
		let key: ConfigKey = "QUERY_BYTES".parse().unwrap();
		assert_eq!(key, ConfigKey::QueryBytes);
		assert_eq!(format!("{}", ConfigKey::QueryBytes), "QUERY_BYTES");
	}

	#[test]
	fn test_query_memory_limit_rejects_zero() {
		// A zero budget would reject every query, including trivial ones, so it must not be settable.
		assert!(ConfigKey::QueryBytes.accept(Value::Uint8(0)).is_err());
		assert_eq!(ConfigKey::QueryBytes.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_query_memory_limit_requires_restart_and_optional() {
		// Read fresh for each query, so a live change takes effect immediately.
		assert!(!ConfigKey::QueryBytes.requires_restart());
		// Always defaulted to 1 GiB, never unset.
		assert!(!ConfigKey::QueryBytes.is_optional());
	}

	#[test]
	fn test_all_contains_every_compact_key_and_has_expected_len() {
		let all = ConfigKey::all();
		assert_eq!(all.len(), 51);
		assert!(all.contains(&ConfigKey::FlowFullWakeInterval));
		assert!(all.contains(&ConfigKey::SubscriptionLoadBatchBytes));
		assert!(all.contains(&ConfigKey::QueryBytes));
		assert!(all.contains(&ConfigKey::RetentionEvictInterval));
		assert!(all.contains(&ConfigKey::RetentionEvictBatchSize));
		assert!(all.contains(&ConfigKey::RetentionEvictMaxBatchesPerTick));
		assert!(all.contains(&ConfigKey::MultiFlushInterval));
		assert!(all.contains(&ConfigKey::MultiWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::OperatorResidentBytes));
		assert!(all.contains(&ConfigKey::OperatorDirtyBytes));
		assert!(all.contains(&ConfigKey::OperatorFlushSliceBytes));
		assert!(all.contains(&ConfigKey::OperatorFlushInterval));
		assert!(all.contains(&ConfigKey::OperatorWalAutocheckpoint));
		assert!(all.contains(&ConfigKey::SubscriptionWaitTimeout));
		assert!(all.contains(&ConfigKey::FlowJoinProbeBlockSize));
		assert!(all.contains(&ConfigKey::MaxRetentionHorizonFloor));
		assert!(all.contains(&ConfigKey::FlowLoadBatchBytes));
		assert!(all.contains(&ConfigKey::OperatorRangeTierBytes));
		assert!(all.contains(&ConfigKey::OperatorDirtyBytes));
		assert!(all.contains(&ConfigKey::OperatorFlushSliceBytes));
		assert!(all.contains(&ConfigKey::MultiPointBufferShards));
		assert!(all.contains(&ConfigKey::MultiRangeBufferShards));
		assert!(all.contains(&ConfigKey::CommitQueueBytes));
		assert!(all.contains(&ConfigKey::FlowPullBatchBytes));
		assert!(all.contains(&ConfigKey::FlowLoadBatchBytes));
		assert!(all.contains(&ConfigKey::QueryRowBatchSize));
		assert!(all.contains(&ConfigKey::ThreadsAsync));
		assert!(all.contains(&ConfigKey::ThreadsCoordination));
		assert!(all.contains(&ConfigKey::ThreadsFlow));
		assert!(all.contains(&ConfigKey::ThreadsTask));
		assert!(all.contains(&ConfigKey::ThreadsCompute));
		assert!(all.contains(&ConfigKey::ThreadsMaintenance));
		assert!(all.contains(&ConfigKey::MetricsFlushInterval));
		assert!(all.contains(&ConfigKey::SubscriptionWorkerThreads));
		assert!(all.contains(&ConfigKey::FlowSampleInterval));
		assert!(all.contains(&ConfigKey::MetricsSampleInterval));
		assert!(all.contains(&ConfigKey::MetricsSnapshotInterval));
		assert!(all.contains(&ConfigKey::QueueLeaseReapInterval));
		assert!(all.contains(&ConfigKey::QueueLeaseReapBatchSize));
		assert!(all.contains(&ConfigKey::QueueRetentionInterval));
		assert!(all.contains(&ConfigKey::QueueRetentionBatchSize));
		assert!(all.contains(&ConfigKey::CommitMergeInterval));
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
	fn test_flow_full_wake_interval_keeps_production_at_the_old_constant_and_rejects_a_busy_loop() {
		// a production value that drifts changes every deployment, and a zero interval loops the supervisor on
		// wake all
		let key = ConfigKey::FlowFullWakeInterval;
		assert_eq!(key.production_value(), Value::duration_milliseconds(100));
		assert_eq!(key.testing_value(), Value::duration_milliseconds(1));
		assert!(key.requires_restart());
		assert!(!key.is_optional());
		assert_eq!(key.to_string().parse::<ConfigKey>().unwrap(), key);
		assert!(key.accept(Value::duration_milliseconds(0)).is_err());
		assert!(key.accept(Value::duration_milliseconds(-1)).is_err());
		assert!(key.accept(Value::duration_milliseconds(1)).is_ok());
	}

	#[test]
	fn test_flow_latency_knobs_default_to_the_behaviour_they_replaced() {
		// a default that drifts from the old constant silently changes every deployment that never set it.
		assert_eq!(ConfigKey::FlowWakeCoalesce.default_value(), Value::duration_milliseconds(20));
		assert_eq!(ConfigKey::FlowActorBatchSize.default_value(), Value::Uint2(8));
		assert_eq!(ConfigKey::FlowCommitterShards.default_value(), Value::Uint2(1));
		assert_eq!(ConfigKey::FlowWakeCoalesce.expected_types(), &[ValueType::Duration]);
		assert_eq!(ConfigKey::FlowActorBatchSize.expected_types(), &[ValueType::Uint2]);
		assert_eq!(ConfigKey::FlowCommitterShards.expected_types(), &[ValueType::Uint2]);
		for key in [ConfigKey::FlowWakeCoalesce, ConfigKey::FlowActorBatchSize, ConfigKey::FlowCommitterShards]
		{
			assert!(key.requires_restart(), "{key} is read once when the flow subsystem starts");
			assert!(!key.is_optional(), "{key} has no off state");
		}
	}

	#[test]
	fn test_flow_latency_knobs_round_trip_their_names() {
		for (name, key) in [
			("FLOW_WAKE_COALESCE", ConfigKey::FlowWakeCoalesce),
			("FLOW_ACTOR_BATCH_SIZE", ConfigKey::FlowActorBatchSize),
			("FLOW_COMMITTER_SHARDS", ConfigKey::FlowCommitterShards),
		] {
			assert_eq!(name.parse::<ConfigKey>().unwrap(), key);
			assert_eq!(format!("{key}"), name);
		}
	}

	#[test]
	fn test_flow_latency_knobs_reject_values_that_stall_or_break_the_flows() {
		// zero shards or a zero batch never runs a commit or a message, a negative window is meaningless.
		let zero = Value::duration_seconds(0);
		assert_eq!(
			ConfigKey::FlowWakeCoalesce.accept(zero.clone()).unwrap(),
			zero,
			"zero turns coalescing off"
		);
		assert!(matches!(
			ConfigKey::FlowWakeCoalesce.accept(Value::duration_milliseconds(-1)),
			Err(AcceptError::InvalidValue(_))
		));
		assert!(matches!(
			ConfigKey::FlowActorBatchSize.accept(Value::Uint2(0)),
			Err(AcceptError::InvalidValue(_))
		));
		assert!(matches!(
			ConfigKey::FlowCommitterShards.accept(Value::Uint2(0)),
			Err(AcceptError::InvalidValue(_))
		));
		assert_eq!(ConfigKey::FlowActorBatchSize.accept(Value::Uint2(1)).unwrap(), Value::Uint2(1));
		assert_eq!(ConfigKey::FlowCommitterShards.accept(Value::Uint2(4)).unwrap(), Value::Uint2(4));
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
		assert_eq!("THREADS_MAINTENANCE".parse::<ConfigKey>().unwrap(), ConfigKey::ThreadsMaintenance);
		assert_eq!(format!("{}", ConfigKey::ThreadsAsync), "THREADS_ASYNC");
		assert_eq!(format!("{}", ConfigKey::ThreadsCoordination), "THREADS_COORDINATION");
		assert_eq!(format!("{}", ConfigKey::ThreadsFlow), "THREADS_FLOW");
		assert_eq!(format!("{}", ConfigKey::ThreadsTask), "THREADS_TASK");
		assert_eq!(format!("{}", ConfigKey::ThreadsCompute), "THREADS_COMPUTE");
		assert_eq!(format!("{}", ConfigKey::ThreadsMaintenance), "THREADS_MAINTENANCE");
	}

	#[test]
	fn test_threads_defaults() {
		assert_eq!(ConfigKey::ThreadsAsync.production_value(), Value::Uint2(1));
		assert_eq!(ConfigKey::ThreadsCoordination.production_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsFlow.production_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsTask.production_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsCompute.production_value(), Value::Uint2(2));
		assert_eq!(ConfigKey::ThreadsMaintenance.production_value(), Value::Uint2(1));
	}

	#[test]
	fn test_threads_reject_zero() {
		for key in [
			ConfigKey::ThreadsAsync,
			ConfigKey::ThreadsCoordination,
			ConfigKey::ThreadsFlow,
			ConfigKey::ThreadsTask,
			ConfigKey::ThreadsCompute,
			ConfigKey::ThreadsMaintenance,
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
		assert_eq!(ConfigKey::ThreadsMaintenance.accept(Value::Uint2(4)).unwrap(), Value::Uint2(4));
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
		assert!(ConfigKey::ThreadsMaintenance.requires_restart());
	}

	#[test]
	fn test_query_row_batch_size_default_is_uint2_128() {
		assert_eq!(ConfigKey::QueryRowBatchSize.production_value(), Value::Uint2(128));
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
		assert_eq!(ConfigKey::MetricsSnapshotInterval.accept(none.clone()).unwrap(), none);
	}

	#[test]
	fn test_accept_rejects_wrong_inner_type_typed_null_for_optional_key() {
		// Optional key still rejects typed-null whose inner doesn't match expected_types.
		let err = ConfigKey::MetricsSnapshotInterval
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
		assert_eq!(ConfigKey::HistoricalGcBatchSize.production_value(), Value::Uint8(50_000));
		assert!(matches!(ConfigKey::HistoricalGcInterval.production_value(), Value::Duration(_)));
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
	fn test_operator_flush_budget_bytes_metadata() {
		assert_eq!(ConfigKey::OperatorResidentBytes.production_value(), Value::Uint8(128 * 1024 * 1024));
		assert_eq!(ConfigKey::OperatorResidentBytes.expected_types(), &[ValueType::Uint8]);
		assert!(!ConfigKey::OperatorResidentBytes.is_optional());
		assert!(
			ConfigKey::OperatorResidentBytes.requires_restart(),
			"the budget sizes a MemoryBudget built once with the commit tier; declaring it live would \
			 promise a rewrite that no running store can adopt"
		);
	}

	#[test]
	fn test_operator_flush_budget_bytes_rejects_zero() {
		// A zero budget moves nothing per slice, so the flush lane spins forever on a backlog it
		// is never allowed to drain.
		match ConfigKey::OperatorResidentBytes.accept(Value::Uint8(0)).unwrap_err() {
			AcceptError::InvalidValue(reason) => {
				assert!(reason.contains("greater than zero"), "unexpected reason: {reason}");
			}
			other => panic!("expected InvalidValue, got {other:?}"),
		}
		assert_eq!(ConfigKey::OperatorResidentBytes.accept(Value::Uint8(1)).unwrap(), Value::Uint8(1));
	}

	#[test]
	fn test_operator_flush_budget_bytes_round_trips_through_display_and_from_str() {
		assert_eq!("OPERATOR_RESIDENT_BYTES".parse::<ConfigKey>().unwrap(), ConfigKey::OperatorResidentBytes);
		assert_eq!(format!("{}", ConfigKey::OperatorResidentBytes), "OPERATOR_RESIDENT_BYTES");
	}

	#[test]
	fn test_operator_wal_autocheckpoint_metadata() {
		assert_eq!(ConfigKey::OperatorWalAutocheckpoint.production_value(), Value::Uint8(1000000));
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
